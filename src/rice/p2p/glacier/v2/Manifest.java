/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute 
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software 
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or 
promote products derived from this software without specific prior written 
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is" 
basis, without any representations or warranties of any kind, express or implied 
including, but not limited to, representations or warranties of 
non-infringement, merchantability or fitness for a particular purpose. In no 
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect, 
incidental, special, exemplary, or consequential damages (including, but not 
limited to, procurement of substitute goods or services; loss of use, data, or 
profits; or business interruption) however caused and on any theory of 
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if 
advised of the possibility of such damage.

*******************************************************************************/ 
package rice.p2p.glacier.v2;

import java.security.*;
import java.io.*;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.glacier.Fragment;

public class Manifest implements Serializable {
  
  static final long serialVersionUID = -436805143199825662L;
  
  protected transient byte[] objectHash;
  protected transient byte[][] fragmentHash;
  protected transient byte[] signature;
  protected long expirationDate;
  
  public Manifest(byte[] objectHash, byte[][] fragmentHash, long expirationDate) {
    this.objectHash = objectHash;
    this.fragmentHash = fragmentHash;
    this.expirationDate = expirationDate;
    this.signature = null;
  }

  public byte[] getObjectHash() {
    return objectHash;
  }
  
  public byte[] getFragmentHash(int fragmentID) {
    return fragmentHash[fragmentID];
  }

  public byte[][] getFragmentHashes() {
    return fragmentHash;
  }
  
  public byte[] getSignature() {
    return signature;
  }

  public void setSignature(byte[] signature) {
    this.signature = signature;
  }
  
  public long getExpiration() {
    return expirationDate;
  }
  
  public void update(long newExpirationDate, byte[] newSignature) {
    expirationDate = newExpirationDate;
    signature = newSignature;
  }
  
  public boolean validatesFragment(Fragment fragment, int fragmentID, Logger logger) {
    if ((fragmentID < 0) || (fragmentID >= fragmentHash.length))
      return false;
      
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("SHA");
    } catch (NoSuchAlgorithmException e) {
      if (logger.level <= Logger.SEVERE) logger.log("*** SHA-1 not supported ***"+toStringFull());
      return false;
    }

    md.reset();
    md.update(fragment.getPayload());
    
    byte[] thisHash = md.digest();
    
    if (thisHash.length != fragmentHash[fragmentID].length) {
      if (logger.level <= Logger.WARNING) logger.log("*** LENGTH MISMATCH: "+thisHash.length+" != "+fragmentHash[fragmentID].length+" ***"+toStringFull());
      return false;
    }
      
    for (int i=0; i<thisHash.length; i++) {
      if (thisHash[i] != fragmentHash[fragmentID][i]) {
        String s= "*** HASH MISMATCH: POS#"+i+", "+thisHash[i]+" != "+fragmentHash[fragmentID][i]+" ***\n";
        s+="Hash: ";
        for (int j=0; j<thisHash.length; j++)
          s+=thisHash[j];
        s+="\n"+toStringFull();
        if (logger.level <= Logger.WARNING) logger.log(s);
        return false;
      }
    }
        
    return true;
  }

  private static String dump(byte[] data, boolean linebreak) {
    final String hex = "0123456789ABCDEF";
    String result = "";
    
    for (int i=0; i<data.length; i++) {
      int d = data[i];
      if (d<0)
        d+= 256;
      int hi = (d>>4);
      int lo = (d&15);
        
      result = result + hex.charAt(hi) + hex.charAt(lo);
      if (linebreak && (((i%16)==15) || (i==(data.length-1))))
        result = result + "\n";
      else if (i!=(data.length-1))
        result = result + " ";
    }
    
    return result;
  }

  public String toString() {
    return "[Manifest obj=["+dump(objectHash, false)+"] expires="+expirationDate+"]";
  }
  
  public String toStringFull() {
    String result = "";
    
    result = result + "Manifest (expires "+expirationDate+")\n";
    result = result + "  - objectHash = ["+dump(objectHash, false)+"]\n";
    result = result + "  - signature  = ["+dump(signature, false)+"]\n";
    for (int i=0; i<fragmentHash.length; i++) 
      result = result + "  - fragmHash"+i+" = ["+dump(fragmentHash[i], false)+"]\n";
      
    return result;
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.defaultWriteObject();
    oos.writeInt(objectHash.length);
    oos.writeInt(fragmentHash.length);
    oos.writeInt(fragmentHash[0].length);
    oos.writeInt(signature.length);
    oos.write(objectHash);
    int dim1 = fragmentHash.length;
    int dim2 = fragmentHash[0].length;
    byte[] fragmentHashField = new byte[dim1*dim2];
    for (int i=0; i<dim1; i++)
      for (int j=0; j<dim2; j++)
        fragmentHashField[i*dim2 + j] = fragmentHash[i][j];
    oos.write(fragmentHashField);
    oos.write(signature);
  }
  
  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
    ois.defaultReadObject();
    int objectHashLength = ois.readInt();
    int fragmentHashLength = ois.readInt();
    int fragmentHashSubLength = ois.readInt();
    int signatureLength = ois.readInt();
    objectHash = new byte[objectHashLength];
    ois.readFully(objectHash, 0, objectHashLength);
    byte[] fragmentHashField = new byte[fragmentHashLength * fragmentHashSubLength];
    ois.readFully(fragmentHashField, 0, fragmentHashLength * fragmentHashSubLength);
    fragmentHash = new byte[fragmentHashLength][fragmentHashSubLength];
    for (int i=0; i<fragmentHashLength; i++)
      for (int j=0; j<fragmentHashSubLength; j++)
        fragmentHash[i][j] = fragmentHashField[i*fragmentHashSubLength + j];
    signature = new byte[signatureLength];
    ois.readFully(signature, 0, signatureLength);
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeInt(objectHash.length);
    buf.writeInt(fragmentHash.length);
    buf.writeInt(fragmentHash[0].length);
    buf.writeInt(signature.length);
    buf.write(objectHash, 0, objectHash.length);
    int dim1 = fragmentHash.length;
    int dim2 = fragmentHash[0].length;
    byte[] fragmentHashField = new byte[dim1*dim2];
    for (int i=0; i<dim1; i++)
      for (int j=0; j<dim2; j++)
        fragmentHashField[i*dim2 + j] = fragmentHash[i][j];
    buf.write(fragmentHashField, 0, fragmentHashField.length);
    buf.write(signature, 0, signature.length);    
  }  
  
  public Manifest(InputBuffer buf) throws IOException {
    int objectHashLength = buf.readInt();
    int fragmentHashLength = buf.readInt();
    int fragmentHashSubLength = buf.readInt();
    int signatureLength = buf.readInt();
    objectHash = new byte[objectHashLength];
    buf.read(objectHash, 0, objectHashLength);
    byte[] fragmentHashField = new byte[fragmentHashLength * fragmentHashSubLength];
    buf.read(fragmentHashField, 0, fragmentHashLength * fragmentHashSubLength);
    fragmentHash = new byte[fragmentHashLength][fragmentHashSubLength];
    for (int i=0; i<fragmentHashLength; i++)
      for (int j=0; j<fragmentHashSubLength; j++)
        fragmentHash[i][j] = fragmentHashField[i*fragmentHashSubLength + j];
    signature = new byte[signatureLength];
    buf.read(signature, 0, signatureLength);    
  }
}
