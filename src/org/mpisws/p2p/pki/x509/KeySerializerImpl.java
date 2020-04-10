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
package org.mpisws.p2p.pki.x509;

import java.io.IOException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.spec.SecretKeySpec;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

public class KeySerializerImpl implements KeySerializer {

  
  public void serialize(Key k, OutputBuffer buf) throws IOException {
    if (k instanceof PrivateKey) {
      buf.writeByte(KEY_PRIVATE);
    } else if (k instanceof PublicKey) {
      buf.writeByte(KEY_PUBLIC);      
    } else {
      buf.writeByte(KEY_SECRET);
    }
    buf.writeUTF(k.getFormat());
    buf.writeUTF(k.getAlgorithm());
    byte[] encoded = k.getEncoded();
    buf.writeInt(encoded.length);
    buf.write(encoded, 0, encoded.length);
  }
  
  public Key deserialize(InputBuffer buf) throws IOException, InvalidKeySpecException, NoSuchAlgorithmException, NoSuchProviderException {
    byte keyType = buf.readByte();
    String format = buf.readUTF();
    String algorithm = buf.readUTF();
    byte[] encoded = new byte[buf.readInt()];
    buf.read(encoded);
    
    KeySpec spec = null;
    if(format.equals("PKCS#8") || format.equals("PKCS8")) {
      spec = new PKCS8EncodedKeySpec(encoded);
    } else if(format.equals("X.509") || format.equals("X509")) {
      spec = new X509EncodedKeySpec(encoded);
    } else if(format.equals("RAW")) {
      return new SecretKeySpec(encoded, algorithm);
    }
    if (spec == null) {
      throw new IOException("Unknown key type. Type: "+keyType+" Format:"+format+" Algorithm:"+algorithm);
    }
    if (keyType == KEY_PRIVATE) {
      return KeyFactory.getInstance(algorithm, "BC").generatePrivate(spec);
    }
    return KeyFactory.getInstance(algorithm, "BC").generatePublic(spec);
  }
}
