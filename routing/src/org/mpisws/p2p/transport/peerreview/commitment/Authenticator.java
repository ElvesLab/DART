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
package org.mpisws.p2p.transport.peerreview.commitment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

public class Authenticator implements Comparable<Authenticator> {
  private long seq;
  private byte[] hash;
  private byte[] signature;
  
  int hashCode = 0;
  
  public Authenticator(long seq, byte[] hash, byte[] signature) {
    this.seq = seq;
    this.hash = hash;
    this.signature = signature;
    hashCode = (int)(seq ^ (seq >>> 32))^Arrays.hashCode(hash)^Arrays.hashCode(signature);
//    System.out.println("Auth:"+this);
  }
  
  public ByteBuffer getPartToHashThenSign() {
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      sob.writeLong(seq);
      sob.write(hash);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return sob.getByteBuffer();
  }
  
  public Authenticator(InputBuffer buf, int hashSize, int signatureSize) throws IOException {
    seq = buf.readLong();
    hash = new byte[hashSize];
    buf.read(hash);
    signature = new byte[signatureSize];
    buf.read(signature);
    hashCode = (int)(seq ^ (seq >>> 32))^Arrays.hashCode(hash)^Arrays.hashCode(signature);    
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeLong(seq);
    buf.write(hash, 0, hash.length);
    buf.write(signature, 0, signature.length);
  }

  public int hashCode() {
    return hashCode;
  }
  
  public boolean equals(Object o) {
    Authenticator that = (Authenticator)o;
    if (seq == that.seq) {
      if (Arrays.equals(hash, that.hash)) {
        if (Arrays.equals(signature, that.signature)) {
          return true;
        }
      }
    }
    return false;
  }
  
  public int compareTo(Authenticator that) {
    if (this.seq > that.seq)
      return -1;     

    if (this.seq < that.seq)
        return 1;    

    // seqs are the same
    if (this.hash == null) {
      return 1;
    }
    
    if (that.hash == null) {
      return -1;
    }
    
    // exit quickly if they are actually equal
    if (this.hashCode == that.hashCode) {
      if (this.equals(that)) {
        return 0;
      }
    }
    
    int ret = ByteBuffer.wrap(hash).compareTo(ByteBuffer.wrap(that.hash));
    if (ret == 0) {
      return ByteBuffer.wrap(signature).compareTo(ByteBuffer.wrap(that.signature));
    }
    return ret;
  }

  public long getSeq() {
    return seq;
  }
  
  public String toString() {
    return seq+" "+MathUtils.toHex(hash)+" "+MathUtils.toHex(signature);
  }

  public byte[] getHash() {
    return hash;
  }

  public byte[] getSignature() {
    return signature;
  }
}
