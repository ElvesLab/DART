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
package org.mpisws.p2p.transport.peerreview.history.hasher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.mpisws.p2p.transport.peerreview.history.HashProvider;

import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

public class SHA1HashProvider implements HashProvider {
  private MessageDigest md;
  
  public SHA1HashProvider() throws NoSuchAlgorithmException {
//    try {
      md = MessageDigest.getInstance("SHA");
//    } catch ( NoSuchAlgorithmException e ) {
//      Logger logger = env.getLogManager().getLogger(getClass(), null);
//      if (logger.level <= Logger.SEVERE) logger.log(
//        "No SHA support!" );
//    }

  }
  
  public byte[] getEmptyHash() {
    return new byte[getHashSizeBytes()];
  }

  public short getHashSizeBytes() {
    return 20;
  }

  public synchronized byte[] hash(long seq, short type, byte[] nodeHash, byte[] contentHash) {
//    System.out.println("Hashing "+seq+","+type+","+MathUtils.toBase64(nodeHash)+","+MathUtils.toBase64(contentHash));
    try {
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      sob.writeLong(seq);
      sob.writeShort(type);
      sob.write(nodeHash);
      sob.write(contentHash);
      return hash(sob.getByteBuffer());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public synchronized byte[] hash(ByteBuffer... hashMe) {    
//    System.out.println("Hashing "+hashMe.length);
    
    for (ByteBuffer bb : hashMe) {
      int pos = bb.position();
//      System.out.println(MathUtils.toHex(bb.array()));
      md.update(bb);
      bb.position(pos);
    }
    byte[] ret = md.digest();
//    System.out.println("hash: "+MathUtils.toBase64(ret));
    return ret;
  }
  
}
