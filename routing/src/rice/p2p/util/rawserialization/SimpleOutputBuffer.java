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
/*
 * Created on Mar 30, 2006
 */
package rice.p2p.util.rawserialization;

import java.io.*;
import java.nio.ByteBuffer;

import rice.p2p.commonapi.rawserialization.OutputBuffer;

public class SimpleOutputBuffer extends DataOutputStream implements OutputBuffer {
  ByteArrayOutputStream baos;

  public SimpleOutputBuffer(int size) {
    super(new ByteArrayOutputStream(size));    
    baos = (ByteArrayOutputStream)out;
  }

  public SimpleOutputBuffer() {
    super(new ByteArrayOutputStream());    
    baos = (ByteArrayOutputStream)out;
  }
  public void writeByte(byte v) throws IOException {
    this.write(v);
  }

  public void writeChar(char v) throws IOException {
    writeChar((int) v);
  }

  public void writeShort(short v) throws IOException {
    writeShort((int) v);
  }
  
  public int bytesRemaining() {
    return Integer.MAX_VALUE;
  }
  
  public byte[] getBytes() {
    return baos.toByteArray();    
  }
  
  public ByteBuffer getByteBuffer() {
    return ByteBuffer.wrap(getBytes());
  }
  
  /**
   * The amount of bytes written so far... the size (not the capacity).
   * @return
   */
  public int getWritten() {
    return written; 
  }  
}
