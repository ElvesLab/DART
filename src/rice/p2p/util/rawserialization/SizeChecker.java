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
package rice.p2p.util.rawserialization;

import java.io.IOException;

import rice.p2p.commonapi.rawserialization.SizeCheckOutputBuffer;

public abstract class SizeChecker implements SizeCheckOutputBuffer {
  protected int count = 0; 
  
  /**
   * Increment count appropriately
   * @throws IOException 
   */
  abstract public void writeSpecial(Object o) throws IOException;
  
  public int bytesWritten() {
    return count;
  }
  
  public int bytesRemaining() {
    return -1;
  }

  public void write(byte[] b, int off, int len) throws IOException {
    count+=len;
  }

  public void writeBoolean(boolean v) throws IOException {
    count++;
  }

  public void writeByte(byte v) throws IOException {
    count+=Byte.SIZE/8;
  }

  public void writeChar(char v) throws IOException {
    count+=Character.SIZE/8;
  }

  public void writeDouble(double v) throws IOException {
    count+=Double.SIZE/8;
  }

  public void writeFloat(float v) throws IOException {
    count+=Float.SIZE/8;
  }

  public void writeInt(int v) throws IOException {
    count+=Integer.SIZE/8;
  }

  public void writeLong(long v) throws IOException {
    count+=Long.SIZE/8;
  }

  public void writeShort(short v) throws IOException {
    count+=Short.SIZE/8;
  }

  /**
   * This is bogus if you aren't using ascii!  Would need to do more introspection.
   */
  public void writeUTF(String str) throws IOException {
    count+=str.length();
  }

}
