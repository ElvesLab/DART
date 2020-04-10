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

package rice.p2p.util;

import java.io.*;
import java.security.*;
import java.util.*;

/**
 * @(#) EncryptedOutputStream.java
 *
 * Class which serves as an output stream and transparently encrypted
 * the stream data with the given public key.  It does this by generating
 * a symmetric key, k, and then encrypting k under the public key.  
 * The header sent over the stream therefore looks like
 *
 * 4 bytes - length(e(k))
 * e(k)
 * chunks of upper-level data
 * 
 * where each chunk is length(data) [4 bytes] followed by data
 *
 * @version $Id: EncryptedOutputStream.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class EncryptedOutputStream extends OutputStream {
  
  // the maximal amount of data to buffer
  public final int BUFFER_SIZE;// = 32678;
  
  // the public key
  protected PublicKey publicKey;
  
  // the symmetric key
  protected byte[] key;
  
  // the actual underlying stream
  protected DataOutputStream stream;
  
  // the temporary array of bytes waiting to be sent out
  protected byte[] buffer;
  
  // the amount of the buffer which is currently used
  protected int bufferLength;
  
  /**
   * Builds an encrypted outputstream given a public key to 
   * encrypt thing under
   *
   * @param key The key
   * @param stream The underlying stream
   */
  public EncryptedOutputStream(PublicKey publicKey, OutputStream stream, int bufferSize) throws IOException {
    BUFFER_SIZE = bufferSize;
    this.publicKey = publicKey;
    this.stream = new DataOutputStream(stream);
    this.key = SecurityUtils.generateKeySymmetric();
    this.buffer = new byte[BUFFER_SIZE];
    this.bufferLength = 0;
    
    writeHeader();
  }
  
  /**
   * Utility method which writes out the header information
   */
  private void writeHeader() throws IOException {
    byte[] enckey =  SecurityUtils.encryptAsymmetric(key, publicKey);
    
    stream.writeInt(enckey.length);
    stream.write(enckey);
  }
  
  /**
   * Writes the specified byte to this output stream. 
   *
   * @param b the byte
   */
  public void write(int b) throws IOException {
    write(new byte[] {(byte) (b & 0xff)});
  }
  
  /**
   * Writes the given bytes to the output
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   */
  public void write(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
               ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }
    
    // if it will fit in the buffer, just fill it up
    // otherwise, write what we can and recur
    if (len <= BUFFER_SIZE - bufferLength) {
      System.arraycopy(b, off, buffer, bufferLength, len);
      bufferLength += len;
    } else {
      int l = BUFFER_SIZE - bufferLength;
      
      System.arraycopy(b, off, buffer, bufferLength, l);
      bufferLength += l;
      writeBuffer();
      
      write(b, off+l, len-l);
    }
  }
  
  /**
   * Internal method which writes out the buffered data
   */
  protected void writeBuffer() throws IOException {    
    byte[] encdata = SecurityUtils.encryptSymmetric(buffer, key, 0, bufferLength);
    stream.writeInt(encdata.length);
    stream.write(encdata); 
    
    this.bufferLength = 0;
  }
  
  /**
   * Flushes this output stream and forces any buffered output bytes 
   * to be written out. 
   */
  public void flush() throws IOException {
    writeBuffer();
    stream.flush();
  }
  
  /**
   * Closes this output stream and releases any system resources 
   * associated with this stream. 
   */
  public void close() throws IOException {
    flush();
    stream.close();
  }
}
    
