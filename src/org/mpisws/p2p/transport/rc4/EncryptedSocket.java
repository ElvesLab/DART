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
package org.mpisws.p2p.transport.rc4;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;

import rice.environment.logging.Logger;

public class EncryptedSocket<Identifier> extends SocketWrapperSocket<Identifier, Identifier>{

  public Cipher encryptCipher;
  public Cipher decryptCipher;

  SecretKey key;  // This is the key for the socket (it's already been hashed with a seed).
  
  
  /**
   * Since we don't know how much we can write, we need to hold any excess encrypted data here
   * 
   * Limit always points where to write new encrypted data from the cipher (write pointer)
   * Position always points where to write encrypted data to the wire (read pointer)
   * 
   * We're going to hold the lock encryptedWriteBuffer when calling socket.write(), but 
   * we have a mechanism in this.write() to prevent reentrance, because we don't want to change
   * the eWB while it is in a socket.write() call.
   */
  byte[] encryptedBytes;
  ByteBuffer encryptedWriteBuffer;
  
  public EncryptedSocket(Identifier identifier, P2PSocket<Identifier> socket,
      Logger logger, ErrorHandler<Identifier> handler, Map<String, Object> options, Cipher encryptCipher, Cipher decryptCipher, int writeBufferSize) {
    super(identifier, socket, logger, handler, options);    
    encryptedBytes = new byte[writeBufferSize];
    encryptedWriteBuffer = ByteBuffer.wrap(encryptedBytes);
    // initialize the limit to zero
    encryptedWriteBuffer.limit(0);
    this.encryptCipher = encryptCipher;
    this.decryptCipher = decryptCipher;
  }
  
  @Override
  public long read(ByteBuffer output) throws IOException {
    ByteBuffer input = ByteBuffer.allocate(output.remaining());
    long ret = socket.read(input);
    if (ret < 0) {
      return ret;
    }
    input.flip();
    try {
      return decryptCipher.update(input, output); 
    } catch (ShortBufferException sbe) {
      // this is a bug if this happens, it shouldn't b/c rc4 encryption/decryption should be the same size
      logger.logException("We got a short buffer exception reading. This indicates a bug in the implementation. "+this, sbe);
      socket.close();      
      return -1;
    }
  }

  @Override
  public long write(ByteBuffer srcs) throws IOException {
    long ret;
    // this protects against reentrance
    synchronized(encryptedWriteBuffer) {
      if (encryptedWriteBuffer.limit() != 0) {
        return 0;
      }
      ret = writeHelper(srcs);
    } 
    if (ret<0) {
      close();
    }
    return ret;
  }
  
  /**
   * Only to be called holding the encryptedWriteBuffer, probably should only be called from this.write()
   * 
   * @param srcs
   * @return
   * @throws IOException
   */
  protected long writeHelper(ByteBuffer srcs) throws IOException {
    long ret;
    
    int len = Math.min(encryptedBytes.length-encryptedWriteBuffer.limit(), srcs.remaining());
    if (len == 0) return 0;

    try {
      encryptCipher.update(srcs.array(), srcs.position(), len, encryptedBytes, encryptedWriteBuffer.limit());
      
      // update the pointers
      srcs.position(srcs.position()+len);
      encryptedWriteBuffer.limit(encryptedWriteBuffer.limit()+len);
      
      ret = socket.write(encryptedWriteBuffer);      
    } catch (ShortBufferException sbe) {
      // this is a bug if this happens, it shouldn't b/c rc4 encryption/decryption should be the same size
      logger.logException("We got a short buffer exception reading. This indicates a bug in the implementation. "+this, sbe);
      return -1;
    }

    if (ret < 0) {
      return ret;
    }
    
    if (encryptedWriteBuffer.hasRemaining()) {
      // we didn't write everything, register for writing
      socket.register(false, true, this);
      return len;
    } else {
      // reset the buffer to completely empty
      encryptedWriteBuffer.position(0);
      encryptedWriteBuffer.limit(0);
      if (srcs.hasRemaining()) {
        // we wrote everything encrypted, and the caller has more to write, 
        // recursively write again, and return the sum
        long recursiveLen = writeHelper(srcs);
        
        // if there was an error, return the error
        if (recursiveLen < 0) {
          return recursiveLen;
        }
        // return the recursive sum
        return len+recursiveLen;
      }
      // we wrote everything the user wants to write
      return len;
    }
  }

  // the problem is we don't want to be holding the encryptedWriteBuffer lock while calling socket.write, however, 
  // writeBufferMutex is really harry, and we don't know what to do on the call to receiveSelectResult
  // receiveSelectResult can only be called on the selector,
  // can we verify that write can only be called on the selector, to eliminate this problem?
  @Override
  public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead,
      boolean canWrite) throws IOException {

    boolean registerToWrite = false;
    
    // if we have remaining encrypted bytes to write, write them before calling super
    synchronized(encryptedWriteBuffer) { // it is dangerous to be holding this lock... but write can be called on any thread
      if (canWrite && encryptedWriteBuffer.hasRemaining()) {
        // write the encyrptedBuffer
        
        long ret = socket.write(encryptedWriteBuffer);
        if (ret < 0) {
          receiveException(socket, new ClosedChannelException("Socket was closed while writing buffered encrypted bytes."));        
          return;
        }
  
        if (encryptedWriteBuffer.hasRemaining()) {
          registerToWrite = true;
          // we didn't finish, only call super with the canRead bit set, and only call super if it is set
          canWrite = false;
          // continue to super call
        } else {
          // we call super but only set canWrite if there is a writer, otherwise, it was only _this_ who registered to write
          // reset the buffer to completely empty
          encryptedWriteBuffer.position(0);
          encryptedWriteBuffer.limit(0);

          canWrite = (writer != null); // we know canWrite is already true, but this can set it to false if there is no writer
        }
      }
    }
    if (registerToWrite) socket.register(false, true, this);

    if (canRead || canWrite) super.receiveSelectResult(socket, canRead, canWrite);    
  }

  

}
