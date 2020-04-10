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
package org.mpisws.p2p.transport.util;

import java.nio.ByteBuffer;

import org.mpisws.p2p.transport.P2PSocket;

import rice.Continuation;

/**
 * Concurrently reads a buffer and writes a buffer at the same time, 
 * then calls the continuation when done.
 * 
 * @author Jeff Hoye
 *
 */
public class BufferReaderWriter<Identifier> {
  boolean doneReading = false;
  boolean doneWriting = false;
  boolean failed = false;
  boolean sentException = false;
  ByteBuffer read = null;
  private Continuation<ByteBuffer, Exception> c;
  private P2PSocket<Identifier> socket;
  
  
  public BufferReaderWriter(P2PSocket<Identifier> sock, ByteBuffer writeMe, boolean writeSize, Continuation<ByteBuffer, Exception> c) {
    this(sock, writeMe, writeSize, c, -1);
  }
  
  /**
   * 
   * @param sock the socket to read/write from/to
   * @param writeMe the bytes to write
   * @param writeSize true if I should write a size header before the buffer, false if it's fixed size
   * @param c the continuation to notify
   * @param readSize the size of the buffer to read, -1 if it should read a size header first
   */
  public BufferReaderWriter(P2PSocket<Identifier> sock, ByteBuffer writeMe, boolean writeSize, Continuation<ByteBuffer, Exception> c, int readSize) {
    this.socket = sock;
    this.c = c;
    new BufferReader<Identifier>(sock,new Continuation<ByteBuffer, Exception>() {
    
      public void receiveResult(ByteBuffer result) {
        read = result;
        doneReading = true;
        done(null);
      }
    
      public void receiveException(Exception exception) {
        failed = true;
        done(exception);
      }    
    },readSize);
    new BufferWriter<Identifier>(writeMe,sock,new Continuation<P2PSocket<Identifier>, Exception>() {

      public void receiveException(Exception exception) {
        failed = true;
        done(exception);
      }

      public void receiveResult(P2PSocket<Identifier> result) {
        doneWriting = true;
        done(null);        
      }    
    },writeSize);
  }
  
  public void done(Exception e) {
//    System.out.println("done r:"+doneReading+" w:"+doneWriting);
    if (failed) {
      if (!sentException) {
        sentException = true;
        c.receiveException(e);
        socket.close();
      }
      return;
    }
    if (doneReading && doneWriting) {
      c.receiveResult(read);
    }
  }
}
