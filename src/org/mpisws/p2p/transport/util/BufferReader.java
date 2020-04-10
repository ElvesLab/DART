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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;

import rice.Continuation;

/**
 * Reads a ByteBuffer to the socket then calls receiveResult().
 * @author Jeff Hoye
 *
 * @param <Identifier>
 */
public class BufferReader<Identifier> implements P2PSocketReceiver<Identifier> {
  boolean readSize = false;
  int size = -1;
  ByteBuffer buf;
  Continuation<ByteBuffer, Exception> continuation;
  
  /**
   * Constructor for variable/unknown sized BB, it reads the size off the stream
   * 
   * @param socket
   * @param continuation
   */
  public BufferReader(P2PSocket<Identifier> socket,
      Continuation<ByteBuffer, Exception> continuation) {
    this(socket, continuation, -1);
  }
  
  /**
   * Constructor for fixed size BB
   * @param socket
   * @param continuation
   * @param size the fixed size buffer to read
   */
  public BufferReader(P2PSocket<Identifier> socket,
      Continuation<ByteBuffer, Exception> continuation, int size) {
    this.continuation = continuation;
    this.size = size;
    if (size < 0) {
      buf = ByteBuffer.allocate(4);
    } else {
      buf = ByteBuffer.allocate(size);
    }
    try {
      receiveSelectResult(socket, true, false);
    } catch (IOException ioe) {
      receiveException(socket, ioe);
    }
  }

  public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
    continuation.receiveException(ioe);
  }

  public void receiveSelectResult(P2PSocket<Identifier> socket,
      boolean canRead, boolean canWrite) throws IOException {
//    System.out.println("BufferReader.rsr()");
    if (socket.read(buf) < 0) {
      receiveException(socket,new ClosedChannelException("Unexpected closure of channel to "+socket.getIdentifier()));
      return;
    }
    if (buf.hasRemaining()) {
      socket.register(true, false, this);
      return;
    }
    
    buf.flip();
    if (size < 0) {
      // we need to read the size from the buffer
      size = buf.asIntBuffer().get();
//      System.out.println("read size");
      buf = ByteBuffer.allocate(size);
      receiveSelectResult(socket, true, false);
    } else {
      continuation.receiveResult(buf);
    }
  }
}
