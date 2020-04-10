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
 * Writes a ByteBuffer to the socket then calls receiveResult().
 * 
 * Used for writing headers.
 * 
 * If continuation is null, then it closes the socket when done, or if there is an error.
 * 
 * @author Jeff Hoye
 *
 * @param <Identifier>
 */
public class BufferWriter<Identifier> implements P2PSocketReceiver<Identifier> {
  ByteBuffer sizeBuf = null;
  ByteBuffer writeMe;
  Continuation<P2PSocket<Identifier>, Exception> continuation;
  
  /**
   * 
   * @param writeMe
   * @param socket
   * @param continuation
   * @param includeSizeHeader true if the size needs to be written in a header, false if it is a fixed size buffer
   */
  public BufferWriter(ByteBuffer writeMe, P2PSocket<Identifier> socket,
      Continuation<P2PSocket<Identifier>, Exception> continuation, boolean includeSizeHeader) {
    this.writeMe = writeMe;
    this.continuation = continuation;
    if (includeSizeHeader) {
      sizeBuf = ByteBuffer.allocate(4);
      sizeBuf.asIntBuffer().put(writeMe.remaining());
      sizeBuf.clear();
    }
    try {
      receiveSelectResult(socket, false, true);
    } catch (IOException ioe) {
      receiveException(socket, ioe);
    }
  }
  
  public BufferWriter(ByteBuffer writeMe, P2PSocket<Identifier> socket,
      Continuation<P2PSocket<Identifier>, Exception> continuation) {
    this(writeMe, socket, continuation, true);
  }

  public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
    if (continuation == null) {
      socket.close();
    } else {
      continuation.receiveException(ioe);
    }
  }

  public void receiveSelectResult(P2PSocket<Identifier> socket,
      boolean canRead, boolean canWrite) throws IOException {
//    System.out.println("BufferWriter.rsr()");
    if (sizeBuf != null && sizeBuf.hasRemaining()) {
      if (socket.write(sizeBuf) < 0) {
        receiveException(socket,new ClosedChannelException("Unexpected closure of channel to "+socket.getIdentifier()));
        return;
      }         
    }
    if (socket.write(writeMe) < 0) {
      receiveException(socket,new ClosedChannelException("Unexpected closure of channel to "+socket.getIdentifier()));
      return;
    }    
    if (writeMe.hasRemaining()) {
      socket.register(false, true, this);
      return;
    }
        
    if (continuation == null) {
      socket.close();
    } else {
      continuation.receiveResult(socket);
    }
  }
}
