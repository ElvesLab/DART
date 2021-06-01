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
package org.mpisws.p2p.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.appsocket.AppSocket;

/**
 * A socket in the layered transport layer.
 * 
 * @author Jeff Hoye
 *
 * @param <Identifier> the identification of the remote node
 */
public interface P2PSocket<Identifier> {
  /**
   * Details on the connectivity of the socket (encrypted, source-routed etc)
   * 
   * @return a read-only list of options on this socket
   */
  Map<String, Object> getOptions();
  
  /**
   * The identification of the node at the other end of the socket.
   * 
   * @return The identification of the node at the other end of the socket.
   */
  Identifier getIdentifier();
  
  /**
   *
   * Reads a sequence of bytes from this channel into a subsequence of the given buffer.
   * 
   * @param dsts
   * @return
   * @throws IOException
   */
  long read(ByteBuffer dsts) throws IOException; 
  
  /**
   * Writes a sequence of bytes to this channel from a subsequence of the given buffers.
   * @throws IOException 
   */  
  long write(ByteBuffer srcs) throws IOException; 
  
  /**
   * Must be called every time a Read/Write occurs to continue operation.
   *
   * Can cancel this task by calling with null.
   *
   * @param wantToRead if you want to read from this socket
   * @param wantToWrite if you want to write to this socket
   * @param receiver will have receiveSelectResult() called on it
   * note that you must call select() each time receiveSelectResult() is called.  This is so
   * your application can properly handle flow control
   */
  void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<Identifier> receiver);
  
  /**
   * Disables the output stream for this socket.  Used to properly close down a socket
   * used for bi-directional communication that can be initated by either side.   
   */
  void shutdownOutput();
  
  /**
   * Closes this socket.
   */
  void close(); 
}
