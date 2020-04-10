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
package org.mpisws.p2p.transport.wire;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;

import rice.Continuation;
import rice.Destructable;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.selector.SelectionKeyHandler;

public class TCPLayer extends SelectionKeyHandler {
  public static final Map<String, Object> OPTIONS;  
  static {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put(WireTransportLayer.OPTION_TRANSPORT_TYPE, WireTransportLayer.TRANSPORT_TYPE_GUARANTEED);
    OPTIONS = Collections.unmodifiableMap(map);    
  }
  
  // the number of sockets where we start closing other sockets
  public final int MAX_OPEN_SOCKETS;
  
  // the size of the buffers for the socket
  public final int SOCKET_BUFFER_SIZE;
  public boolean TCP_NO_DELAY = false;
  
  WireTransportLayerImpl wire;
    
  // the key to accept from
  private SelectionKey key;
  
  // the buffer used to read the header
  private ByteBuffer buffer;
  
  Logger logger;
  
//  Set<SocketAcceptor> pending;
  /**
   * Which socket to collect.
   */
//  LinkedHashMap<SocketManager, SocketManager> sockets;

  public TCPLayer(WireTransportLayerImpl wire, boolean enableServer) throws IOException {
    this.wire = wire;
    this.logger = wire.environment.getLogManager().getLogger(TCPLayer.class, null);
  
    Parameters p = wire.environment.getParameters();
    MAX_OPEN_SOCKETS = p.getInt("pastry_socket_scm_max_open_sockets");
    SOCKET_BUFFER_SIZE = p.getInt("pastry_socket_scm_socket_buffer_size"); // 32768
    if (p.contains("transport_tcp_no_delay")) {
      TCP_NO_DELAY = p.getBoolean("transport_tcp_no_delay");
    }
    ServerSocketChannel temp = null; // just to clean up after the exception
//    sockets = new LinkedHashMap<SocketManager, SocketManager>(10,0.75f,true);    
//    pending = new HashSet<SocketAcceptor>();
    
    // bind to port
    if (enableServer) {
      final ServerSocketChannel channel = ServerSocketChannel.open();
      temp = channel;
      channel.configureBlocking(false);
      channel.socket().setReuseAddress(true);
      channel.socket().bind(wire.bindAddress);
      if (logger.level <= Logger.INFO) logger.log("TCPLayer bound to "+wire.bindAddress);
      
      this.key = wire.environment.getSelectorManager().register(channel, this, SelectionKey.OP_ACCEPT);
    }
  }

  public SocketRequestHandle<InetSocketAddress> openSocket(
      InetSocketAddress destination, 
      SocketCallback<InetSocketAddress> deliverSocketToMe,
      Map<String, Object> options) {
    if (isDestroyed()) return null;
    if (logger.level <= Logger.FINEST) {
      logger.logException("openSocket("+destination+")", new Exception("Stack Trace"));
    } else {
      if (logger.level <= Logger.FINE) logger.log("openSocket("+destination+")");
    }
    if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");
    try {
      wire.broadcastChannelOpened(destination, options, true);

      synchronized (sockets) {
        SocketManager sm = new SocketManager(this, destination, deliverSocketToMe, options); 
        sockets.add(sm);
        return sm;
      }
    } catch (IOException e) {
      if (logger.level <= Logger.WARNING) logger.logException("GOT ERROR " + e + " OPENING PATH - MARKING PATH " + destination + " AS DEAD!",e);
      SocketRequestHandle<InetSocketAddress> can = new SocketRequestHandleImpl<InetSocketAddress>(destination, options, logger);
      deliverSocketToMe.receiveException(can, e);
      return can;
    }
  }
  
  Collection<SocketManager> sockets = new HashSet<SocketManager>();
  
  protected void socketClosed(SocketManager sm) {
    wire.broadcastChannelClosed(sm.addr, sm.options);
    sockets.remove(sm);
  }

  /**
   * Method which cloeses a socket to a given remote node handle, and updates
   * the bookkeeping to keep track of this closing.  Note that this method does
   * not completely close the socket, rather,  it simply calls shutdown(), which
   * starts the shutdown process.
   *
   * @param address The address of the remote node
   */
//  protected void closeSocket(InetSocketAddress addr) {
//    synchronized (sockets) {
//      if (sockets.containsKey(addr)) {
//        ((SocketManager) sockets.get(addr)).shutdown();
//      } else {
//        if (logger.level <= Logger.SEVERE) logger.log( "(SCM) SERIOUS ERROR: Request to close socket to non-open handle to path " + addr);
//      }
//    }
//  }

  public void destroy() {
    if (logger.level <= Logger.INFO) logger.log("destroy()");

    try {
      if (key != null) {
        key.channel().close();
        key.cancel();    
        key.attach(null);
      }
    } catch (IOException ioe) {
      wire.errorHandler.receivedException(null, ioe); 
    }

    // TODO: add a flag to disable this to simulate a silent fault
    for (SocketManager socket : new ArrayList<SocketManager>(sockets)) {
     // logger.log("closing "+socket);
      socket.close();      
    }
  }

  public void acceptSockets(final boolean b) {
    Runnable r = new Runnable(){    
      public void run() {
        if (b) {
          key.interestOps(key.interestOps() | SelectionKey.OP_ACCEPT);
        } else {
          key.interestOps(key.interestOps() & ~SelectionKey.OP_ACCEPT);
        }
      }    
    };
    
    // thread safety
    if (wire.environment.getSelectorManager().isSelectorThread()) {
      r.run();
    } else {
      wire.environment.getSelectorManager().invoke(r);
    }
  }
  
  /**
   * Specified by the SelectionKeyHandler interface. Is called whenever a key
   * has become acceptable, representing an incoming connection. This method
   * will accept the connection, and attach a SocketConnector in order to read
   * the greeting off of the channel. Once the greeting has been read, the
   * connector will hand the channel off to the appropriate node handle.
   *
   * @param key The key which is acceptable.
   */
  public void accept(SelectionKey key) {
    try {
      SocketManager sm = new SocketManager(this, key); 
      synchronized (sockets) {
        sockets.add(sm);
      }
      wire.incomingSocket(sm);      
    } catch (IOException e) {
      if (logger.level <= Logger.WARNING) logger.log( "ERROR (accepting connection): " + e);
      wire.errorHandler.receivedException(null, e);
    }
  }

  public boolean isDestroyed() {
    return wire.isDestroyed();
  }
}
