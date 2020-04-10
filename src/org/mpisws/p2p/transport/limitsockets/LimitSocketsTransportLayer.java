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
package org.mpisws.p2p.transport.limitsockets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.selector.Timer;
import rice.selector.TimerTask;

/**
 * Automatically closes sockets based on LRU.
 * 
 * Uses the LinkedHashMap to perfrom the LRU policy.
 * 
 * @author Jeff Hoye
 *
 */
public class LimitSocketsTransportLayer<Identifier, MessageType> implements TransportLayer<Identifier, MessageType>, TransportLayerCallback<Identifier, MessageType> {
  int MAX_SOCKETS;
  protected TransportLayer<Identifier, MessageType> tl;
  protected LinkedHashMap<LSSocket, LSSocket> cache;
  protected Logger logger;
  protected TransportLayerCallback<Identifier, MessageType> callback;
  protected Timer timer;
  protected ErrorHandler<Identifier> handler;

  
  public LimitSocketsTransportLayer(int max_sockets, TransportLayer<Identifier, MessageType> tl, ErrorHandler<Identifier> handler, Environment env) {
    this.MAX_SOCKETS = max_sockets;
    this.tl = tl;
    this.logger = env.getLogManager().getLogger(LimitSocketsTransportLayer.class, null);
    this.timer = env.getSelectorManager().getTimer();
    this.cache = new LinkedHashMap<LSSocket, LSSocket>(MAX_SOCKETS,0.75f,true);
    this.handler = handler;
    if (this.handler == null) {
      this.handler = new DefaultErrorHandler<Identifier>(logger);
    }
    
    tl.setCallback(this);
  }

  public SocketRequestHandle<Identifier> openSocket(final Identifier i, final SocketCallback<Identifier> deliverSocketToMe, final Map<String, Object> options) {
    if (logger.level <= Logger.FINER) logger.logException(LimitSocketsTransportLayer.this+".openSocket("+i+","+deliverSocketToMe+","+options+")",new Exception("Stack Trace"));
    
    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(i, options, logger) {
      @Override
      public boolean cancel() {
        if (logger.level <= Logger.FINER) logger.log(this+".openSocket("+i+","+deliverSocketToMe+"):"+this+".cancel()");
        return super.cancel();
      }      
      
      public String toString() {
        return LimitSocketsTransportLayer.this+"RequestHandle.openSocket("+i+","+deliverSocketToMe+","+options+")";
      }
    };
    
    ret.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>(){
      public void receiveResult(SocketRequestHandle<Identifier> cancellable, P2PSocket<Identifier> sock) {
        if (logger.level <= Logger.FINER) logger.log(this+".openSocket("+i+","+deliverSocketToMe+"):"+ret+".receiveResult()");
        deliverSocketToMe.receiveResult(ret, getLSSock(sock));
      }
      public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
        if (logger.level <= Logger.FINER) logger.log(this+".openSocket("+i+","+deliverSocketToMe+"):"+ret+".receiveException("+ex+")");
        deliverSocketToMe.receiveException(ret, ex);
      }
      public String toString() {
        return LimitSocketsTransportLayer.this+"SocketCallback.openSocket("+i+","+deliverSocketToMe+","+options+")";
      }

    }, options));

    return ret;
  }
  
  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
    if (logger.level <= Logger.FINER) logger.log(this+".incomingSocket("+s+")");
    callback.incomingSocket(getLSSock(s));
  }

  protected LSSocket getLSSock(P2PSocket<Identifier> sock) {
    LSSocket ret = new LSSocket(sock);
    cache.put(ret,ret);
    closeIfNecessary();
    return ret;
  }

  protected void closeIfNecessary() {
    Collection<LSSocket> closeMe = new ArrayList<LSSocket>();
    synchronized(cache) {
      while (cache.size() > MAX_SOCKETS) {
        Iterator<LSSocket> i = cache.keySet().iterator();
        closeMe.add(i.next());
        i.remove();
      }
    }
    for (LSSocket sock : closeMe) {
      sock.forceClose();
    }
  }
  
  class LSSocket extends SocketWrapperSocket<Identifier, Identifier> {
    boolean closed = false;
    boolean forcedClose = false;
    public LSSocket(P2PSocket<Identifier> socket) {
      super(socket.getIdentifier(), socket, LimitSocketsTransportLayer.this.logger, handler, socket.getOptions());
    }
    
    /**
     * Called when we force a socket closed.
     *
     */
    public void forceClose() {
//      logger.log(this+".forceClose()");
      if (logger.level <= Logger.FINE) logger.log(this+".forceClose()");
      forcedClose = true;
      super.shutdownOutput();
//      super.close();
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            close();
          } catch (Exception ioe) {
            // do nothing, it's probably already closed anyway
          }
        }        
      }, 3000);
    }
    
    /**
     * Called by the higher layer
     */
    @Override
    public void close() {
      if (logger.level <= Logger.FINER) logger.log(this+".close()");
      closed = true;
      cache.remove(this);
      super.close();
    }
    
    @Override
    public long read(ByteBuffer dsts) throws IOException {
      if (!closed) touch(this);
      try {
        return super.read(dsts);
      } catch (IOException ioe) {
        close();
        throw ioe;
      }
    }

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<Identifier> receiver) {
      if (forcedClose) {
        if (wantToWrite) {
          receiver.receiveException(this, new ClosedChannelException("Limit Sockets forced close. "+this));        
        }
        if (wantToRead) {
          super.register(true, false, receiver);          
        }
        return;
      }
      if (!closed) touch(this);
      super.register(wantToRead, wantToWrite, receiver);
    }

    @Override
    public long write(ByteBuffer srcs) throws IOException {
      if (forcedClose) throw new ClosedChannelException("Limit Sockets forced close. "+this);        
      if (!closed) touch(this);      
      try {
        return super.write(srcs);
      } catch (IOException ioe) {
        close();
        throw ioe;
      }
    }

    @Override
    public String toString() {
      return LimitSocketsTransportLayer.this.toString()+"$LSSocket<"+identifier+">["+(closed?"closed":"open")+"]@"+System.identityHashCode(this)+socket.toString();
    }
  }
  
  public void touch(LSSocket socket) {
    synchronized(cache) {
      if (cache.get(socket) == null) {
        cache.put(socket, socket);
        closeIfNecessary();
      }
    }
  }

  public String toString() {
    return "LimitSocks<"+cache.size()+">";
  }
  
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  public MessageRequestHandle<Identifier, MessageType> sendMessage(Identifier i, MessageType m, MessageCallback<Identifier, MessageType> deliverAckToMe, Map<String, Object> options) {
    return tl.sendMessage(i, m, deliverAckToMe, options);
  }

  public void messageReceived(Identifier i, MessageType m, Map<String, Object> options) throws IOException {
    callback.messageReceived(i, m, options);
  }
  
  public void setCallback(TransportLayerCallback<Identifier, MessageType> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.handler = handler;
    tl.setErrorHandler(handler);    
  }

  public void destroy() {
    tl.destroy();
  }

  
}
