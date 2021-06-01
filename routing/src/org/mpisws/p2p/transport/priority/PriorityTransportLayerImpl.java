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
package org.mpisws.p2p.transport.priority;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.TransportLayerListener;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.identity.MemoryExpiredException;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.priority.PriorityTransportLayerImpl.EntityManager.MessageWrapper;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.wire.WireTransportLayer;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.exception.NodeIsDeadException;
import rice.p2p.util.MathUtils;
import rice.p2p.util.SortedLinkedList;
import rice.p2p.util.tuples.Tuple;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;

/**
 * 
 * @author Jeff Hoye
 *
 */
public class PriorityTransportLayerImpl<Identifier> implements PriorityTransportLayer<Identifier>, LivenessListener<Identifier>, TransportLayerCallback<Identifier, ByteBuffer> {

  TransportLayer<Identifier, ByteBuffer> tl;
  LivenessProvider<Identifier> livenessProvider;
  ProximityProvider<Identifier> proximityProvider;
  
  public static final byte PASSTHROUGH_SOCKET_B = 0;
  public static final byte PRIMARY_SOCKET_B = 1;
  public static final byte BIG_MSG_SOCKET_B = 2;
  public static final byte[] PASSTHROUGH_SOCKET = {PASSTHROUGH_SOCKET_B};
  public static final byte[] PRIMARY_SOCKET = {PRIMARY_SOCKET_B};
  public static final byte[] BIG_MSG_SOCKET = {BIG_MSG_SOCKET_B};
  
  public int MAX_MSG_SIZE = 10000;
  public int MAX_QUEUE_SIZE = 30;
  
  /**
   * BIG messages open a socket especially for big messages.  This is the bigest message size allowed.
   */
  public int MAX_BIG_MSG_SIZE = Integer.MAX_VALUE;
  
  public Logger logger;
  
  protected Map<Identifier, EntityManager> entityManagers;

  private TransportLayerCallback<Identifier, ByteBuffer> callback;

  private ErrorHandler<Identifier> errorHandler;
  
  protected SelectorManager selectorManager;
  protected Environment environment;
  
  protected ArrayList<PrimarySocketListener<Identifier>> primarySocketListeners = new ArrayList<PrimarySocketListener<Identifier>>();
  
  /**
   * The maximum message size;
   * 
   * @param env
   * @param maxMsgSize
   */
  public PriorityTransportLayerImpl(TransportLayer<Identifier, ByteBuffer> tl, 
      LivenessProvider<Identifier> livenessProvider,
      ProximityProvider<Identifier> proximityProvider,
      Environment env, 
      int maxMsgSize,
      int maxQueueSize,
      ErrorHandler<Identifier> handler) {
    entityManagers = new HashMap<Identifier, EntityManager>();
    this.logger = env.getLogManager().getLogger(PriorityTransportLayerImpl.class, null);
    this.selectorManager = env.getSelectorManager();
    this.environment = env;
    this.MAX_MSG_SIZE = maxMsgSize;
    this.MAX_QUEUE_SIZE = maxQueueSize;
    this.tl = tl;    
    if (logger.level <= Logger.INFO) logger.log("MAX_QUEUE_SIZE:"+MAX_QUEUE_SIZE+" MAX_MSG_SIZE:"+MAX_MSG_SIZE);
    this.livenessProvider = livenessProvider;
    this.proximityProvider = proximityProvider;
    tl.setCallback(this);
    livenessProvider.addLivenessListener(this);
    this.errorHandler = handler;
    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<Identifier>(logger); 
    }
  }

  /**
   * We have to read the first byte and see if this is a 
   * passthrough (the layer higher than us asked to open it) socket or a 
   * primary (our layer tried to open it) socket.
   */
  public void incomingSocket(final P2PSocket<Identifier> s) throws IOException {
    s.register(true, false, new P2PSocketReceiver<Identifier>() {
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
        if (socket != s) throw new IllegalArgumentException("Sockets not equal!!! s:"+s+" socket:"+socket);
        if (canWrite || !canRead) throw new IllegalArgumentException("Should only be able to read! canRead:"+canRead+" canWrite:"+canWrite);
        // the first thing we need to do is to find out if this is a primary socket or a passthrough
        ByteBuffer hdr = ByteBuffer.allocate(1);
        int ret;
        try {
          ret = (int)socket.read(hdr);
        } catch (IOException ioe) {
//          errorHandler.receivedException(s.getIdentifier(), new IOException("Incoming socket terminated early."));
          socket.close();
          return;
        }
        switch (ret) {
        case -1: 
          // closed... strange
          socket.close();
          break;
        case 0:
          // reregister
          socket.register(true, false, this);
          break;
        case 1:
          // success
          hdr.flip();
          byte val = hdr.get();
          switch(val) {
          case PASSTHROUGH_SOCKET_B:
            callback.incomingSocket(s);
            break;
          case PRIMARY_SOCKET_B:
            if (logger.level <= Logger.FINE) logger.log("Opened Primary Socket from "+s.getIdentifier());
            getEntityManager(s.getIdentifier()).primarySocketAvailable(s, null);
            break;
          case BIG_MSG_SOCKET_B:
            if (logger.level <= Logger.FINE) logger.log("Opened BIG Message Socket from "+s.getIdentifier());
            getEntityManager(s.getIdentifier()).handleBigMsgSocket(s);
            break;
          }
          break;
        default:
          //Whisky Tango Foxtrot?
          socket.close();
          throw new IllegalStateException("Read "+ret+" bytes?  Not good.  Expected to read 1 byte.");
        }
      }
      
      public void receiveException(P2PSocket<Identifier> socket, Exception e) {
        errorHandler.receivedException(socket.getIdentifier(), e);
      }      
//    }.receiveSelectResult(s, true, false);    
      });    
  }

  public SocketRequestHandle<Identifier> openSocket(Identifier i, final SocketCallback<Identifier> deliverSocketToMe, Map<String, Object> options) {
    if (deliverSocketToMe == null) throw new IllegalArgumentException("No handle to return socket to! (deliverSocketToMe must be non-null!)");
    
    final SocketRequestHandleImpl<Identifier> handle = new SocketRequestHandleImpl<Identifier>(i, options, logger);    
    handle.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>() {
      public void receiveResult(SocketRequestHandle<Identifier> cancellable, final P2PSocket<Identifier> sock) {
        
        handle.setSubCancellable(new Cancellable() {        
          public boolean cancel() {
            sock.close();
            return true;
          }        
        });
        
        sock.register(false, true, new P2PSocketReceiver<Identifier>() {

          public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
            if (canRead || !canWrite) throw new IllegalArgumentException("expected to write!  canRead:"+canRead+" canWrite:"+canWrite);
            socket.write(ByteBuffer.wrap(PASSTHROUGH_SOCKET));
            if (deliverSocketToMe != null) deliverSocketToMe.receiveResult(handle, socket);
          }        
          
          public void receiveException(P2PSocket<Identifier> socket, Exception e) {
            if (deliverSocketToMe != null) deliverSocketToMe.receiveException(handle, e);
          }
        });
      } // receiveResult()
      public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
        if (handle.getSubCancellable() != null && s != handle.getSubCancellable()) throw new IllegalArgumentException("s != handle.getSubCancellable() must be a bug. s:"+s+" sub:"+handle.getSubCancellable());
        if (deliverSocketToMe != null) deliverSocketToMe.receiveException(handle, ex);
      }
    }, options));
    
    return handle;
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

  public void messageReceived(Identifier i, ByteBuffer m, Map<String, Object> options) throws IOException {
    callback.messageReceived(i, m, options);
    notifyListenersRead(m.remaining(), i, options);
  }  

  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(final Identifier i, ByteBuffer m, MessageCallback<Identifier, ByteBuffer> deliverAckToMe, final Map<String, Object> options) {
    if (logger.level <= Logger.FINE) logger.log("sendMessage("+i+","+m+","+deliverAckToMe+","+options+")");
    
//    if (options == null) throw new IllegalArgumentException("options is null"); // delme, only for debugging something else
    // if it is to be sent UDP, just pass it through
    if (options != null && 
        options.containsKey(WireTransportLayer.OPTION_TRANSPORT_TYPE)) {
        Integer val = (Integer)options.get(WireTransportLayer.OPTION_TRANSPORT_TYPE);
        if (val != null &&
            val.intValue() == WireTransportLayer.TRANSPORT_TYPE_DATAGRAM) {
          final int originalSize = m.remaining();
          return tl.sendMessage(i, m, new MessageCallback<Identifier, ByteBuffer>() {

            public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
              notifyListenersWrote(originalSize, i, options);
            }

            public void sendFailed(
                MessageRequestHandle<Identifier, ByteBuffer> msg,
                Exception reason) {
              notifyListenersDropped(originalSize, i, options);
            }
            
          }, options);
        }
    }
    
    return getEntityManager(i).send(i, m, deliverAckToMe, options);
  }

  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  protected boolean destroyed = false;
  public void destroy() {
    if (destroyed) return;
    if (environment.getSelectorManager().isSelectorThread()) {
      destroyed = true;    
      tl.destroy();    
    } else {
      environment.getSelectorManager().invoke(new Runnable() {
        public void run() {
          destroy();
        }        
      });
    }
  }
  
  protected EntityManager getEntityManager(Identifier i) {
    synchronized(entityManagers) {
      EntityManager ret = entityManagers.get(i); 
      if (ret == null) {
        ret = new EntityManager(i);
        entityManagers.put(i, ret);
      }
      return ret;
    }
  }
  
  protected EntityManager deleteEntityManager(Identifier i) {
    synchronized(entityManagers) {
      EntityManager ret = entityManagers.get(i); 
      if (ret != null) {
        ret.clearState();
      }
      return ret;
    }
  }

  public void livenessChanged(Identifier i, int val, Map<String, Object> options) {
    if (val >= LivenessListener.LIVENESS_DEAD) {
      getEntityManager(i).markDead();
    }
  }

  public void cancelLivenessChecker(Identifier i) {
    getEntityManager(i).stopLivenessChecker();
  }
  
  /**
   * Problem?: this method should perhaps take the EntityManager as an arg.
   * @param i
   * @param options
   * @return
   */
//  protected void openPrimarySocket(final Identifier i, Map<String, Object> options) {
////    if (livenessProvider.getLiveness(i, options) >= LIVENESS_DEAD) {
////      if (logger.level <= Logger.WARNING) logger.log("Not opening primary socket to "+i+" because it is dead.");  
////      return null;
////    }     
//    if (logger.level <= Logger.FINE) logger.log("Opening Primary Socket to "+i);
//    
//    final SocketRequestHandleImpl<Identifier> handle = new SocketRequestHandleImpl<Identifier>(i, options, logger) {
//      public boolean cancel() {
//        cancelLivenessChecker(i);
//        return super.cancel();
//      }
//    };
//    
//    handle.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>() {
//      public void receiveResult(SocketRequestHandle<Identifier> cancellable, final P2PSocket<Identifier> sock) {
//        handle.setSubCancellable(new Cancellable(){        
//          public boolean cancel() {
//            sock.close();
//            return true;
//          }        
//        });
//        sock.register(false, true, new P2PSocketReceiver<Identifier>() {
//          ByteBuffer writeMe = ByteBuffer.wrap(PRIMARY_SOCKET);
//          public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
//            if (canRead || !canWrite) throw new IllegalArgumentException("expected to write!  canRead:"+canRead+" canWrite:"+canWrite);
//            if (logger.level <= Logger.FINE) logger.log("Opened Primary socket "+socket+" to "+i);
////            if (logger.level <= Logger.FINE) logger.log("Opened Primary socket "+socket+" to "+i);
//            cancelLivenessChecker(i);
//            if (socket.write(writeMe) == -1) {
//              cancelLivenessChecker(i);
//              getEntityManager(socket.getIdentifier()).receiveSocketException(handle, new org.mpisws.p2p.transport.ClosedChannelException("Channel closed while writing."));
//              return;
//            }
//            if (writeMe.hasRemaining()) {
//              socket.register(false, true, this);
//            } else {
//              getEntityManager(socket.getIdentifier()).incomingSocket(socket, handle);
//            }
//          }        
//          
//          public void receiveException(P2PSocket<Identifier> socket, Exception e) {
//            cancelLivenessChecker(i);
//            getEntityManager(socket.getIdentifier()).receiveSocketException(handle, e);
//          }
//          
//          public String toString() {
//            return "PriorityTLi: Primary Socket shim to "+i;
//          }
//        });
//      } // receiveResult()
//      
//      public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
//        if (handle.getSubCancellable() != null && s != handle.getSubCancellable()) throw new IllegalArgumentException(
//            "s != handle.getSubCancellable() must be a bug. s:"+
//            s+" sub:"+handle.getSubCancellable());
//        getEntityManager(s.getIdentifier()).receiveSocketException(handle, ex);
//      }
//    }, options));
//    
////    return handle;
//  }
  
  public void printMemStats(int logLevel) {
    if (logLevel <= Logger.FINE) {
      synchronized(entityManagers) {
        int queueSum = 0;
        for(EntityManager em : entityManagers.values()) {
          int queueSize = em.queue.size();
          queueSum+=queueSize;          
          if (logLevel <= Logger.FINEST ||   // finest prints empty queues 
             (queueSize > 0 && logLevel <= Logger.FINER)) {  // only prints non-empty queues
            Identifier temp = em.identifier.get();
            String s = "";
            Map<String, Object> options = null; 
            if (temp != null) {
              MessageWrapper peek = em.peek();
              if (peek != null) {
                options = peek.options;
              }
              s = ""+livenessProvider.getLiveness(temp, options);
            }
            logger.log("EM{"+temp+","+s+","+em.writingSocket+","+em.pendingSocket+"} queue:"+queueSize+" reg:"+em.registered+" lChecker:"+em.livenessChecker);
          }
        }        
        logger.log("NumEMs:"+entityManagers.size()+" numPendingMsgs:"+queueSum);
      } // synchronized
    }
  }
  
  /**
   * Responsible for writing messages to the socket.
   * 
   * Synchronization: all state is changed on the selector thread, except the queue, which must be carefully 
   * synchronized.
   * 
   * If we have something to write that means !queue.isEmpty() || messageThatIsBeingWritten != null, 
   *   we should have a writingSocket, or a pendingSocket
   * 
   * We only touch writingSocket if there is an error, or on scheduleToWriteIfNeeded()
   * 
   * We only change messageThatIsBeingWritten as a result of a call from receiveResult(socket, false, true);
   * 
   * @author Jeff Hoye
   */
  class EntityManager implements P2PSocketReceiver<Identifier> {
    // TODO: think about the behavior of this when it wraps around...
    int seq = Integer.MIN_VALUE;
    SortedLinkedList<MessageWrapper> queue; // messages we want to send
    Collection<P2PSocket<Identifier>> sockets;
    
    WeakReference<Identifier> identifier;
    
    SocketRequestHandle<Identifier> pendingSocket; // the receipt that we are opening a socket
    P2PSocket<Identifier> writingSocket; // don't try to write to multiple socktes, it will confuse things
    P2PSocket<Identifier> closeWritingSocket; // could be a boolean, but we store the writingSocket here just for debugging, == writingSocket if should close it after the current write
    MessageWrapper messageThatIsBeingWritten; // the current message we are sending, if this is null, we aren't in the middle of sending a message
    // Invariant: if (messageThatIsBeingWritten != null) then (writingSocket != null)
    private boolean registered = false;  // true if registed for writing
    
    EntityManager(Identifier identifier) {
      this.identifier = new WeakReference<Identifier>(identifier);
      queue = new SortedLinkedList<MessageWrapper>();
      sockets = new HashSet<P2PSocket<Identifier>>();
    }

    public String toString() {
      return "EM{"+identifier.get()+"}";
    }
    
    public void clearState() {
      if (!selectorManager.isSelectorThread()) {
        selectorManager.invoke(new Runnable() {      
          public void run() {
            clearState();      
          }      
        });
        return;
      }
      
      for (P2PSocket socket : sockets) {
//        try {
          socket.close();
//        } catch (IOException ioe) {
//          errorHandler.receivedException(i, error)
//        }
      }
      synchronized(queue) {
        queue.clear();
        messageThatIsBeingWritten = null;
      }
      synchronized(EntityManager.this) {  
        if (logger.level <= logger.INFO) logger.log(EntityManager.this+".clearState() setting pendingSocket to null "+pendingSocket);

        if (pendingSocket != null) {
          pendingSocket.cancel();
          stopLivenessChecker();
        }
        pendingSocket = null;
      }
    }

    /**
     * Read an error, or socket was closed.
     * 
     * The purpose of this method is to let the currently writing message to complete.
     * 
     * @param socket
     * @return true if we did it now
     */
    public boolean closeMe(P2PSocket<Identifier> socket) {
      if (logger.level <= Logger.FINER) logger.logException("closeMe("+socket+"):"+(socket == writingSocket)+","+messageThatIsBeingWritten, new Exception("Stack Trace"));
      if (socket == writingSocket) {
        if (messageThatIsBeingWritten == null) {
          sockets.remove(socket);
          socket.close();
          setWritingSocket(null);
          return true;
        }
        closeWritingSocket = writingSocket;
        return false;
      } else {
        sockets.remove(socket);
        socket.close();
        return true;
      }
    }
    
    /**
     * Get's the socket, both when we open it, and when a remote node opens it.
     * 
     * @param s
     * @param receipt null if a remote node opened the socket
     */
    public void primarySocketAvailable(P2PSocket<Identifier> s, SocketRequestHandle<Identifier> receipt) {
      // make sure we're on the selector thread so synchronization of writingSocket is simple
      if (!selectorManager.isSelectorThread()) throw new IllegalStateException("Must be called on the selector");

      if (logger.level <= Logger.FINE) logger.log("primarySocketAvailable("+s+","+receipt+")");
      
      // set pendingSocket to null if possible
      synchronized(EntityManager.this) {
        if (receipt != null) {
          if (receipt == pendingSocket) {
            if (logger.level <= logger.INFO) logger.log(EntityManager.this+".primarySocketAvailable setting pendingSocket to null "+pendingSocket);
            stopLivenessChecker();
            if (logger.level <= Logger.FINE) logger.log("got socket:"+s+" clearing pendingSocket:"+pendingSocket);
            pendingSocket = null;  // this is the one we requested
          } else {
            // why would this ever be equal? -Jeff 01.11.08
  //          logger.log("receipt != pendingSocket!!! receipt:"+receipt+" pendingSocket:"+pendingSocket);
          } 
        }
      }
      
      sockets.add(s);
      scheduleToWriteIfNeeded();
      
      // also, be able to read incoming messages on every socket
      new SizeReader(s);
    }
    
    public void setWritingSocket(P2PSocket<Identifier> s/*, String loc*/) {
//      logger.logException(this+".setWritingSocket("+s+")", new Exception());
      if (logger.level <= Logger.INFO-1) logger.log(this+".setWritingSocket("+s+")");
//      if (logger.level <= Logger.FINEST) logger.log(this+".setWritingSocket("+s+")");
//      if (logger.level <= Logger.INFO) logger.log(this+".setWritingSocket("+s+") loc:"+loc);
      writingSocket = s;
      if (primarySocketListeners.isEmpty()) return;
      if (s == null) {
        for (PrimarySocketListener<Identifier> l : primarySocketListeners) {
          l.notifyPrimarySocketClosed(identifier.get());
        }
      } else {
        for (PrimarySocketListener<Identifier> l : primarySocketListeners) {
          l.notifyPrimarySocketOpened(s.getIdentifier(), s.getOptions());
        }
      }
    }

    /**
     * Must be called on selectorManager.
     *
     * A) finds a writingSocket if possible
     *   opens one if needed
     */
    protected void scheduleToWriteIfNeeded() {
      if (!selectorManager.isSelectorThread()) throw new IllegalStateException("Must be called on the selector");

      Identifier temp = identifier.get();
      if (temp == null) {
        purge(new MemoryExpiredException("No record of identifier for "+this)); 
        return;
      }
      
      // make progress acquiring a writingSocket
      if (writingSocket == null) {
        registered = false;
        if (!sockets.isEmpty()) {
          setWritingSocket(sockets.iterator().next()/*, "scheduleToWriteIfNeeded"*/);
        } else {
          // we need to get a writingSocket
          if (pendingSocket == null) {
            MessageWrapper peek = peek();
            if (peek != null) {
              openPrimarySocketHelper(temp, peek.options);
            }
          }
        }
      }
      
      // register on the writingSocket if needed
      if (!registered && writingSocket != null) {
        if (haveMessageToSend()) {
          //logger.log(this+" registering on "+writingSocket);
          // maybe we should remember if we were registered, and don't reregister, but for now it doesn't hurt
          registered = true;  // may fail in this call and set registered back to false, so make sure to do this before calling register          
          if (logger.level <= Logger.FINEST) logger.log(this+".scheduleToWriteIfNeeded() registering to write on "+writingSocket);
          writingSocket.register(false, true, this);
        }
      }      
    }
    
    public void openPrimarySocketHelper(final Identifier i, Map<String, Object> options) {
      synchronized(this) {
        if (pendingSocket != null || writingSocket != null) return;
        if (logger.level <= Logger.FINE) logger.log("Opening Primary Socket to "+i);
        
        final SocketRequestHandleImpl<Identifier> handle = new SocketRequestHandleImpl<Identifier>(i, options, logger) {
          public boolean cancel() {
            getEntityManager(i).receiveSocketException(this, new org.mpisws.p2p.transport.ClosedChannelException("Channel cancelled."));
            return super.cancel();
          }
        };

        if (logger.level <= Logger.INFO) logger.log(EntityManager.this+".openPrimarySocketHelper() setting pendingSocket to "+handle);
        pendingSocket = handle;
        startLivenessChecker(i, options);
        
        handle.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>() {
          public void receiveResult(SocketRequestHandle<Identifier> cancellable, final P2PSocket<Identifier> sock) {
            handle.setSubCancellable(new Cancellable(){        
              public boolean cancel() {
                sock.close();
                return true;
              }        
            });
            sock.register(false, true, new P2PSocketReceiver<Identifier>() {
              ByteBuffer writeMe = ByteBuffer.wrap(PRIMARY_SOCKET);
              public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
                if (canRead || !canWrite) throw new IllegalArgumentException("expected to write!  canRead:"+canRead+" canWrite:"+canWrite);
                if (logger.level <= Logger.FINE) logger.log("Opened Primary socket "+socket+" to "+i);
//                if (logger.level <= Logger.FINE) logger.log("Opened Primary socket "+socket+" to "+i);
//                cancelLivenessChecker(i);
                if (socket.write(writeMe) == -1) {
                  cancelLivenessChecker(i);
                  getEntityManager(socket.getIdentifier()).receiveSocketException(handle, new org.mpisws.p2p.transport.ClosedChannelException("Channel closed while writing."));
                  return;
                }
                if (writeMe.hasRemaining()) {
                  socket.register(false, true, this);
                } else {
                  getEntityManager(socket.getIdentifier()).primarySocketAvailable(socket, handle);
                }
              }        
              
              public void receiveException(P2PSocket<Identifier> socket, Exception e) {
//                cancelLivenessChecker(i);
                getEntityManager(socket.getIdentifier()).receiveSocketException(handle, e);
              }
              
              public String toString() {
                return "PriorityTLi: Primary Socket shim to "+i;
              }
            });
          } // receiveResult()
          
          public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
            if (handle.getSubCancellable() != null && s != handle.getSubCancellable()) throw new IllegalArgumentException(
                "s != handle.getSubCancellable() must be a bug. s:"+
                s+" sub:"+handle.getSubCancellable());
            getEntityManager(s.getIdentifier()).receiveSocketException(handle, ex);
          }
        }, options));

      }
    }
    
    
    TimerTask livenessChecker = null;
    public void startLivenessChecker(final Identifier temp, final Map<String, Object> options) {
      if (options == null) {
        throw new IllegalArgumentException("Options is null");
      }
      if (livenessChecker == null) {
        if (logger.level <= Logger.FINER) logger.log("startLivenessChecker("+temp+","+options+") pend:"+pendingSocket+" writingS:"+writingSocket+" theQueue:"+queue.size());
        livenessChecker = new TimerTask() {      
          @Override
          public void run() {            
            synchronized(EntityManager.this) {
              if (cancelled) return;
              stopLivenessChecker(); // sets livenssChecker back to null
              if (destroyed) return;
//            Map<String, Object> options;
//            MessageWrapper peek = peek();
//            if (peek != null) {
//              options = peek.options;
//            } else {
//              options = this.options;
//            }
              livenessProvider.checkLiveness(temp, options);        

            // if this throws a NPE, there is a bug, cause this should have been cancelled if pendingSocket == null
              if (logger.level <= logger.INFO) logger.log(EntityManager.this+".liveness checker setting pendingSocket to null "+pendingSocket);
              pendingSocket.cancel();  
              pendingSocket = null;
            }
            scheduleToWriteIfNeeded();  // will restart this livenessChecker, create a new pendingSocket
          }        
        };

        int delay = proximityProvider.proximity(temp, options)*4;
        if (delay < 5000) delay = 5000; // 1 second
        if (delay > 40000) delay = 40000; // 20 seconds
        
        selectorManager.schedule(livenessChecker, delay);
      }
    }
      
    public void stopLivenessChecker() {
      if (livenessChecker == null) return;
      if (logger.level <= Logger.FINER) logger.log("stopLivenessChecker("+identifier.get()+") pend:"+pendingSocket+" writingS:"+writingSocket+" theQueue:"+queue.size());

      livenessChecker.cancel();
      livenessChecker = null;
    }
    
    /**
     * Returns the messageThatIsBeingWritten, or the first in the queue, w/o setting messageThatIsBeingWritten
     * @return
     */
    private MessageWrapper peek() {
      synchronized(queue) {
        if (messageThatIsBeingWritten == null) {
          return queue.peek();
        }
        return messageThatIsBeingWritten;
      }
    }
    
    /**
     * Returns the messageThatIsBeingWritten, polls the queue if it is null
     * @return
     */
    private MessageWrapper poll() {
      synchronized(queue) {  
        if (messageThatIsBeingWritten == null) {
          messageThatIsBeingWritten = queue.poll();
          if (logger.level <= Logger.FINEST) logger.log("poll("+identifier.get()+") set messageThatIsBeingWritten = "+messageThatIsBeingWritten);
        }
        if (queue.size() >= (MAX_QUEUE_SIZE-1) && logger.level <= Logger.INFO) {
          logger.log(this+"polling from full queue (this is a good thing) "+messageThatIsBeingWritten);
        }      
        return messageThatIsBeingWritten;
      }
    }
    
    /**
     * True if we have a message to send
     * @return
     */
    private boolean haveMessageToSend() {
      if (messageThatIsBeingWritten == null && queue.isEmpty()) return false; 
      return true;
    }

    /**
     * This is called when the socket has an exception but was already opened.
     */
    public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
//      if (ioe instanceof NodeIsFaultyException) {
//        if (livenessProvider.getLiveness(identifier, )
//            
//        markDead();
//        return; 
//      }
      
      if (logger.level <= Logger.FINER) {
        logger.logException(this+".receiveException("+socket+","+ioe+"):"+messageThatIsBeingWritten+" wrS:"+writingSocket, ioe);
      } else if (logger.level <= Logger.INFO) logger.log(this+".receiveException("+socket+","+ioe+"):"+messageThatIsBeingWritten+" wrS:"+writingSocket+" "+ioe);
      registered = false;
      sockets.remove(socket);
      if (ioe instanceof ClosedChannelException) {
        // don't close, will get cleaned up by the reader
      } else {
        socket.close();
      }
        
      if (socket == writingSocket) {
        clearAndEnqueue(messageThatIsBeingWritten);
      }
      scheduleToWriteIfNeeded();
    }

    public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
      registered  = false;
      if (canRead || !canWrite) throw new IllegalStateException(this+" Expected only to write. canRead:"+canRead+" canWrite:"+canWrite+" socket:"+socket);
      if (socket != writingSocket) {
        // this is because the close() method calls receiveSelectResult
//        if (writingSocket == null) {          
//          scheduleToWriteIfNeeded();
//        }
//        logger.logException("receivedSelectResult("+socket+", r:"+canRead+" w:"+canWrite+") ws:"+writingSocket, new IOException());
        if (logger.level <= Logger.WARNING) logger.log("receivedSelectResult("+socket+", r:"+canRead+" w:"+canWrite+") ws:"+writingSocket);
        return;
      }
      
//      logger.log("receivedSelectResult("+socket+","+canRead+","+canWrite);
      if (logger.level <= Logger.FINEST) logger.log("receivedSelectResult("+socket+","+canRead+","+canWrite);
      MessageWrapper current = poll();
      while (current != null && current.receiveSelectResult(writingSocket)) {
        current = poll();
      }
      scheduleToWriteIfNeeded();
    }
    
    /**
     * TODO: The synchronization here may need work.
     * 
     * This is called while we are waiting to open the new socket.
     * 
     * @param handle
     * @param ex
     */
    public void receiveSocketException(SocketRequestHandleImpl<Identifier> handle, Exception ex) {      
//      if (ex instanceof NodeIsFaultyException) {
//        markDead();
//        return; 
//      }
      synchronized(EntityManager.this) {
        if (handle == pendingSocket) {        
          if (logger.level <= logger.INFO) logger.log(EntityManager.this+".receiveSocketException("+ex+") setting pendingSocket to null "+pendingSocket);

          stopLivenessChecker();
          pendingSocket = null; 
        }
      }
      scheduleToWriteIfNeeded();
    }

    /**
     * Enqueue the message.
     * @param ret
     */
    private void enqueue(MessageWrapper ret) {
//      logger.log("enqueue("+ret+")");
      synchronized(queue) {
        queue.add(ret);       
        
        // drop the lowest priority message if the queue is overflowing        
        while (queue.size() > MAX_QUEUE_SIZE) {
          MessageWrapper w = queue.removeLast();
          if (logger.level <= Logger.CONFIG) logger.log("Dropping "+w+" because queue is full. MAX_QUEUE_SIZE:"+MAX_QUEUE_SIZE);
          w.drop();
        }
      }
    }

    /**
     * This method is a keeper, but may need some additional functions, and/or error handling.
     *
     */
    public void markDead() {
      purge(new NodeIsFaultyException(identifier.get()));
    }
    
    public void purge(IOException ioe) {
      if (logger.level <= Logger.FINE) logger.log(this+"purge("+ioe+"):"+messageThatIsBeingWritten);
      ArrayList<Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper>> callSendFailed = 
        new ArrayList<Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper>>();
      synchronized(queue) {
        // return NodeIsFaultyException to all of the message(s) deliverAckToMe(s)
        if (messageThatIsBeingWritten != null) {
          messageThatIsBeingWritten.reset();
          if (messageThatIsBeingWritten.deliverAckToMe != null) {
            callSendFailed.add(new Tuple(messageThatIsBeingWritten.deliverAckToMe, messageThatIsBeingWritten));
//            messageThatIsBeingWritten.deliverAckToMe.sendFailed(messageThatIsBeingWritten, ioe);           
          }
          messageThatIsBeingWritten = null;
        }
        for (MessageWrapper msg : queue) {
          if (msg.deliverAckToMe != null) {
            callSendFailed.add(new Tuple(msg.deliverAckToMe, msg));
//            msg.deliverAckToMe.sendFailed(msg, ioe); 
          }
        }
        queue.clear();
      }
      
      for (Tuple<MessageCallback<Identifier, ByteBuffer>, MessageWrapper> t : callSendFailed) {
        t.a().sendFailed(t.b(), ioe);
      }
      
      synchronized(sockets) {
        for (P2PSocket<Identifier> sock : sockets) {
//          try {
            sock.close();
//          } catch (IOException ioe) {
//            if (logger.level <= Logger.WARNING) logger.logException("Error closing "+sock,ioe);
//          }
        }
        sockets.clear();
      }
      setWritingSocket(null/*, "purge"*/);
      synchronized(EntityManager.this) {
        if (logger.level <= logger.INFO) logger.log(EntityManager.this+".purge setting pendingSocket to null "+pendingSocket);
        if (pendingSocket != null) {
          stopLivenessChecker();
  //        logger.log("cancelling "+pendingSocket);
          pendingSocket.cancel();
        }

        pendingSocket = null;
      }
    }
    
    // ******************************* Big messages **************************** //
    Map<Identifier,PendingMessages> pendingBigMessages = new HashMap<Identifier, PendingMessages>();
    class PendingMessages implements SocketCallback<Identifier>, P2PSocketReceiver<Identifier> {
      Identifier i;
      
      /**
       * Put on back, get from front
       */
      LinkedList<PendingMessage> msgs = 
        new LinkedList<PendingMessage>();
      P2PSocket<Identifier> socket;
      ByteBuffer header;
      
      public PendingMessages(Identifier i) {
        this.i = i;
        header = ByteBuffer.wrap(BIG_MSG_SOCKET);
      }
      
      public void start(Map<String,Object> options) {
        if (logger.level <= Logger.FINER) logger.log("Opening BIG message socket to:"+i);
        tl.openSocket(i, this, options);
      }

      public MessageRequestHandle<Identifier, ByteBuffer> addMessage(
          ByteBuffer m, MessageCallback<Identifier, ByteBuffer> deliverAckToMe,
          Map<String, Object> options) {
        if (logger.level <= Logger.FINE) logger.log("Sending BIG message of size:"+m.remaining()+" to:"+i);
        PendingMessage ret = new PendingMessage(m,deliverAckToMe,options);
        msgs.addLast(ret);
        return ret;
      }

      public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
        receiveException(ex);
      }
      public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
        receiveException(ioe);
      }
      public void receiveException(Exception ex) {
        pendingBigMessages.remove(this);
        for (PendingMessage foo : msgs) {
          foo.sendFailed(ex);
        }
        if (socket != null) {
          P2PSocket<Identifier> temp = socket;
          socket = null;
          temp.close();
        }
      }

      public void receiveSelectResult(P2PSocket<Identifier> socket,
          boolean canRead, boolean canWrite) throws IOException {
        if (header.hasRemaining()) {
          if (socket.write(header) < 0) {
            if (logger.level <= Logger.WARNING) logger.log("Error writing BIG message header to:"+socket);
            receiveException(new org.mpisws.p2p.transport.ClosedChannelException("Socket closed before writing BIG header."));
            return;
          }
          if (header.hasRemaining()) {
            // keep trying to write the header
            socket.register(false, true, this);
            return;
          } else {
            if (logger.level <= Logger.FINER) logger.log("Wrote BIG message header to:"+socket);
          }
        }
        
        try {
          sendNextMessage();
        } catch (IOException ioe) {
          receiveException(ioe);
        }
      }

      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> sock) {  
        socket = sock;
        try {
          if (logger.level <= Logger.INFO) logger.log("Opened BIG message socket to:"+i);
          receiveSelectResult(sock, false, true);
        } catch (IOException ioe) {
          receiveException(ioe);
        }
      }    

      protected void sendNextMessage() throws IOException {
        if (msgs.isEmpty()) {
          socket.close();
          pendingBigMessages.remove(i);
          return;
        }
        msgs.getFirst().send();
        
      }
      
      class PendingMessage implements MessageRequestHandle<Identifier, ByteBuffer>, P2PSocketReceiver<Identifier> {
        ByteBuffer msg;
        MessageCallback<Identifier, ByteBuffer> deliverAckToMe;
        Map<String, Object> options;
        ByteBuffer sizeBuffer;
        
        boolean started = false;
        
        public PendingMessage(ByteBuffer msg, MessageCallback<Identifier, ByteBuffer> deliverAckToMe,Map<String, Object> options) {
          this.msg = msg;
          this.deliverAckToMe = deliverAckToMe;
          this.options = options;
          this.sizeBuffer = ByteBuffer.wrap(MathUtils.intToByteArray(msg.remaining()));        
        }
        
        public void sendFailed(Exception ex) {
          if (deliverAckToMe != null) {
            deliverAckToMe.sendFailed(this, ex);
          }
        }

        public void send() throws IOException {
          started = true;
          receiveSelectResult(socket, false, true);
        }

        public Identifier getIdentifier() {
          return i;
        }

        public ByteBuffer getMessage() {
          return msg;
        }

        public Map<String, Object> getOptions() {
          return options;
        }

        public boolean cancel() {
          if (!started) {
            msgs.remove(this);
            return true;
          }
          return false;
        }

        /**
         * The socket is blown, just fail them all.
         */
        public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
          PendingMessages.this.receiveException(ioe);
        }

        public void receiveSelectResult(P2PSocket<Identifier> socket,
            boolean canRead, boolean canWrite) throws IOException {
          if (sizeBuffer.hasRemaining()) {
            long ret = socket.write(sizeBuffer);
            if (ret == -1) {
              socket.close();
              receiveException(socket, new org.mpisws.p2p.transport.ClosedChannelException("Remote node closed channel: "+socket));
              return;
            }
            if (sizeBuffer.hasRemaining()) {
              socket.register(false, true, this);
              return;
            }
          }
          if (msg.hasRemaining()) {
            long ret = socket.write(msg);
            if (ret == -1) {
              socket.close();
              receiveException(socket, new org.mpisws.p2p.transport.ClosedChannelException("Remote node closed channel: "+socket));
              return;
            }            
            if (logger.level <= Logger.FINEST) logger.log("BIG message wrote: "+ret+" of "+msg.capacity());
            if (msg.hasRemaining()) {
              socket.register(false, true, this);
            } else {
              // done
              if (msgs.removeFirst() != this) throw new RuntimeException("Error, removing first was not this!"+this); 
              sendNextMessage();
            }
          }
        }
      }
    }
   
    /**
     * Read a sizeBuf, then a msgBuff, repeat
     * 
     * @param socket
     */
    protected void handleBigMsgSocket(P2PSocket<Identifier> socket) {
      if (logger.level <= Logger.INFO) logger.log("handling BIG message socket from:"+socket);
      try {
        new P2PSocketReceiver<Identifier>() {
          byte[] sizeBytes = new byte[4];
          ByteBuffer sizeBuf = ByteBuffer.wrap(sizeBytes);
          ByteBuffer msgBuf = null;
          
          public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
            errorHandler.receivedException(socket.getIdentifier(), ioe);
            socket.close();
          }
  
          public void receiveSelectResult(P2PSocket<Identifier> socket,
              boolean canRead, boolean canWrite) throws IOException {
            if (sizeBuf.hasRemaining()) {
              long ret = socket.read(sizeBuf);
              if (ret == -1) {
                socket.close();
                return;
              }
              if (sizeBuf.hasRemaining()) {
                socket.register(true, false, this);
                return;
              } else {
                int size = MathUtils.byteArrayToInt(sizeBytes);
                msgBuf = ByteBuffer.allocate(size);
                if (logger.level <= Logger.FINER) logger.log("Receiving BIG message of size:"+size+" from:"+socket);
                // continue
                if (size > MAX_BIG_MSG_SIZE) {
                  if (logger.level <= Logger.WARNING) logger.log("Closing socket, BIG message of size:"+size+" is too big! (max:"+MAX_BIG_MSG_SIZE+") from:"+socket);
                  socket.close();
                  return;
                }
              }
            }
            
            // msgBuf should not be null
            if (msgBuf.hasRemaining()) {
              long ret = socket.read(msgBuf);
              if (ret == -1) {
                socket.close();
                return;
              }
              if (msgBuf.hasRemaining()) {
                socket.register(true, false, this);
                return;
              } else {
                // done with this msg
                if (logger.level <= Logger.FINE) logger.log("Received BIG message of size:"+msgBuf.capacity()+" from:"+socket);
                msgBuf.flip();
                sizeBuf.clear();
                callback.messageReceived(socket.getIdentifier(), msgBuf, socket.getOptions());
                msgBuf = null;
                socket.register(true, false, this);    
                return;
              }
            }            
          }      
        }.receiveSelectResult(socket, true, false);
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    // ************************* End handle big messages **********************
    
    /**
     * Note: We got to get rid of all the calls to poll().
     *  
     * @param message
     * @param deliverAckToMe
     * @param options
     * @return
     */
    public MessageRequestHandle<Identifier, ByteBuffer> send(
        Identifier temp,
        ByteBuffer message, 
        MessageCallback<Identifier, ByteBuffer> deliverAckToMe, 
        final Map<String, Object> options) {      
      if (logger.level <= Logger.FINER) logger.log(this+"send("+message+")");

      // pick the priority
      int priority = DEFAULT_PRIORITY;
      if (options != null) {
        if (options.containsKey(OPTION_PRIORITY)) {
          priority = ((Integer)options.get(OPTION_PRIORITY)).intValue();          
        }
      }

      MessageWrapper ret;

      // throw an error if it's too large
      int remaining = message.remaining();
      if (remaining > MAX_MSG_SIZE) {
        // open a special socket for big messages (the new policy, Dec-2008)
        if (remaining > MAX_BIG_MSG_SIZE) {
          ret = new MessageWrapper(temp, message, deliverAckToMe, options, priority, 0);
          if (deliverAckToMe != null) 
            deliverAckToMe.sendFailed(ret, 
              new SocketException("Message too large. msg:"+message+" size:"+remaining+" max:"+Math.max(MAX_MSG_SIZE, MAX_BIG_MSG_SIZE)));
  
          return ret;           
        }
        
        PendingMessages pm = pendingBigMessages.get(temp);
        if (pm == null) {
          pm = new PendingMessages(temp);
          // note, this fixes a timing issue, because the socket is closed if there are no pending big messages
          MessageRequestHandle<Identifier, ByteBuffer> ret2 = pm.addMessage(message,deliverAckToMe,options);          
          pendingBigMessages.put(temp, pm);
          pm.start(options);
          return ret2;
        } else {
          return pm.addMessage(message,deliverAckToMe,options);              
        }
        

        // drop policy (the old policy):
//        ret = new MessageWrapper(temp, message, deliverAckToMe, options, priority, 0);
//        if (deliverAckToMe != null) 
//          deliverAckToMe.sendFailed(ret, 
//            new SocketException("Message too large. msg:"+message+" size:"+remaining+" max:"+MAX_MSG_SIZE));
//
//        return ret; 
      }
      
      // make sure it's alive
      if (livenessProvider.getLiveness(temp, options) >= LIVENESS_DEAD) {
        ret = new MessageWrapper(temp, message, deliverAckToMe, options, priority, 0);
        if (deliverAckToMe != null) 
          deliverAckToMe.sendFailed(ret, new NodeIsFaultyException(temp, message));
        return ret;
      }
      
      // enqueue the message
      ret = new MessageWrapper(temp, message, deliverAckToMe, options, priority, seq++);        
      notifyListenersEnqueued(ret.originalSize, temp, options);
      enqueue(ret);
      if (selectorManager.isSelectorThread()) {
        scheduleToWriteIfNeeded();
      } else {
        selectorManager.invoke(new Runnable() { public void run() {scheduleToWriteIfNeeded();}});
      }
      
      return ret;
    }

    protected boolean complete(MessageWrapper wrapper) {
      if (logger.level <= Logger.FINE) logger.log(this+".complete("+wrapper+")");
      if (wrapper != messageThatIsBeingWritten) throw new IllegalArgumentException("Wrapper:"+wrapper+" messageThatIsBeingWritten:"+messageThatIsBeingWritten);
      
      synchronized(queue) {
        messageThatIsBeingWritten = null;
      }
      
      // notify deliverAckToMe
      wrapper.complete();
      
      // close the socket if we need to 
      if (closeWritingSocket == writingSocket) {
        writingSocket.close();
        setWritingSocket(null/*, "complete("+wrapper+")"*/);
        closeWritingSocket = null;          
        return false;
      }
      return true;
    }

    public void clearAndEnqueue(MessageWrapper wrapper) {
      if (wrapper != messageThatIsBeingWritten) throw new IllegalArgumentException("Wrapper:"+wrapper+" messageThatIsBeingWritten:"+messageThatIsBeingWritten);
      synchronized(queue) {
        if (messageThatIsBeingWritten != null) messageThatIsBeingWritten.reset();
        messageThatIsBeingWritten = null;
        if (writingSocket != null) {
  //        writingSocket.close();
          sockets.remove(writingSocket);
          setWritingSocket(null/*, "CaE("+wrapper+")"*/);
        }
        if (wrapper != null) {
          wrapper.reset();
          enqueue(wrapper);      
        }
      }
    }

    class MessageWrapper implements 
        Comparable<MessageWrapper>, 
        MessageRequestHandle<Identifier, ByteBuffer> {
      int priority;
      int seq;
      Identifier myIdentifier;
      
      P2PSocket socket; // null if we aren't registered, aka, we aren't pending/writing
      
      ByteBuffer originalMessage;
      ByteBuffer message;
      MessageCallback<Identifier, ByteBuffer> deliverAckToMe;
      Map<String, Object> options;      
      int originalSize;
      boolean cancelled = false; // true when cancel is called
      boolean completed = false; // true when completed is called
      
      MessageWrapper(
          Identifier temp,
          ByteBuffer message, 
          MessageCallback<Identifier, ByteBuffer> deliverAckToMe, 
          Map<String, Object> options, int priority, int seq) {
        this.originalSize = message.remaining();
//        if (options == null) throw new RuntimeException("options is null");  // debugging
        
        this.myIdentifier = temp;
        this.originalMessage = message;

        // head the message with the size
        int size = message.remaining();
        this.message = ByteBuffer.allocate(message.remaining()+4);
        this.message.put((byte)((size >>> 24) & 0xFF));
        this.message.put((byte)((size >>> 16) & 0xFF));
        this.message.put((byte)((size >>>  8) & 0xFF));
        this.message.put((byte)((size >>>  0) & 0xFF));
        this.message.put(message);
        this.message.clear();
        
        this.deliverAckToMe = deliverAckToMe;
        this.options = options;
        this.priority = priority;
        this.seq = seq;      
      }
      
      public MessageInfo getMessageInfo() {
        return new MessageInfoImpl(originalMessage,options,priority);
      }
      
      public void complete() {
        completed = true;
        if (deliverAckToMe != null) deliverAckToMe.ack(this);
        notifyListenersWrote(originalSize, myIdentifier, options);        
      }

      /**
       * When is this registered?  May be registered too often.
       * 
       * @return true if should keep writing
       */
      public boolean receiveSelectResult(P2PSocket<Identifier> socket) throws IOException {
        if (logger.level <= Logger.FINEST) logger.log(this+".receiveSelectResult("+socket+")");
        try {
        if (this.socket != null && this.socket != socket) {
          // this shouldn't happen
          if (logger.level <= Logger.WARNING) logger.log(this+" Socket changed!!! can:"+cancelled+" comp:"+completed+" socket:"+socket+" writingSocket:"+writingSocket+" this.socket:"+this.socket);
          socket.shutdownOutput();
          
          // do we need to reset?
          return false;
        }

//        if (socket == null) logger.log("Starting to write "+this+" on "+socket);
        
        // in case we don't complete the write, remember where we are writing
        this.socket = socket;
        
        if (cancelled && message.position() == 0) {
          if (logger.level <= Logger.FINEST) logger.log(this+".rsr("+socket+") cancelled"); 
          // cancel
          return true;
        } else {
          long bytesWritten;
          if ((bytesWritten = socket.write(message)) == -1) {
            // socket was closed, need to register new socket
            if (logger.level <= Logger.FINEST) logger.log(this+".rsr("+socket+") socket was closed"); 
            clearAndEnqueue(this); //             messageThatIsBeingWritten = null;            
            return false;
          }
          if (logger.level <= Logger.FINER) logger.log(this+" wrote "+bytesWritten+" bytes of "+message.capacity()+" remaining:"+message.remaining());

          if (message.hasRemaining()) {
            if (logger.level <= Logger.FINEST) logger.log(this+".rsr("+socket+") has remaining"); 
            return false;
          }
        }
                
        return EntityManager.this.complete(this); 
        } catch (IOException ioe) {
          // note, clearAndEnqueue() gets called later by the writer when the stack unravels again
          if (logger.level <= Logger.FINEST) logger.logException(this+".rsr("+socket+")", ioe);
          throw ioe;
        }
      }
      
      public void drop() {
        // TODO: make sure we've done evrything necessary here to clean this up        
        if (deliverAckToMe != null) deliverAckToMe.sendFailed(this, new QueueOverflowException(identifier.get(), originalMessage));
        notifyListenersDropped(originalSize, myIdentifier, options);
      }
            
      /**
       * Compares first on priority, second on seq.
       */
      public int compareTo(MessageWrapper that) {
        if (this.priority == that.priority) {
          return this.seq-that.seq;        
        }
        return this.priority-that.priority;
      }

      public Identifier getIdentifier() {
        return myIdentifier;
      }

      public ByteBuffer getMessage() {
        return originalMessage;
      }

      public Map<String, Object> getOptions() {
        return options;
      }
      
      public void reset() {
        message.clear();
        socket = null;
      }
      
      public boolean cancel() {
        cancelled = true;
        synchronized(queue) {
          if (this.equals(messageThatIsBeingWritten)) {
            if (message.position() == 0) {
              // TODO: can still cancel the message, but have to have special behavior when the socket calls us back 
              messageThatIsBeingWritten = null;
              return true;
            } else {
              return false;
            }
          }
          return queue.remove(this);
        }
      }
      public String toString() {
        return "MessagWrapper{"+message+"}@"+System.identityHashCode(this)+"->"+identifier.get()+" pri:"+priority+" seq:"+seq+" s:"+this.socket; 
      }

    }
    
    // *********************** Reader ************************
    
    /**
     * Reads the size of the object, then launches a new ObjectReader with the appropriate buffer size.
     * 
     * @author Jeff Hoye
     */
    class SizeReader extends BufferReader {
      
      public SizeReader(P2PSocket<Identifier> socket) {
        super(4, socket); 
      }
      
      @Override
      public void done(P2PSocket<Identifier> socket) throws IOException {
        int msgSize = buf.asIntBuffer().get();
        if (logger.level <= Logger.FINER) logger.log(EntityManager.this+" reading message of size "+msgSize);

        if (msgSize > MAX_MSG_SIZE) {
          if (logger.level <= Logger.WARNING) logger.log(socket+" attempted to send a message of size "+msgSize+". MAX_MSG_SIZE = "+MAX_MSG_SIZE);
          closeMe(socket);
          return;
        }
        
        new BufferReader(msgSize, socket);
      }
      
      public String toString() {
        return "SizeReader";
      }
    }
    
    /**
     * Reads into the buf, or closes the socket.
     * 
     * @author Jeff Hoye
     */
    class BufferReader implements P2PSocketReceiver<Identifier> {
      ByteBuffer buf;
      
      public BufferReader(int size, P2PSocket<Identifier> socket) {
        buf = ByteBuffer.allocate(size);
        socket.register(true, false, this);
      }
      
      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
        if (canWrite || !canRead) throw new IllegalStateException(EntityManager.this+" Expected only to read. canRead:"+canRead+" canWrite:"+canWrite+" socket:"+socket);
        
        try {
          if (socket.read(buf) == -1) {
            closeMe(socket);
            return;
          }
        } catch (IOException ioe) {
          receiveException(socket, ioe);
          return;
        }
        
        if(buf.remaining() == 0) {
          buf.flip();
          done(socket);
        } else {
          socket.register(true, false, this); 
        }        
      }
      
      public void receiveException(P2PSocket<Identifier> socket, Exception e) {
//        if (e instanceof NodeIsFaultyException) {
//          markDead();
//          return; 
//        }

        if (e instanceof ClosedChannelException) {
          return;
        }
        
        boolean printError = true;
        
        if (e instanceof NodeIsFaultyException) {
          printError = false;
        }
        
        if (e instanceof IOException) {
          if (e.getMessage().equals("An established connection was aborted by the software in your host machine")) {
            printError = false;
          }
        }
        
        if (printError) {
          errorHandler.receivedException(socket.getIdentifier(), e);
        }
        closeMe(socket);
      }                    
      
      public void done(P2PSocket<Identifier> socket) throws IOException {
        if (logger.level <= Logger.FINE) logger.log(EntityManager.this+" read message of size "+buf.capacity()+" from "+socket);        
        notifyListenersRead(buf.capacity(), socket.getIdentifier(), socket.getOptions());
        callback.messageReceived(socket.getIdentifier(), buf, socket.getOptions()); 
        new SizeReader(socket);
      }
      
      public String toString() {
        return "BufferReader{"+buf+"}";
      }
    }

    public int queueLength() {
      int ret = queue.size();
      if (messageThatIsBeingWritten != null) ret++;
      return ret;
    }
    
    public long bytesPending() {
      long ret = 0;
      synchronized(queue) {
        if (messageThatIsBeingWritten != null) {
          ret+=messageThatIsBeingWritten.message.remaining();        
        }
        for (MessageWrapper foo : queue) {
          ret+=foo.message.remaining();
        }
      }
      return ret;
    }
    
    public List<MessageInfo> getPendingMessages() {
      synchronized(queue) {
        ArrayList<MessageInfo> ret = new ArrayList<MessageInfo>(queue.size());
//        if (messageThatIsBeingWritten != null) {
//          ret+=messageThatIsBeingWritten.message.remaining();        
//        }
        for (MessageWrapper foo : queue) {
          ret.add(foo.getMessageInfo());
        }
        return ret;
      }
    }
  } // EntityManager
  
  // *************************** Listeners *********************
  /**
   * Note that listeners contains a superset of plisteners
   */
  ArrayList<TransportLayerListener<Identifier>> listeners = new ArrayList<TransportLayerListener<Identifier>>();
  ArrayList<PriorityTransportLayerListener<Identifier>> plisteners = new ArrayList<PriorityTransportLayerListener<Identifier>>();
  
  public void addTransportLayerListener(TransportLayerListener<Identifier> listener) {
    synchronized(listeners) { 
      listeners.add(listener);
    }
  }
  public void removeTransportLayerListener(TransportLayerListener<Identifier> listener) {
    synchronized(listeners) { 
      listeners.remove(listener);
    }
  }
  
  public void addPriorityTransportLayerListener(PriorityTransportLayerListener<Identifier> listener) {
    synchronized(plisteners) { 
      plisteners.add(listener);
    }
    addTransportLayerListener(listener);
  }
  public void removePriorityTransportLayerListener(PriorityTransportLayerListener<Identifier> listener) {
    synchronized(plisteners) { 
      plisteners.remove(listener);
    }
    removeTransportLayerListener(listener);
  }
  
  public void notifyListenersRead(int size, Identifier source,
      Map<String, Object> options) {

    if (listeners.isEmpty()) return;
    ArrayList<TransportLayerListener<Identifier>> temp;
    
    synchronized(listeners) { 
      temp = new ArrayList<TransportLayerListener<Identifier>>(listeners);
    }
    for (TransportLayerListener<Identifier> l : temp) {
      l.read(size, source, options, true, true);
    }
  }

  public void notifyListenersWrote(int size, Identifier dest,
      Map<String, Object> options) {
    if (listeners.isEmpty()) return;
    ArrayList<TransportLayerListener<Identifier>> temp;
    
    synchronized(listeners) { 
      temp = new ArrayList<TransportLayerListener<Identifier>>(listeners);
    }
    for (TransportLayerListener<Identifier> l : temp) {
      l.wrote(size, dest, options, true, true);
    }
  }

  public void notifyListenersEnqueued(int size, Identifier dest,
      Map<String, Object> options) {
    if (plisteners.isEmpty()) return;
    ArrayList<PriorityTransportLayerListener<Identifier>> temp;
    
    synchronized(plisteners) { 
      temp = new ArrayList<PriorityTransportLayerListener<Identifier>>(plisteners);
    }
    for (PriorityTransportLayerListener<Identifier> l : temp) {
      l.enqueued(size, dest, options, true, true);
    }
  }

  public void notifyListenersDropped(int size, Identifier dest,
      Map<String, Object> options) {
    if (plisteners.isEmpty()) return;
    ArrayList<PriorityTransportLayerListener<Identifier>> temp;
    
    synchronized(plisteners) { 
      temp = new ArrayList<PriorityTransportLayerListener<Identifier>>(plisteners);
    }
    for (PriorityTransportLayerListener<Identifier> l : temp) {
      l.dropped(size, dest, options, true, true);
    }
  }

  
  // ********************************** introspection ***********************
  public long bytesPending(Identifier i) {
    return getEntityManager(i).bytesPending();
  }

  public int queueLength(Identifier i) {
    return getEntityManager(i).queueLength();
  }
  
  public List<MessageInfo> getPendingMessages(Identifier i) {
    return getEntityManager(i).getPendingMessages();
  }
  
  public Collection<Identifier> nodesWithPendingMessages() {
    ArrayList<Identifier> ret = new ArrayList<Identifier>();
    synchronized(entityManagers) {
      for(EntityManager m : entityManagers.values()) {
        if (m.peek() != null) {
          ret.add(m.identifier.get());
        }
      }
    }
    return ret;
  }
  
  /**
   * 
   */
  public Map<String, Object> connectionOptions(Identifier i) {
    // This is written with temp variables to simplify the synchronization problem
    EntityManager manager = getEntityManager(i);
    P2PSocket<Identifier> temp = manager.writingSocket;
    if (temp != null) {
      return temp.getOptions();
    }
    SocketRequestHandle<Identifier> temp2 = manager.pendingSocket;
    if (temp2 != null) return temp2.getOptions();
    return null;
  }
  
  public int connectionStatus(Identifier i) {
    EntityManager manager = getEntityManager(i);
    // this may not be thread safe, but ... who cares, it at least won't throw an exception.
    if (manager.writingSocket != null) return STATUS_CONNECTED;
    if (manager.pendingSocket != null) return STATUS_CONNECTING;
    return STATUS_NOT_CONNECTED;
  }
  
  public void openPrimaryConnection(Identifier i, Map<String, Object> options) {
    getEntityManager(i).openPrimarySocketHelper(i, options);
  }

  public void addPrimarySocketListener(PrimarySocketListener<Identifier> listener) {
    primarySocketListeners.add(listener);
  }
  
  public void removePrimarySocketListener(PrimarySocketListener<Identifier> listener) {
    primarySocketListeners.remove(listener);
  }

}