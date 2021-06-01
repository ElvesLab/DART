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
package org.mpisws.p2p.transport.sourceroute.manager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.PingListener;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.proximity.ProximityListener;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.manager.SourceRouteManagerImpl.AddressManager.PendingMessage;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.util.TimerWeakHashMap;

/**
 * This class adapts a SourceRoute transport layer back to an Identifier
 * layer.  It does this by selecting an appropriate source route from a SourceRouteStrategy, 
 * which generates SourceRoutes to try.
 * 
 * @author Jeff Hoye
 *
 */
public class SourceRouteManagerImpl<Identifier> implements 
    SourceRouteManager<Identifier>, 
    TransportLayerCallback<SourceRoute<Identifier>, ByteBuffer>, 
    LivenessListener<SourceRoute<Identifier>>, 
    ProximityListener<SourceRoute<Identifier>> {
  
  // the default distance, which is used before a ping
  public static final int DEFAULT_PROXIMITY = 60*60*1000;
  // the minimum amount of time between pings
  public long PING_THROTTLE;
  public int NUM_SOURCE_ROUTE_ATTEMPTS;
  public int CHECK_LIVENESS_THROTTLE = 5000;

  TransportLayer<SourceRoute<Identifier>, ByteBuffer> tl;
  LivenessProvider<SourceRoute<Identifier>> livenessProvider; 
  ProximityProvider<SourceRoute<Identifier>> proxProvider;
  SourceRouteStrategy<Identifier> strategy;
  Environment environment;
  Logger logger;
  Identifier localAddress;
  Map<Identifier, AddressManager> addressManagers;
  private TransportLayerCallback<Identifier, ByteBuffer> callback;  
  private ErrorHandler<Identifier> errorHandler;
  Set<AddressManager> hardLinks;
  List<LivenessListener<Identifier>> livenessListeners;
  List<PingListener<Identifier>> pingListeners;
  SourceRouteFactory<Identifier> srFactory;
  
  public SourceRouteManagerImpl(
      SourceRouteFactory<Identifier> srFactory,
      TransportLayer<SourceRoute<Identifier>, ByteBuffer> tl, 
      LivenessProvider<SourceRoute<Identifier>> livenessProvider, 
      ProximityProvider<SourceRoute<Identifier>> proxProvider, 
      Environment env, 
      SourceRouteStrategy<Identifier> strategy) {
    
    if (tl == null) throw new IllegalArgumentException("tl == null");
    if (proxProvider == null) throw new IllegalArgumentException("proxProvider == null");
    if (strategy == null) throw new IllegalArgumentException("strategy == null");
    
    this.tl = tl;
    this.livenessProvider = livenessProvider;
    this.proxProvider = proxProvider;
    this.proxProvider.addProximityListener(this);
    this.strategy = strategy;
    this.environment = env;
    this.logger = env.getLogManager().getLogger(SourceRouteManagerImpl.class, null);
    this.srFactory = srFactory;
    this.localAddress = tl.getLocalIdentifier().getFirstHop();    
    tl.setCallback(this);
    livenessProvider.addLivenessListener(this);
    //    addressManagers = new TimerWeakHashMap<Identifier, AddressManager>(environment.getSelectorManager().getTimer(),30000);
    
    addressManagers = new HashMap<Identifier, AddressManager>();
    Parameters p = environment.getParameters();
    PING_THROTTLE = p.getLong("pastry_socket_srm_ping_throttle");
    NUM_SOURCE_ROUTE_ATTEMPTS = p.getInt("pastry_socket_srm_num_source_route_attempts");
    hardLinks = new HashSet<AddressManager>();
    livenessListeners = new ArrayList<LivenessListener<Identifier>>();
    pingListeners = new ArrayList<PingListener<Identifier>>();
  }
  
  /**
   * Method which sends a message across the wire.
   *
   * @param message The message to send
   * @param address The address to send the message to
   */
//  public void send(final Identifier address, final SocketBuffer message) {    
  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(
      Identifier i, 
      ByteBuffer m, 
      MessageCallback<Identifier, ByteBuffer> deliverAckToMe, 
      Map<String, Object> options) {
    return getAddressManager(i).sendMessage(m, deliverAckToMe, options);
  }

  /**
   * Internal method which returns (or builds) the manager associated
   * with an address
   *
   * @param address The remote address
   */
//  Map<Identifier, Identifier> delmeAddrManagers = new HashMap<Identifier, Identifier>();
  protected AddressManager getAddressManager(Identifier address) {
    synchronized(addressManagers) {
      AddressManager manager = addressManagers.get(address); 
      
      if (manager == null) {
//        delmeAddrManagers.put(address, address);
        manager = new AddressManager(address);
        addressManagers.put(address, manager);
      } else {
//        Identifier key = delmeAddrManagers.get(address);
//        if (key != address) {
//          throw new IllegalArgumentException("key != address");
//        }
      }
      
      return manager;
    }
  }

  
  public void clearState(Identifier i) {
    getAddressManager(i).clearLivenessState();
  }

  public void addHardLink(AddressManager am) {
    synchronized(hardLinks) {
      hardLinks.add(am); 
    }
  }
  
  public void removeHardLink(AddressManager am) {
    synchronized(hardLinks) {
      hardLinks.remove(am); 
    }
  }
  
  /**
   * Should be called while synchronized on nodeHandles
   * 
   * @param address
   * @param manager
   */
//  public AddressManager putAddressManager(Identifier address,
//      boolean search) {
//
//    AddressManager manager = addressManagers.get(address);
//    if (manager == null) {
//      manager = new AddressManager(address, search);
//    }
//    return manager;
//  }
  

  
  /**
   * Method which sends a message across the wire.
   *
   * @param message The message to send
   * @param address The address to send the message to
   */
//  public void connect(final Identifier address, final int appAddress, final AppSocketReceiver receiver, final int timeout) {
  public SocketRequestHandle<Identifier> openSocket(
      Identifier i, 
      SocketCallback<Identifier> deliverSocketToMe, 
      Map<String, Object> options) {
    if (logger.level <= Logger.INFO-50) logger.log("openSocket("+i+","+deliverSocketToMe+","+options+")");
    return getAddressManager(i).openSocket(deliverSocketToMe, options);
  }
  
  /**
   * Method which suggests a ping to the remote node.
   *
   * @param address DESCRIBE THE PARAMETER
   * @param prl DESCRIBE THE PARAMETER
   */
//  public boolean ping(Identifier address, Map<String, Object> options) {
//    return getAddressManager(address).ping(options);
//  } 
  
  /**
   * Method which FORCES a check of liveness of the remote node.  Note that
   * this method should ONLY be called by internal Pastry maintenance algorithms - 
   * this is NOT to be used by applications.  Doing so will likely cause a
   * blowup of liveness traffic.
   *
   * @return true if node is currently alive.
   */
  public boolean checkLiveness(Identifier address, Map<String, Object> options) {
    return getAddressManager(address).checkLiveness(options);
  }

  /**
   * Method which returns the last cached liveness value for the given address.
   * If there is no cached value, then LIVENESS_ALIVE
   *
   * @param address The address to return the value for
   * @return The liveness value
   */
  public int getLiveness(Identifier address, Map<String, Object> options) {
    return getAddressManager(address).getLiveness(options);
  }  
   
  /**
   * Method which returns the last cached proximity value for the given address.
   * If there is no cached value, then DEFAULT_PROXIMITY is returned.
   *
   * @param address The address to return the value for
   * @return The ping value to the remote address
   */
  public int proximity(Identifier address, Map<String, Object> options) {
    return getAddressManager(address).proximity(options);
  }
  
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  public Identifier getLocalIdentifier() {
    return localAddress;
  }

  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;    
  }

  public void destroy() {
    tl.destroy();
  }
  
  /**
   * Internal class which is tasked with maintaining the status of a single
   * remote address.  This class is in charge of all source routes to that address,
   * as well as declaring liveness/death of this address
   */
  protected class AddressManager {
    
    // the remote address of this manager
    protected Identifier address;
    
    /**
     * the current best route to this remote address
     * 
     * if best == null, we are already in a CheckDead, which means
     * we are searching for a path
     */
    protected SourceRoute<Identifier> best;
    
    /** 
     *  the queue of messages waiting for a route
     *  
     *  of SocketBuffer
     */
    protected LinkedList<PendingMessage> pendingMessages;
    
    // the queue of appSockets waiting for a connection
    protected LinkedList<PendingSocket> pendingSockets;
    
    // the current liveness of this address
    protected int liveness;
    
    // the last time this address was pinged
    protected long updated;
    
    public static final int LIVENESS_UNKNOWN = -1;
    
    HashSet<SourceRoute<Identifier>> routes = new HashSet<SourceRoute<Identifier>>();
    
    /**
     * Constructor, given an address and whether or not it should attempt to
     * find the best route
     *
     * @param address The address
     * @param search Whether or not the manager should try and find a route
     */
    public AddressManager(Identifier address) {
      this.address = address;
      this.pendingMessages = new LinkedList<PendingMessage>();
      this.pendingSockets = new LinkedList<PendingSocket>();
      
      if (logger.level <= Logger.FINE) logger.log("new AddressManager("+address+")");      
      clearLivenessState();
      
//      this.liveness = LIVENESS_UNKNOWN; // don't stay dead forever... we may have a new connection
//      this.updated = 0L;
//      best = srFactory.getSourceRoute(localAddress, address);
//      routes.add(best);
            
    }
    
    public void clearLivenessState() {
//      synchronized(routes) {
      ArrayList<SourceRoute<Identifier>> temp = new ArrayList<SourceRoute<Identifier>>(routes);
      routes.clear();
//      }
      
      for (SourceRoute<Identifier> sr : temp) {
        livenessProvider.clearState(sr);
        proxProvider.clearState(sr);
      }

      // these may never get called due to the above clearing of the source routes
      if (!pendingMessages.isEmpty() || !pendingSockets.isEmpty()) {
        Exception reason = new NodeIsFaultyException(this.address, "State cleared. for "+this);
        
        ArrayList<PendingSocket> temp3 = new ArrayList<PendingSocket>(pendingSockets); 
        pendingSockets.clear();
        for (PendingSocket foo : temp3) {
          if (logger.level <= Logger.FINE) logger.log(this+".clearLivenessState()1 "+foo);
          foo.fail(reason);
        }
        
        ArrayList<PendingMessage> temp2 = new ArrayList<PendingMessage>(pendingMessages); 
        pendingMessages.clear();
        for (PendingMessage foo : temp2) {
          if (logger.level <= Logger.FINE) logger.log(this+".clearLivenessState()2 "+foo);
          foo.fail(reason);
        }
      }
      
      this.liveness = LIVENESS_UNKNOWN; // don't stay dead forever... we may have a new connection
      this.updated = 0L;
      best = srFactory.getSourceRoute(localAddress, address);
      routes.add(best);
    }

    class PendingSocket implements SocketRequestHandle<Identifier>, SocketCallback<SourceRoute<Identifier>> {
      private SocketCallback<Identifier> deliverSocketToMe;
      private Map<String, Object> options;
      private Cancellable cancellable;
      
      public PendingSocket(SocketCallback<Identifier> deliverSocketToMe, Map<String, Object> options) {
        this.deliverSocketToMe = deliverSocketToMe;
        this.options = options;        
      }

      public void receiveResult(SocketRequestHandle<SourceRoute<Identifier>> cancellable, P2PSocket<SourceRoute<Identifier>> sock) {
        deliverSocketToMe.receiveResult(this, new SourceRouteManagerP2PSocket<Identifier>(sock, logger, errorHandler, environment));
      }
      
      public void receiveException(SocketRequestHandle<SourceRoute<Identifier>> s, Exception ex) {
        deliverSocketToMe.receiveException(this, ex);
      }        

      public void fail(Exception ex) {
        cancel();
        receiveException(null, ex);
      }        
      
      public boolean cancel() {
        if (cancellable == null) {
          return pendingSockets.remove(this);
        } 
        return cancellable.cancel();
      }

      public Identifier getIdentifier() {
        return address;
      }

      public Map<String, Object> getOptions() {
        return options;
      }      
    }
    
    class PendingMessage implements MessageRequestHandle<Identifier, ByteBuffer>, MessageCallback<SourceRoute<Identifier>, ByteBuffer> {
      private ByteBuffer message;
      private MessageCallback<Identifier, ByteBuffer> deliverAckToMe;
      private Map<String, Object> options;
      private Cancellable cancellable;
      
      public PendingMessage(ByteBuffer message, MessageCallback<Identifier, ByteBuffer> deliverAckToMe, Map<String, Object> options) {
        this.message = message;
        this.deliverAckToMe = deliverAckToMe;
        this.options = options;        
      }

      public boolean cancel() {
        if (cancellable == null) {
          return pendingMessages.remove(this);
        } 
        return cancellable.cancel();
      }      
      
      public Map<String, Object> getOptions() {
        return options;
      }

      public Identifier getIdentifier() {
        return address;
      }

      public ByteBuffer getMessage() {
        return message;
      }

      public void ack(MessageRequestHandle<SourceRoute<Identifier>, ByteBuffer> msg) {
        deliverAckToMe.ack(this);
      }

      public void sendFailed(MessageRequestHandle<SourceRoute<Identifier>, ByteBuffer> msg, Exception reason) {
        deliverAckToMe.sendFailed(this, reason);        
      }
      
      public void fail(Exception reason) {
        cancel();
        sendFailed(null, reason);
      }
    }
    
    /**
     * Method which returns the last cached proximity value for the given address.
     * If there is no cached value, then DEFAULT_PROXIMITY is returned.
     *
     * @param address The address to return the value for
     * @return The ping value to the remote address
     */
    public int proximity(Map<String, Object> options) {
      if (best == null)
        return DEFAULT_PROXIMITY;
      else
        return proxProvider.proximity(best, options);
    }  
    
//    public int rto() {
//      if (best == null)
//        return DEFAULT_RTO;
//      else
//        return getRouteManager(best).rto();
//    }  
//    
    /**
     * Method which returns the last cached liveness value for the given address.
     * If there is no cached value, then true is returned.
     *
     * @param address The address to return the value for
     * @return The Alive value
     */
    public int getLiveness(Map<String, Object> options) {
      if (liveness == LIVENESS_UNKNOWN) {
        // don't ping too often while we're already waiting for a response
        if (environment.getTimeSource().currentTimeMillis() >= this.updated+CHECK_LIVENESS_THROTTLE) {
          checkLiveness(options);
        }
//        SourceRoute direct = srFactory.getSourceRoute(localAddress, address);
//        tl.checkLiveness(direct, options);
//        this.updated = environment.getTimeSource().currentTimeMillis();
        return LIVENESS_SUSPECTED;
      }
      return liveness;
    } 
    

    /**
     * Method which enqueues a message to this address
     *
     * @param message The message to send
     */
    public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(
        ByteBuffer message, 
        final MessageCallback<Identifier, ByteBuffer> deliverAckToMe, 
        Map<String, Object> options) {
      
      // if we're dead, we go ahead and just checkDead on the direct route
      if (liveness == LIVENESS_DEAD) {
        livenessProvider.checkLiveness(srFactory.getSourceRoute(getLocalIdentifier(), address), options);
        this.updated = environment.getTimeSource().currentTimeMillis();
      }
      
      // and in any case, we either send if we have a best route or add the message
      // to the queue
      if (best == null) {
        PendingMessage pending = new PendingMessage(message, deliverAckToMe, options);
        pendingMessages.addLast(pending);
        addHardLink(this);
        return pending;
      }
      
      final MessageRequestHandleImpl<Identifier, ByteBuffer> handle 
        = new MessageRequestHandleImpl<Identifier, ByteBuffer>(address, message, options);
      handle.setSubCancellable(tl.sendMessage(best, message, new MessageCallback<SourceRoute<Identifier>, ByteBuffer>(){
        public void ack(MessageRequestHandle<SourceRoute<Identifier>, ByteBuffer> msg) {
          if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
          if (deliverAckToMe != null) deliverAckToMe.ack(handle);
        }
            
        public void sendFailed(MessageRequestHandle<SourceRoute<Identifier>, ByteBuffer> msg, Exception ex) {
          if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
          if (deliverAckToMe == null) {
            errorHandler.receivedException(address, ex);
          } else {
            deliverAckToMe.sendFailed(handle, ex);
          }
        }      
      }, options));      
      return handle;
    }    
    
    /**
     * Method which opens an app socket to this address
     *
     * @param message The message to send
     */
//    public synchronized void connect(int appAddress, AppSocketReceiver receiver, int timeout) {
    public SocketRequestHandle<Identifier> openSocket(
        final SocketCallback<Identifier> deliverSocketToMe, 
        Map<String, Object> options) {
      if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");
      // if we're dead, we go ahead and just checkDead on the direct route
      if (liveness == LIVENESS_DEAD) {
        livenessProvider.checkLiveness(srFactory.getSourceRoute(getLocalIdentifier(),address), options);
        this.updated = environment.getTimeSource().currentTimeMillis();
      }
      
      // and in any case, we either send if we have a best route or add the message
      // to the queue
      
      if (best == null) {
        PendingSocket pending = new PendingSocket(deliverSocketToMe, options);
        pendingSockets.addLast(pending);
        addHardLink(this);
        return pending;
      } else {
        final SocketRequestHandleImpl<Identifier> handle = 
          new SocketRequestHandleImpl<Identifier>(address, options, logger);
        
        handle.setSubCancellable(tl.openSocket(best, new SocketCallback<SourceRoute<Identifier>>(){        
          public void receiveResult(
              SocketRequestHandle<SourceRoute<Identifier>> cancellable, 
              P2PSocket<SourceRoute<Identifier>> sock) {
            deliverSocketToMe.receiveResult(handle, new SourceRouteManagerP2PSocket<Identifier>(sock, logger, errorHandler, environment));
          }        
          public void receiveException(SocketRequestHandle<SourceRoute<Identifier>> s, Exception ex) {
            deliverSocketToMe.receiveException(handle, ex);
          }
        }, options));
        return handle;
      }
    }
    
    /**
     * Method which suggests a ping to the remote node.
     */
//    public boolean ping(Map<String, Object> options) {
//      if (environment.getTimeSource().currentTimeMillis() - updated > PING_THROTTLE) {
//        this.updated = environment.getTimeSource().currentTimeMillis();
//        
//        switch (liveness) {
//          case LIVENESS_DEAD_FOREVER:
//            return false;
//          case LIVENESS_DEAD:
//            if (logger.level <= Logger.FINE) logger.log( "(SSRM) PING: PINGING DEAD ADDRESS " + address + " - JUST IN CASE, NO HARM ANYWAY");
//            return pinger.ping(srFactory.getSourceRoute(getLocalIdentifier(), address), options);
//          default:
//            if (best != null) {
//              boolean ret = pinger.ping(best, options);
//              
//              // check to see if the direct route is available
//              if (! best.isDirect()) 
//                pinger.ping(srFactory.getSourceRoute(getLocalIdentifier(), address), options);
//              return ret;
//            }
//            return false;
//        }
//      } else {
//        return false;
//      }
//    }  
    
    /**
     * Method which suggests a ping to the remote node.
     */
    public boolean checkLiveness(Map<String, Object> options) {
      long now = environment.getTimeSource().currentTimeMillis();
//      if (now < this.updated+CHECK_LIVENESS_THROTTLE) {
//        return false;
//      }
        
      this.updated = now;
      
      switch (liveness) {
        case LIVENESS_DEAD_FOREVER:
//          return false;
        case LIVENESS_DEAD:
        {
          if (logger.level <= Logger.FINE) logger.logException( "(SSRM) CHECKLIVENESS: CHECKING DEAD ON DEAD ADDRESS " + address + " - JUST IN CASE, NO HARM ANYWAY", new Exception("Stack Trace"));
          boolean ret = false;
          if (livenessProvider.checkLiveness(srFactory.getSourceRoute(getLocalIdentifier(), address), options)) ret = true; // only checks the direct route

          Collection<SourceRoute<Identifier>> newroutes = strategy.getSourceRoutes(address);
          for (SourceRoute<Identifier> route : newroutes) {
            if (livenessProvider.checkLiveness(route, options)) ret = true;
          }
          return ret;
        } 
        default:
          SourceRoute<Identifier> temp = best;
          if (temp != null) {
            boolean ret = livenessProvider.checkLiveness(best, options);

            // check to see if the direct route is available
            if (! temp.isDirect()) 
              livenessProvider.checkLiveness(srFactory.getSourceRoute(getLocalIdentifier(), address), options);
            return ret;
          }          
          return false;
      }  
    }   
    
    public String toString() {
      return "AM "+this.address; 
    }

    public void livenessChanged(SourceRoute<Identifier> i, int val, Map<String, Object> options) {
      routes.add(i);
      if (!i.getLastHop().equals(address)) throw new IllegalArgumentException(i+"!="+address+" val:"+val);
      switch(val) {
      case LIVENESS_ALIVE:
        markAlive(i, options);
        return;
      case LIVENESS_SUSPECTED:
        markSuspected(i, options);
        return;
      case LIVENESS_DEAD:
        markDead(i, options);
        return;
      case LIVENESS_DEAD_FOREVER:
        markDeadForever(options);
        return;        
      default:
        throw new IllegalArgumentException("Unexpected val:"+val+" i:"+i+" address:"+address);
      }
    }
    
    /**
     * This method should be called when a known route is declared
     * alive.
     *
     * @param route The now-live route
     */
    protected synchronized void markAlive(SourceRoute<Identifier> route, Map<String, Object> options) {
      if (logger.level <= Logger.FINER) logger.log(this+" markAlive("+route+"):"+best);
      // first, we check and see if we have no best route (this can happen if the best just died)
      if (best == null) {
        if (logger.level <= Logger.FINE) logger.log( "(SSRM) No previous best route existed to " + address + " route " + route + " is now the best");
        best = route;        
      }
      
      // now, we check if the route is (a) shorter, or (b) the same length but quicker
      // if se, we switch our best route to that one
      if ((best.getNumHops() > route.getNumHops()) || 
          ((best.getNumHops() == route.getNumHops()) &&
           (proxProvider.proximity(best, options) > proxProvider.proximity(route, options)))) {
        if (logger.level <= Logger.FINE) logger.log( "(SSRM) Route " + route + " is better than previous best route " + best + " - replacing");
            
        best = route;  
      }
      
      // finally, mark this address as alive
      setAlive(options);
    }
    
    /**
     * This method should be called when a known route is declared
     * suspected.
     *
     * @param route The now-suspected route
     */
    protected synchronized void markSuspected(SourceRoute<Identifier> route, Map<String, Object> options) {      
      if (logger.level <= Logger.FINER) logger.log(this+" markSuspected("+route+"):"+best);
      // mark this address as suspected, if this is currently the best route
      if (((best == null) || (best.equals(route))) && // because we set the best == null when we didn't have a route
          (liveness < LIVENESS_DEAD)) // don't promote the node
          setSuspected(options);
    }
    
    /**
     * This method should be called when a known route is declared
     * dead.
     *
     * @param route The now-dead route
     */
    protected synchronized void markDead(SourceRoute<Identifier> deadRoute, Map<String, Object> options) {
//      logger.logException(this+" markDead("+deadRoute+"):"+best, new Exception());
      if (logger.level <= Logger.FINE) logger.log(this+" markDead("+deadRoute+"):"+best);
      
      // if we're already dead, who cares
      if (liveness >= LIVENESS_DEAD)
        return;
      
      // if this route was the best, or if we have no best, we need to
      // look for alternate routes - if all alternates are now dead,
      // we mark ourselves as dead
      if ((best == null) || (deadRoute.equals(best))) {
        best = null;

        Collection<SourceRoute<Identifier>> newroutes = strategy.getSourceRoutes(address);
        this.routes.addAll(newroutes);
        // if we found a route, or are probing one, this goes true, otherwise we go dead
        boolean found = false;

        SourceRoute<Identifier> newBest = null;
        
        // must wrap it in another collection to prevent a ConcurrentModificationException
        for (SourceRoute<Identifier> route : new ArrayList<SourceRoute<Identifier>>(this.routes)) {
          // assert the strategy did the right thing
          if (!route.getLastHop().equals(address)) {
            if (logger.level <= Logger.SEVERE) logger.log("SRStrategy "+strategy+" is broken.  It returned "+route+" as a route to "+address);
          } else {
            //TODO: need to keep track of when we checked these routes, so that we can go to markDead
            if (livenessProvider.checkLiveness(route, options)) {
              if (logger.level <= Logger.FINEST) logger.log(this+" Checking "+route);
              found = true;
            }
            
            // now, we check if the route is (a) shorter, or (b) the same length but quicker
            // if se, we switch our best route to that one
            if (livenessProvider.getLiveness(route, options) < LIVENESS_DEAD) {
              if (newBest == null ||
                 (newBest.getNumHops() > route.getNumHops()) || 
                 ((newBest.getNumHops() == route.getNumHops()) &&
                   (proxProvider.proximity(newBest, options) > proxProvider.proximity(route, options)))) {                    
                newBest = route;
              }
              if (logger.level <= Logger.FINEST) logger.log(this+" Found "+route);
              found = true;
            }
          }
        }
        
        if (newBest != null) {
          if (logger.level <= Logger.FINE) logger.log("Found existing known route " + newBest + " to replace old dead route " + deadRoute + " - replacing");
          best = newBest;
          // finally, mark this address as alive
          int tempLiveness = livenessProvider.getLiveness(newBest, options);
          if (tempLiveness == LIVENESS_ALIVE) {
            setAlive(options);
          } else if (tempLiveness == LIVENESS_SUSPECTED) {
            setSuspected(options);
          }
          return;
        }
        
        if (found) {
          // newBest == null
          setSuspected(options);
        } else {
          setDead(options);
        }
      } 
    }    
    
    /**
     * This method should be called when a known node is declared dead - this is
     * ONLY called when a new epoch of that node is detected.  Note that this method
     * is silent - no checks are done.  Caveat emptor.
     *
     * @param address The now-dead address
     */
    protected synchronized void markDeadForever(Map<String, Object> options) {      
      this.best = null;            
      setDeadForever(options);
    }
    
    /**
     * This method should be called when a known route has its proximity updated
     *
     * @param route The route
     * @param proximity The proximity
     */
    protected synchronized void markProximity(SourceRoute<Identifier> route, int proximity, Map<String, Object> options) {
//      getRouteManager(route).markAlive();
//      getRouteManager(route).markProximity(proximity);
      
      // first, we check and see if we have no best route (this can happen if the best just died)
      if (best == null) {
        if (logger.level <= Logger.FINE) logger.log( "(SSRM) No previous best route existed to " + address + " route " + route + " is now the best");
        best = route;        
      }
      
//      setAlive();
        
      // next, we update everyone if this is the active route
      if (route.equals(best)) {
        notifyProximityListeners(address, proximity, options);
      }
    }
    
    /**
     * Internal method which marks this address as being alive.  If we were dead before, it
     * sends an update out to the observers.
     * 
     * best must be non-null
     * 
     * @throws IllegalStateException if best is null.
     */
    protected void setAlive(Map<String, Object> options) {
      if (logger.level <= Logger.FINE) logger.log(this+"setAlive():"+best);

      if (best == null) throw new IllegalStateException("best is null in "+toString());
      
      // we can now send any pending messages
      while (!pendingMessages.isEmpty()) {
        PendingMessage pm = pendingMessages.removeFirst();
        pm.cancellable = tl.sendMessage(best, pm.message, pm, pm.options);          
      }
      
      // we can now send any pending messages
      while (!pendingSockets.isEmpty()) {
        PendingSocket pas = pendingSockets.removeFirst();
        pas.cancellable = tl.openSocket(best, pas, pas.options);
      }
      
      if (pendingMessages.isEmpty() && pendingSockets.isEmpty()) hardLinks.remove(this);      
      
      switch (liveness) {
        case LIVENESS_DEAD_FOREVER:
        case LIVENESS_DEAD:
          liveness = LIVENESS_ALIVE;
          notifyLivenessListeners(address, LIVENESS_ALIVE, options);
          if (logger.level <= Logger.FINE) logger.log( "COUNT: " + localAddress + " Found address " + address + " to be alive again.");
          break;
        case LIVENESS_UNKNOWN:
        case LIVENESS_SUSPECTED:
          liveness = LIVENESS_ALIVE;
          notifyLivenessListeners(address, LIVENESS_ALIVE, options);
          if (logger.level <= Logger.FINE) logger.log( "COUNT: " + localAddress + " Found address " + address + " to be unsuspected.");
          break;
//        case LIVENESS_DEAD_FOREVER:
//          if (logger.level <= Logger.WARNING) logger.log( "ERROR: Found dead-forever handle to " + address + " to be alive again!");
//          break;
      }
    }
    
    /**
     * Internal method which marks this address as being suspected.
     */
    protected void setSuspected(Map<String, Object> options) {
      switch (liveness) {
        case LIVENESS_UNKNOWN:
        case LIVENESS_ALIVE:
          liveness = LIVENESS_SUSPECTED;
          notifyLivenessListeners(address, LIVENESS_SUSPECTED, options);
          if (logger.level <= Logger.FINE) 
            logger.log("COUNT: " + environment.getTimeSource().currentTimeMillis() + 
                " " + localAddress + " Found address " + address + " to be suspected.");
          break;
        case LIVENESS_DEAD_FOREVER:
        case LIVENESS_DEAD:
          liveness = LIVENESS_SUSPECTED;
          notifyLivenessListeners(address, LIVENESS_SUSPECTED, options);
          if (logger.level <= Logger.WARNING) 
            logger.logException(
                "ERROR: Found node handle " + address + 
                " to be suspected from dead - should not happen!", 
                new Exception("Stack Trace"));
          break;
//        case LIVENESS_DEAD_FOREVER:
//          if (logger.level <= Logger.WARNING) 
//            logger.logException(
//                "ERROR: Found node handle " + address + 
//                " to be suspected from dead forever - should never ever happen!", 
//                new Exception("Stack Trace"));
//          break;
      }
      
      // and finally we can now reroute any route messages
//      Object[] array = pendingMessages.toArray();
//      
//      for (int i=0; i<array.length; i++) {
//        SocketBuffer sb = (SocketBuffer)array[i];
//        if (sb.isRouteMessage()) {
//          if (sb.getOptions().multipleHopsAllowed() && sb.getOptions().rerouteIfSuspected()) {
//            //if (logger.level <= Logger.FINE) logger.log( "REROUTE: Rerouting message " + sb + " due to suspected next hop " + address);
//            reroute(address, sb);
//            queue.remove(sb);
//          }
//        }
//      }
//      if (queue.isEmpty() && pendingSockets.isEmpty()) hardLinks.remove(this);
    }
    
    /**
     * Internal method which marks this address as being dead.  If we were alive or suspected before, it
     * sends an update out to the observers.
     */
    protected void setDead(Map<String, Object> options) {
//      logger.log(this+" marking as dead.");
      switch (liveness) {
        case LIVENESS_DEAD_FOREVER:
        case LIVENESS_DEAD:
          return;
//        case LIVENESS_DEAD_FOREVER:
//          if (logger.level <= Logger.WARNING) 
//            logger.log(
//                "ERROR: Found node handle " + address + 
//                " to be dead from dead forever - should not happen!");
//          break;
        default:
          this.best = null;
          this.liveness = LIVENESS_DEAD;
          notifyLivenessListeners(address, LIVENESS_DEAD, options);
//          if (address != null) address.update(DECLARED_DEAD);   
//          if (address != null) manager.declaredDead(address);
          if (logger.level <= Logger.FINE) 
            logger.log( "COUNT: " + localAddress + 
                " Found address " + address + " to be dead.");
          break;
      }

      purgeQueue();
//      clearLivenessState();
    }

    /**
     * Internal method which marks this address as being dead.  If we were alive or suspected before, it
     * sends an update out to the observers.
     */
    protected void setDeadForever(Map<String, Object> options) {
      switch (liveness) {
        case LIVENESS_DEAD_FOREVER:
          return;
        case LIVENESS_DEAD:
          this.liveness = LIVENESS_DEAD_FOREVER;
          if (logger.level <= Logger.FINE) logger.log("Found address " + address + " to be dead forever.");
          break;
        default:
          this.best = null;
          this.liveness = LIVENESS_DEAD_FOREVER;
          notifyLivenessListeners(address, LIVENESS_DEAD_FOREVER, options);
          if (logger.level <= Logger.FINE) logger.log("Found address " + address + " to be dead forever.");
          break;
      }
      purgeQueue();
      clearLivenessState();
    }
    
    protected void purgeQueue() {
      // and finally we can now send any pending messages
      while (!pendingMessages.isEmpty()) {
        PendingMessage pm = pendingMessages.removeFirst();
        if (pm.deliverAckToMe != null) pm.deliverAckToMe.sendFailed(pm, new NodeIsFaultyException(address));
//        reroute(address, (SocketBuffer) queue.removeFirst());
      }
      while (!pendingSockets.isEmpty()) {
        PendingSocket ps = pendingSockets.removeFirst();
        ps.deliverSocketToMe.receiveException(ps, new NodeIsFaultyException(address));
//        pas.receiver.receiveException(null, new NodeIsDeadException());
      }
      removeHardLink(this);      
    }
  }

  public void addLivenessListener(LivenessListener<Identifier> name) {
    synchronized(livenessListeners) {
      livenessListeners.add(name);
    }
  }

  public boolean removeLivenessListener(LivenessListener<Identifier> name) {
    synchronized(livenessListeners) {
      return livenessListeners.remove(name);
    }
  }
  
  private void notifyLivenessListeners(Identifier i, int liveness, Map<String, Object> options) {
    if (logger.level <= Logger.FINER) logger.log("notifyLivenessListeners("+i+","+liveness+")");
    List<LivenessListener<Identifier>> temp;
    synchronized(livenessListeners) {
      temp = new ArrayList<LivenessListener<Identifier>>(livenessListeners);
    }
    for (LivenessListener<Identifier> listener : temp) {
      listener.livenessChanged(i, liveness, options);
    }
  }
  
//  public void addPingListener(PingListener<Identifier> name) {
//    synchronized(pingListeners) {
//      pingListeners.add(name);
//    }
//  }
//
//  public boolean removePingListener(PingListener<Identifier> name) {
//    synchronized(pingListeners) {
//      return pingListeners.remove(name);
//    }
//  }
//  
//  private void notifyPingListeners(Identifier i, int rtt) {
//    List<PingListener<Identifier>> temp;
//    synchronized(pingListeners) {
//      temp = new ArrayList<PingListener<Identifier>>(pingListeners);
//    }
//    for (PingListener<Identifier> listener : temp) {
//      listener.pingResponse(i, rtt, null);
//    }
//  }

  public void incomingSocket(P2PSocket<SourceRoute<Identifier>> s) throws IOException {
    callback.incomingSocket(new SourceRouteManagerP2PSocket<Identifier>(s, logger, errorHandler, environment));
  }

  public void messageReceived(SourceRoute<Identifier> i, ByteBuffer m, Map<String, Object> options) throws IOException {
    callback.messageReceived(i.getLastHop(), m, options);
  }

  public void livenessChanged(SourceRoute<Identifier> i, int val, Map<String, Object> options) {
    if (logger.level <= Logger.FINER) logger.log("livenessChanged("+i+","+val+")");
    getAddressManager(i.getLastHop()).livenessChanged(i,val, options);
  }

//  public void pingResponse(SourceRoute<Identifier> i, int rtt, Map<String, Object> options) {
//    getAddressManager(i.getLastHop()).markProximity(i, rtt, options);
//  }
//  
//  public void pingReceived(SourceRoute<Identifier> i, Map<String, Object> options) {
//    getAddressManager(i.getLastHop()).markProximity(i, rtt, options);
//  }

  Collection<ProximityListener<Identifier>> listeners = new ArrayList<ProximityListener<Identifier>>();
  public void addProximityListener(ProximityListener<Identifier> listener) {
    synchronized(listeners) {
      listeners.add(listener);
    }
  }

  public boolean removeProximityListener(ProximityListener<Identifier> listener) {
    synchronized(listeners) {
      return listeners.remove(listener);
    }
  }

  public void proximityChanged(SourceRoute<Identifier> i, int newProximity, Map<String, Object> options) {
    getAddressManager(i.getLastHop()).markProximity(i, newProximity, options);
  }
  
  public void notifyProximityListeners(Identifier i, int prox, Map<String, Object> options) {
    Collection<ProximityListener<Identifier>> temp;
    synchronized(listeners) {
      temp = new ArrayList<ProximityListener<Identifier>>(listeners);
    }
    for (ProximityListener<Identifier> p : temp) {
      p.proximityChanged(i, prox, options);
    }
  }
}
