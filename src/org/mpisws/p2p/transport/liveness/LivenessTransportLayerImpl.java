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
package org.mpisws.p2p.transport.liveness;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.util.TimerWeakHashMap;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.selector.Timer;
import rice.selector.TimerTask;

public class LivenessTransportLayerImpl<Identifier> implements 
    LivenessTypes,
    LivenessTransportLayer<Identifier, ByteBuffer>, 
    TransportLayerCallback<Identifier, ByteBuffer>,
    OverrideLiveness<Identifier> {
  // how long to wait for a ping response to come back before declaring lost
  public final int PING_DELAY;
  
  // factor of jitter to adjust to the ping waits - we may wait up to this time before giving up
  public final float PING_JITTER;
  
  // how many tries to ping before giving up
  public final int NUM_PING_TRIES;
  
  // the maximal amount of time to wait for write to be called before checking liveness
//  public final int WRITE_WAIT_TIME;
  
  // the initial timeout for exponential backoff
  public final long BACKOFF_INITIAL;
  
  // the limit on the number of times for exponential backoff
  public final int BACKOFF_LIMIT;
  
  // the minimum amount of time between check dead checks on dead routes
  public long CHECK_DEAD_THROTTLE;
  
  // the minimum amount of time between liveness checks
//  public long LIVENESS_CHECK_THROTTLE;
  
//  public int NUM_SOURCE_ROUTE_ATTEMPTS;
  
  // the default distance, which is used before a ping
//  public static final int DEFAULT_PROXIMITY = 60*60*1000;
  
  /**
   * millis for the timeout
   * 
   * The idea is that we don't want this parameter to change too fast, 
   * so this is the timeout for it to increase, you could set this to infinity, 
   * but that may be bad because it doesn't account for intermediate link failures
   */
//  public int PROX_TIMEOUT;// = 60*60*1000;

  public int DEFAULT_RTO;// = 3000;
  
  /**
   * RTO helper see RFC 1122 for a detailed description of RTO calculation
   */
  int RTO_UBOUND;// = 10000; // 10 seconds
  /**
   * RTO helper see RFC 1122 for a detailed description of RTO calculation
   */
  int RTO_LBOUND;// = 50;

  /**
   * RTO helper see RFC 1122 for a detailed description of RTO calculation
   */
  double gainH;// = 0.25;

  /**
   * RTO helper see RFC 1122 for a detailed description of RTO calculation
   */
  double gainG;// = 0.125;


  protected TransportLayer<Identifier, ByteBuffer> tl;
  protected Logger logger;
  protected Environment environment;
  protected TimeSource time;
  protected Timer timer;
  protected Random random;

  /**
   * Pass the msg to the callback if it is NORMAL
   */
  public static final byte HDR_NORMAL = 0;
  public static final byte HDR_PING = 1;
  public static final byte HDR_PONG = 2;
  
  /**
   * Holds only pending DeadCheckers
   */
  Map<Identifier, EntityManager> managers;

  private TransportLayerCallback<Identifier, ByteBuffer> callback;
  
  private ErrorHandler<Identifier> errorHandler;
  
//  public LivenessTransportLayerImpl(TransportLayer<Identifier, ByteBuffer> tl, Environment env, ErrorHandler<Identifier> errorHandler, int checkDeadThrottle) {
//    this(tl,env,errorHandler,checkDeadThrottle,false);
//  }
  
  public LivenessTransportLayerImpl(TransportLayer<Identifier, ByteBuffer> tl, Environment env, ErrorHandler<Identifier> errorHandler, int checkDeadThrottle) {
    this.tl = tl;
    this.environment = env;
    this.logger = env.getLogManager().getLogger(LivenessTransportLayerImpl.class, null);
//    logger.logException("LivenessTransportLayerImpl@"+System.identityHashCode(this),new Exception("Stack Trace"));
    this.time = env.getTimeSource();
    this.timer = env.getSelectorManager().getTimer();
    random = new Random();
    this.livenessListeners = new ArrayList<LivenessListener<Identifier>>();
    this.pingListeners = new ArrayList<PingListener<Identifier>>();
//    this.managers = new HashMap<Identifier, EntityManager>();
//    if (cleanMemory) {
      this.managers = new TimerWeakHashMap<Identifier, EntityManager>(env.getSelectorManager(), 300000);
//    } else {
//      this.managers = new HashMap<Identifier, EntityManager>();      
//    }
    Parameters p = env.getParameters();
    PING_DELAY = p.getInt("pastry_socket_scm_ping_delay");
    PING_JITTER = p.getFloat("pastry_socket_scm_ping_jitter");
    NUM_PING_TRIES = p.getInt("pastry_socket_scm_num_ping_tries");
    BACKOFF_INITIAL = p.getInt("pastry_socket_scm_backoff_initial");
    BACKOFF_LIMIT = p.getInt("pastry_socket_scm_backoff_limit");
    CHECK_DEAD_THROTTLE = checkDeadThrottle; //p.getLong("pastry_socket_srm_check_dead_throttle"); // 300000
    DEFAULT_RTO = p.getInt("pastry_socket_srm_default_rto"); // 3000 // 3 seconds
    RTO_UBOUND = p.getInt("pastry_socket_srm_rto_ubound");//240000; // 240 seconds
    RTO_LBOUND = p.getInt("pastry_socket_srm_rto_lbound");//1000;
    gainH = p.getDouble("pastry_socket_srm_gain_h");//0.25;
    gainG = p.getDouble("pastry_socket_srm_gain_g");//0.125;

    tl.setCallback(this);
    this.errorHandler = errorHandler;
    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    }
  }
  
  public void clearState(Identifier i) {
    if (logger.level <= Logger.FINE) logger.log("clearState("+i+")");
    deleteManager(i);
  }

  public boolean checkLiveness(Identifier i, Map<String, Object> options) {
    return getManager(i).checkLiveness(options);
  }
  
  public P2PSocket<Identifier> getLSocket(P2PSocket<Identifier> s, EntityManager manager) {
    LSocket sock = new LSocket(manager, s, manager.identifier.get());
    synchronized(manager.sockets) {
      manager.sockets.add(sock);
    }
    return sock;
  }
  
  public EntityManager getManager(Identifier i) {
    synchronized(managers) {
      EntityManager manager = managers.get(i);
      if (manager == null) {
        manager = new EntityManager(i);
        managers.put(i,manager);
      }
      return manager;
    }
  }
  
  public EntityManager deleteManager(Identifier i) {
    synchronized(managers) {
      EntityManager manager = managers.remove(i);
      if (manager != null) {
        if (manager.getPending() != null) manager.getPending().cancel();
      }
      return manager;
    }
  }
  
  public int getLiveness(Identifier i, Map<String, Object> options) {
    if (logger.level <= Logger.FINEST) logger.log("getLiveness("+i+","+options+")");
    synchronized(managers) {
      if (managers.containsKey(i))
        return managers.get(i).liveness;
    }
    return LIVENESS_SUSPECTED;
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

  /**
   * TODO: make this a number of failures?  Say 3?  Use 1 if using SourceRoutes
   */
  boolean connectionExceptionMeansFaulty = true;
  /**
   * Set this to true if you want a ConnectionException to mark the 
   * connection as faulty.  Default = true;
   *
   */
  public void connectionExceptionMeansFaulty(boolean b) {
    connectionExceptionMeansFaulty = b;
  }
  
  public SocketRequestHandle<Identifier> openSocket(final Identifier i, final SocketCallback<Identifier> deliverSocketToMe, final Map<String, Object> options) {
    // this code marks the Identifier faulty if there is an error connecting the socket.  It's possible that this
    // should be moved to the source route manager, but there needs to be a way to cancel the liveness
    // checks, or maybe the higher layer can ignore them.
    if (logger.level <= Logger.INFO-50) logger.log("openSocket("+i+","+deliverSocketToMe+","+options+")");
    return tl.openSocket(i, new SocketCallback<Identifier>(){
      public void receiveResult(SocketRequestHandle<Identifier> cancellable, P2PSocket<Identifier> sock) {
        deliverSocketToMe.receiveResult(cancellable, getLSocket(sock,getManager(i)));
      }    
      
      public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
        // the upper layer is probably going to retry, so mark this dead first
        if (connectionExceptionMeansFaulty) {
          if (ex instanceof java.nio.channels.ClosedChannelException) {
            // don't mark dead
          } else {
            if (logger.level <= Logger.FINER) logger.logException("Marking "+s+" dead due to exception opening socket.", ex);
            getManager(i).markDead(options);
          }
        }
        deliverSocketToMe.receiveException(s, ex);
      }
    }, options);
  }

  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(
      final Identifier i, 
      final ByteBuffer m, 
      final MessageCallback<Identifier, ByteBuffer> deliverAckToMe, 
      Map<String, Object> options) {
//    logger.log("sendMessage("+i+","+m+")");      
    final MessageRequestHandleImpl<Identifier, ByteBuffer> handle = 
      new MessageRequestHandleImpl<Identifier, ByteBuffer>(i, m, options);
        
    EntityManager mgr = getManager(i);
    if ((mgr != null) && (mgr.liveness >= LIVENESS_DEAD)) {
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(handle, new NodeIsFaultyException(i, m));
      return handle;
    }
//    logger.log("sendMessage2("+i+","+m+")");      
    
    byte[] msgBytes = new byte[m.remaining()+1];
    msgBytes[0] = HDR_NORMAL;
    m.get(msgBytes, 1, m.remaining());
    final ByteBuffer myMsg = ByteBuffer.wrap(msgBytes);

    handle.setSubCancellable(tl.sendMessage(i, myMsg, new MessageCallback<Identifier, ByteBuffer>() {    
      public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
        if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != handle.getSubCancelable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
        if (deliverAckToMe != null) deliverAckToMe.ack(handle);
      }    
      public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception ex) {
        if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != handle.getSubCancelable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
        if (deliverAckToMe == null) {
          errorHandler.receivedException(i, ex);
        } else {
          deliverAckToMe.sendFailed(handle, ex);
        }
      }
    }, options));
    return handle;
  }

  public void messageReceived(Identifier i, ByteBuffer m, Map<String, Object> options) throws IOException {
//    logger.log("messageReceived1("+i+","+m+"):"+m.remaining());      
    byte hdr = m.get();
    switch(hdr) {
    case HDR_NORMAL:
      if (logger.level <= Logger.FINEST) logger.log("messageReceived("+i+","+m+")");
//      logger.log("messageReceived2("+i+","+m+"):"+m.remaining());      
      callback.messageReceived(i, m, options);                
      return;
    case HDR_PING:
      if (logger.level <= Logger.FINEST) logger.log("messageReceived("+i+", PING)");
//      logger.log("Got ping from "+i);
      pong(i, m.getLong(), options);      // it's important to the rendezvous layer to reuse the options
      notifyPingListenersPing(i);
      return;
    case HDR_PONG:
      if (logger.level <= Logger.FINEST) logger.log("messageReceived("+i+", PONG)");
      EntityManager manager = getManager(i);
      long sendTime = m.getLong();
      int rtt = (int)(time.currentTimeMillis()-sendTime);      
      if (rtt >= 0) {
//        logger.log(this+"PONG1");
        manager.updateRTO(rtt);
        boolean markAlive = false;
        synchronized(manager) {
          if (manager.getPending() != null) {
//            logger.log(this+"PONG2");
            manager.getPending().pingResponse(sendTime, options); 
            markAlive = true;
//          } else {
//            logger.log("Got pong from "+i+", but there is no DeadChecker for "+manager+"@"+System.identityHashCode(manager));            
          }
        }
        manager.markAlive(options); // do this outside of the synchronized block
        notifyPingListenersPong(i,rtt, options);
      } else {
        if (logger.level <= Logger.WARNING) logger.log("I think the clock is fishy, rtt must be >= 0, was:"+rtt);
        errorHandler.receivedUnexpectedData(i, m.array(), 0, null);      
      }
      return;            
    default:
      errorHandler.receivedUnexpectedData(i, m.array(), 0, null);      
    }
  }

  /**
   * True if there was a pending liveness check.
   * 
   * @param i
   * @param options
   * @return
   */
  public boolean cancelLivenessCheck(Identifier i, Map<String, Object> options) {
    EntityManager manager = getManager(i);
    if (manager == null) {
      return false;
    } 
    return cancelLivenessCheck(manager, options);
  }
  
  public boolean cancelLivenessCheck(EntityManager manager, Map<String, Object> options) {
    synchronized(manager) {
      if (manager.getPending() != null) {
//          logger.log(this+"PONG2");
        manager.getPending().cancel();
        return true;
      }
    }
    manager.markAlive(options);
    return false;
  }
  
  public String toString() {
    return "LivenessTL{"+getLocalIdentifier()+"}";
  }
  
  /**
   * Send the ping.
   * 
   * @param i
   */
  public boolean ping(final Identifier i, final Map<String, Object> options) {
    if (logger.level <= Logger.FINER) logger.log("ping("+i+")");
    if (i.equals(tl.getLocalIdentifier())) return false;
    try {
      SimpleOutputBuffer sob = new SimpleOutputBuffer(1024);
      sob.writeByte(HDR_PING);
      final long now = time.currentTimeMillis();
      new PingMessage(now).serialize(sob);
      tl.sendMessage(i, ByteBuffer.wrap(sob.getBytes()), new MessageCallback<Identifier, ByteBuffer>(){
        public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
        }
        public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception reason) {
          if (logger.level <= Logger.FINER) {
            logger.logException("ping("+i+","+now+","+options+") failed", reason);
          } else {
            if (logger.level <= Logger.FINE) logger.log("ping("+i+","+now+","+options+") failed "+reason);
          }
        }       
      }, options);
      return true;
    } catch (IOException ioe) {
      //Should not happen.  There must be a bug in our serialization code.
      errorHandler.receivedException(i, ioe);
    }
    return false;
  }

  /**
   * Send the pong();
   * 
   * @param i
   * @param senderTime
   */
  public void pong(final Identifier i, final long senderTime, final Map<String, Object> options) {
    if (logger.level <= Logger.FINEST) logger.log("pong("+i+","+senderTime+")");
    try {
      SimpleOutputBuffer sob = new SimpleOutputBuffer(1024);
      sob.writeByte(HDR_PONG);
      new PongMessage(senderTime).serialize(sob);
      tl.sendMessage(i, ByteBuffer.wrap(sob.getBytes()), new MessageCallback<Identifier, ByteBuffer>(){
        public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
        }
        public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception reason) {
          if (logger.level <= Logger.FINER) {
            logger.logException("pong("+i+","+senderTime+","+options+") failed", reason);
          } else {
            if (logger.level <= Logger.FINE) logger.log("pong("+i+","+senderTime+","+options+") failed "+reason);
          }
        }       
      }, options);
    } catch (IOException ioe) {
      //Should not happen.  There must be a bug in our serialization code.
      errorHandler.receivedException(i, ioe);
    }
  }

  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    errorHandler = handler;
    this.errorHandler = errorHandler;
    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    }
  }

  boolean destroyed = false;
  public void destroy() {
    if (logger.level <= Logger.INFO) logger.log("destroy()");
    destroyed = true;
    tl.destroy();    
    livenessListeners.clear();
    livenessListeners = null;
    pingListeners.clear();
    pingListeners = null;
    for (EntityManager em : managers.values()) {
      em.destroy();
    }
    managers.clear();    
//    managers = null;
  }

  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {    
    EntityManager m = getManager(s.getIdentifier());
    if (m.liveness > LIVENESS_SUSPECTED) {
      m.updated = 0L;
      m.checkLiveness(s.getOptions());
    }
    callback.incomingSocket(getLSocket(s, getManager(s.getIdentifier())));    
  }

  List<LivenessListener<Identifier>> livenessListeners;
  public void addLivenessListener(LivenessListener<Identifier> name) {
    if (destroyed) return;
    synchronized(livenessListeners) {
      livenessListeners.add(name);
    }
  }

  public boolean removeLivenessListener(LivenessListener<Identifier> name) {
    if (destroyed) return true;
    synchronized(livenessListeners) {
      return livenessListeners.remove(name);
    }
  }
  
  private void notifyLivenessListeners(Identifier i, int liveness, Map<String, Object> options) {
    if (destroyed) return;
    if (logger.level <= Logger.FINER) logger.log("notifyLivenessListeners("+i+","+liveness+")");
    List<LivenessListener<Identifier>> temp;

    synchronized(livenessListeners) {
      temp = new ArrayList<LivenessListener<Identifier>>(livenessListeners);
    }
    for (LivenessListener<Identifier> listener : temp) {
      listener.livenessChanged(i, liveness, options);
    }
  }
  
  List<PingListener<Identifier>> pingListeners;
  public void addPingListener(PingListener<Identifier> name) {
    synchronized(pingListeners) {
      pingListeners.add(name);
    }
  }

  public boolean removePingListener(PingListener<Identifier> name) {
    synchronized(pingListeners) {
      return pingListeners.remove(name);
    }
  }
  
  private void notifyPingListenersPing(Identifier i) {
    List<PingListener<Identifier>> temp;
    synchronized(pingListeners) {
      temp = new ArrayList<PingListener<Identifier>>(pingListeners);
    }
    for (PingListener<Identifier> listener : temp) {
      listener.pingReceived(i, null);
    }
  }
  
  private void notifyPingListenersPong(Identifier i, int rtt, Map<String, Object> options) {
    List<PingListener<Identifier>> temp;
    synchronized(pingListeners) {
      temp = new ArrayList<PingListener<Identifier>>(pingListeners);
    }
    for (PingListener<Identifier> listener : temp) {
      listener.pingResponse(i, rtt, options);
    }
  }
  
  /**
   * DESCRIBE THE CLASS
   *
   * @version $Id: SocketCollectionManager.java 3613 2007-02-15 14:45:14Z jstewart $
   * @author jeffh
   */
  protected class DeadChecker extends TimerTask {

    // The number of tries that have occurred so far
    protected int tries = 1;
    
    // the total number of tries before declaring death
    protected int numTries;
    
    // the path to check
    protected EntityManager manager;
    
    // for debugging
    long startTime; // the start time
    int initialDelay; // the initial expected delay
    
    Map<String, Object> options;
    
    /**
     * Constructor for DeadChecker.
     *
     * @param address DESCRIBE THE PARAMETER
     * @param numTries DESCRIBE THE PARAMETER
     * @param mgr DESCRIBE THE PARAMETER
     */
    public DeadChecker(EntityManager manager, int numTries, int initialDelay, Map<String, Object> options) {
      if (logger.level <= Logger.FINE) {
//        String s = 
//        if (options.containsKey("identity.node_handle_to_index")) {
//          
//        }
        logger.log("DeadChecker@"+System.identityHashCode(this)+" CHECKING DEATH OF PATH " + manager.identifier.get()+" rto:"+initialDelay+" options:"+options);
      }
      
      this.manager = manager;
      this.numTries = numTries;
      this.options = options;
      
      this.initialDelay = initialDelay;
      this.startTime = time.currentTimeMillis();
    }

    /**
     * DESCRIBE THE METHOD
     *
     * @param address DESCRIBE THE PARAMETER
     * @param RTT DESCRIBE THE PARAMETER
     * @param timeHeardFrom DESCRIBE THE PARAMETER
     */
    public void pingResponse(long RTT, Map<String, Object> options) {
//      logger.log(this+".pingResponse()");
      if (!cancelled) {
        if (tries > 1) {
          long delay = time.currentTimeMillis()-startTime;
          if (logger.level <= Logger.FINE) logger.log(
              "DeadChecker.pingResponse("+manager.identifier.get()+") tries="+tries+
              " estimated="+initialDelay+" totalDelay="+delay);        
        }
      }      
      if (logger.level <= Logger.FINE) logger.log("Terminated DeadChecker@"+System.identityHashCode(this)+"(" + manager.identifier.get() + ") due to ping.");
      cancel();
    }

    /**
     * Main processing method for the DeadChecker object
     * 
     * value of tries before run() is called:the time since ping was called:the time since deadchecker was started 
     * 
     * 1:500:500
     * 2:1000:1500
     * 3:2000:3500
     * 4:4000:7500
     * 5:8000:15500 // ~15 seconds to find 1 path faulty, using source routes gives us 30 seconds to find a node faulty
     * 
     */
    public void run() {
      if (destroyed) return;
//      logger.log(this+".run()");
      if (tries < numTries) {
        tries++;
//        if (manager.getLiveness(path.getLastHop()) == SocketNodeHandle.LIVENESS_ALIVE)
        manager.markSuspected(options);        

        Identifier temp = manager.identifier.get();
        if (temp != null) { // could happen during a garbage collection
          if (logger.level <= Logger.FINER) logger.log("DeadChecker@"+System.identityHashCode(this)+"(" +temp+") pinging "+tries+" "+manager.getPending());
          ping(temp, options);
        }
        int absPD = (int)(PING_DELAY*Math.pow(2,tries-1));
        int jitterAmt = (int)(((float)absPD)*PING_JITTER);
        int scheduledTime = absPD-jitterAmt+random.nextInt(jitterAmt*2);
//        logger.log(this+".run():scheduling for "+scheduledTime);
        timer.schedule(this,scheduledTime);
      } else {
        if (logger.level <= Logger.FINE) logger.log("DeadChecker@"+System.identityHashCode(this)+"(" + manager.identifier.get() + ") expired - marking as dead.");
//        cancel(); // done in markDead()
        manager.markDead(options);
      }
    }

    public boolean cancel() {
      synchronized(manager) {
        manager.setPending(null);
      }
      return super.cancel();
    }
    
    public String toString() {
      return "DeadChecker("+manager.identifier.get()+" #"+System.identityHashCode(this)+"):"+tries+"/"+numTries; 
    }    
  }
  
  /**
   * Internal class which is charges with managing the remote connection via
   * a specific route
   * 
   */
  public class EntityManager {
    
    /**
     * Retransmission Time Out
     */
    int RTO = DEFAULT_RTO; 

    /**
     * Average RTT
     */
    double RTT = 0;

    /**
     * Standard deviation RTT
     */
    double standardD = RTO/4.0;  // RFC1122 recommends choose value to target RTO = 3 seconds
    
    
    // the remote route of this manager
    protected WeakReference<Identifier> identifier;
    
    // the current liveness of this route
    protected int liveness;
    
    // the current best-known proximity of this route
//    protected int proximity;
//    protected long proximityTimeout; // when the proximity is no longer valid
    
    // the last time the liveness information was updated
    protected long updated;
    
    // whether or not a check dead is currently being carried out on this route
    private DeadChecker pendingDeadchecker;
    
    protected Set<LSocket> sockets;
    
    /**
     * Constructor - builds a route manager given the route
     *
     * @param route The route
     */
    public EntityManager(Identifier identifier) {
//      logger.log("new EntityManager("+identifier+")@"+System.identityHashCode(this)+" @"+System.identityHashCode(LivenessTransportLayerImpl.this));        
      if (identifier == null) throw new IllegalArgumentException("identifier is null");
      this.identifier = new WeakReference<Identifier>(identifier);
      this.liveness = LIVENESS_SUSPECTED;

//      proximity = DEFAULT_PROXIMITY;
//      proximityTimeout = time.currentTimeMillis()+PROX_TIMEOUT;
      
      this.pendingDeadchecker = null;
      this.updated = 0L;
      sockets = new HashSet<LSocket>();
    }
    
    public DeadChecker getPending() {
      return pendingDeadchecker;
    }
    public void setPending(DeadChecker d) {
//      logger.log(this+"@"+System.identityHashCode(this)+".setPending("+d+")");
      pendingDeadchecker = d;
    }

//    public String toString() {
//      return "LivenessTLi.EM{"+identifier.get()+"}:"+liveness;
//    }
    
    public void removeSocket(LSocket socket) {
      synchronized(sockets) {
        sockets.remove(socket);
      }
    }

    public int rto() {
      return (int)RTO; 
    }
    
    /**
     * Method which returns the last cached proximity value for the given address.
     * If there is no cached value, then DEFAULT_PROXIMITY is returned.
     *
     * @param address The address to return the value for
     * @return The ping value to the remote address
     */
//    public int proximity() {
//      long now = time.currentTimeMillis();
//      // prevent from changing too much
//      if (proximityTimeout > now) return proximity;
//
//      proximity = (int)RTT;
//      proximityTimeout = now+PROX_TIMEOUT;
//      
//      // TODO, schedule notification
//      
//      return proximity;
//    }
     
    /**
     * This method should be called when this route is declared
     * alive.
     */
    protected void markAlive(Map<String, Object> options) {
      boolean notify = false;
      if (liveness != LIVENESS_ALIVE) notify = true;
      this.liveness = LIVENESS_ALIVE;
      if (notify) {
        Identifier temp = identifier.get();
        if (temp != null) {
          notifyLivenessListeners(temp, liveness, options);
        }
      }
    }
    
    /**
     * This method should be called when this route is declared
     * suspected.
     */
    protected void markSuspected(Map<String, Object> options) {      
      // note, can only go from alive -> suspected, can't go from dead->suspected
      if (liveness > LIVENESS_SUSPECTED) return;
      
      boolean notify = false;
      if (liveness != LIVENESS_SUSPECTED) notify = true;
      this.liveness = LIVENESS_SUSPECTED;
      if (notify) {
        if (logger.level <= Logger.FINE) logger.log(this+".markSuspected() notify = true");
        Identifier temp = identifier.get();
        if (temp != null) {
          notifyLivenessListeners(temp, liveness, options);
        }
      }
    }    
    
    /**
     * This method should be called when this route is declared
     * dead.
     */
    protected void markDead(Map<String, Object> options) {
      boolean notify = false;
      if (liveness < LIVENESS_DEAD) notify = true;
//      if (notify) {
//        logger.log(this+".markDead()");
//      }
      if (logger.level <= Logger.FINER) logger.log(this+".markDead() notify:"+notify);
      markDeadHelper(LIVENESS_DEAD, options, notify);
    }
    
    protected void markDeadForever(Map<String, Object> options) {
      boolean notify = false;
      if (liveness < LIVENESS_DEAD_FOREVER) notify = true;
      if (logger.level <= Logger.FINER) logger.log(this+".markDeadForever() notify:"+notify);
      markDeadHelper(LIVENESS_DEAD_FOREVER, options, notify);
    }
    
    protected void markDeadHelper(int liveness, Map<String, Object> options, boolean notify) {
      this.liveness = liveness;
      if (getPending() != null) {
        getPending().cancel(); // sets to null too
//      } else {
//        logger.log(this+".markDeadHelper() getPending was null");
      }
      
      // it's very important to notify before closing the sockets,
      // otherwise the higher layers may fight you here by trying to open 
      // sockets that this layer is closing
      if (notify) {
        Identifier temp = identifier.get();
        if (temp != null) {
          notifyLivenessListeners(temp, liveness, options);
        } else {
          if (logger.level <= Logger.WARNING) logger.log("markDeadHelper("+liveness+","+options+","+notify+") temp == null!  Can't notify listeners!");
        } 
      }
      
      ArrayList<LSocket> temp;
      synchronized(sockets) {
        // the close() operation can cause a ConcurrentModificationException
        temp = new ArrayList<LSocket>(sockets);
        sockets.clear();
      }
      for (LSocket sock : temp) {
        if (logger.level <= Logger.INFO) logger.log("closing "+sock);
        sock.close(); 
//        sock.notifyRecievers();          
      }
    }
    
    
    /**
     * This method should be called when this route has its proximity updated
     *
     * @param proximity The proximity
     */
//    protected void markProximity(int proximity) {
//      if (proximity < 0) throw new IllegalArgumentException("proximity must be >= 0, was:"+proximity);
//      updateRTO(proximity);
//      if (this.proximity > proximity) {
//        proximityTimeout = time.currentTimeMillis();
//        this.proximity = proximity;
//      }
//      // TODO: Schedule notification
//    }

    /**
     * Adds a new round trip time datapoint to our RTT estimate, and 
     * updates RTO and standardD accordingly.
     * 
     * @param m new RTT
     */
    private void updateRTO(long m) {      
      if (m < 0) throw new IllegalArgumentException("rtt must be >= 0, was:"+m);
      
      // rfc 1122
      double err = m-RTT;
      double absErr = err;
      if (absErr < 0) {
        absErr *= -1;
      }
      RTT = RTT+gainG*err;
      standardD = standardD + gainH*(absErr-standardD);
      RTO = (int)(RTT+(4.0*standardD));
      if (RTO > RTO_UBOUND) {
        RTO = RTO_UBOUND;
      }
      if (RTO < RTO_LBOUND) {
        RTO = RTO_LBOUND;
      }
//        System.out.println("CM.updateRTO() RTO = "+RTO+" standardD = "+standardD+" suspected in "+getTimeToSuspected(RTO)+" faulty in "+getTimeToFaulty(RTO));
    }      
    
    /**
     * Method which checks to see this route is dead.  If this address has
     * been checked within the past CHECK_DEAD_THROTTLE millis, then
     * this method does not actually do a check.
     *
     * @return true if there will be an update (either a ping, or a change in liveness)
     */
//    long start = 0; // delme
//    int ctr = 0; // delme
    protected boolean checkLiveness(final Map<String, Object> options) {
//      logger.log("checkLiveness() selector: "+environment.getSelectorManager().isSelectorThread());
//      if (options == null) throw new RuntimeException("options is null"); // remove, this is for debugging
//      logger.log(this+".checkLiveness()");
      if (logger.level <= Logger.FINER) logger.log(this+".checkLiveness()");

      // *************** delme ******************
      // we're gonna exit if checkLilveness was called 100 times in 1 second
//      ctr++;
//      if (ctr%100 == 0) {
//        ctr = 0;
//        long time_now = time.currentTimeMillis();        
//        if ((time_now - start) < 1000) {
//          logger.logException("great scotts! "+start+" "+this+" "+this.liveness, new Exception("Stack Trace"));
//          System.exit(1);
//        }
//        start = time_now;
//      }      
      // *************** end delme **********
      
      boolean ret = false;
      int rto = DEFAULT_RTO;
      synchronized (this) {
        if (this.getPending() != null) {
          if (this.liveness < LIVENESS_DEAD) { 
            return true;
          } else {
            return false; // prolly won't change
          }
        } 
        
        long now = time.currentTimeMillis();
        if ((this.liveness < LIVENESS_DEAD) || 
            (this.updated < now - CHECK_DEAD_THROTTLE)) {
          this.updated = now;
          rto = rto();
          this.setPending(new DeadChecker(this, NUM_PING_TRIES, rto, options));
          ret = true;
        } else {
          if (logger.level <= Logger.FINER) {
            logger.log(this+".checkLiveness() not checking "+identifier.get()+" checked to recently, can't check for "+((updated+CHECK_DEAD_THROTTLE)-now)+" millis.");
          }
        }
      }
      if (ret) {
        final int theRTO = rto;
        // delme
//        if (!options.containsKey(org.mpisws.p2p.transport.commonapi.CommonAPITransportLayerImpl.DESTINATION_IDENTITY)) 
//          throw new RuntimeException("options doesn't contain "+org.mpisws.p2p.transport.commonapi.CommonAPITransportLayerImpl.DESTINATION_IDENTITY+" "+options);
        // /delme
        
        Runnable r = new Runnable() {
          public void run() {
            if (getPending() == null) return;  // could have been set to null in the meantime
            timer.schedule(getPending(), theRTO);
            Identifier temp = identifier.get();
            if (temp != null) {
              ping(temp, options);
            }
          }
          
          public String toString() {
            return EntityManager.this.toString();
          }
        };
        if (environment.getSelectorManager().isSelectorThread()) {
          r.run();
        } else {
          environment.getSelectorManager().invoke(r);
        }
      }
      
      if (this.liveness >= LIVENESS_DEAD) return false; // prolly won't change
      
      return ret;
    }
    
    public String toString() {
      Identifier temp = identifier.get();
      if (temp == null) return "null";
      return temp.toString();
//      return "SRM{"+System.identityHashCode(this)+"}"+identifier;
    }
    
    public void destroy() {      
      if (getPending() != null) getPending().cancel();
    }
  }

  /**
   * The purpose of this class is to checkliveness on a stalled socket that we are waiting to write on.
   * 
   * the livenessCheckerTimer is set every time we want to write, and killed every time we do write
   * 
   * TODO: think about exactly what we want to use for the delay on the timer, currently using rto*4
   * 
   * @author Jeff Hoye
   *
   */
  class LSocket extends SocketWrapperSocket<Identifier, Identifier> {
    EntityManager manager;
    
    /**
     * This is for memory management, so that we don't collect the identifier in the EntityManager 
     * while we still have open sockets.
     */
    Identifier hardRef;
    
    /**
     * set every time we want to write, and killed every time we do write
     */
    TimerTask livenessCheckerTimer;
    
    boolean closed = false;
    
    public LSocket(EntityManager manager, P2PSocket<Identifier> socket, Identifier hardRef) {
      super(socket.getIdentifier(), socket, LivenessTransportLayerImpl.this.logger, LivenessTransportLayerImpl.this.errorHandler, socket.getOptions());
      if (hardRef == null) throw new IllegalArgumentException("hardRef == null "+manager+" "+socket);
      this.manager = manager;
      this.hardRef = hardRef;
    }

    // done with call to close
//    public void notifyRecievers() {
//      if (reader != null) reader.receiveException(this, new NodeIsFaultyException(manager.identifier.get()));
//      if (writer != null && writer != reader) writer.receiveException(this, new NodeIsFaultyException(manager.identifier.get()));      
//    }

//    P2PSocketReceiver<Identifier> reader, writer;

    @Override
    public void register(boolean wantToRead, boolean wantToWrite, final P2PSocketReceiver<Identifier> receiver) {     
      if (closed) {
//        logger.logException("closeEx", closeEx);
//        logger.logException("here", new Exception());
        receiver.receiveException(this, new ClosedChannelException("Socket "+this+" is already closed."));
        return;
      }
      if (wantToWrite) startLivenessCheckerTimer();
      super.register(wantToRead, wantToWrite, receiver);
    }
    
    @Override
    public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
      EntityManager m = getManager(socket.getIdentifier());
      if (m.liveness > LIVENESS_SUSPECTED) {
        m.updated = 0L;
        m.checkLiveness(socket.getOptions());
      }
      if (canWrite) {
        stopLivenessCheckerTimer();
      }
      super.receiveSelectResult(socket, canRead, canWrite);
    }
    

    public void startLivenessCheckerTimer() {      
      // it's already going to check
//      stopLivenessCheckerTimer();
      synchronized(LSocket.this) {
        if (livenessCheckerTimer != null) return;
        livenessCheckerTimer = new TimerTask(){      
          @Override
          public void run() {
            synchronized(LSocket.this) {
              if (livenessCheckerTimer == this) livenessCheckerTimer = null;
            }
            manager.checkLiveness(options);
          }      
        };
      } // sync
      if (logger.level <= Logger.FINER) logger.log("Checking liveness on "+manager.identifier.get()+" in "+manager.rto()+" millis if we don't write.");
      timer.schedule(livenessCheckerTimer, manager.rto()*4, 30000);
    }

    public void stopLivenessCheckerTimer() {
      synchronized(LSocket.this) {  
        if (livenessCheckerTimer != null) livenessCheckerTimer.cancel();
        livenessCheckerTimer = null;
      }
    }

//    Exception closeEx;
    @Override
    public void close() {
      closed = true;
//      closeEx = new Exception();
      manager.removeSocket(this);
      super.close();
    }
    
    public String toString() {
      return "LSocket{"+socket+"}";
    }
//    public void closeButDontRemove() {
//      super.close();
//    }
  }

  public void setLiveness(Identifier i, int liveness, Map<String, Object> options) {
    EntityManager man = getManager(i);
    switch(liveness) {
    case LIVENESS_ALIVE:
      man.markAlive(options);
      return;
    case LIVENESS_SUSPECTED:
      man.markSuspected(options);
      return;
    case LIVENESS_DEAD:
      man.markDead(options);
      return;
    case LIVENESS_DEAD_FOREVER:      
      man.markDeadForever(options);
      return;
    }    
  }
}
