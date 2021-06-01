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
/*
 * Created on Nov 8, 2005
 */
package rice.pastry.direct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.direct.Delivery;
import org.mpisws.p2p.transport.direct.DirectTransportLayer;
import org.mpisws.p2p.transport.direct.EventSimulator;
import org.mpisws.p2p.transport.direct.GenericNetworkSimulator;
import org.mpisws.p2p.transport.liveness.LivenessListener;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.environment.random.RandomSource;
import rice.environment.time.TimeSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.CancellableTask;
import rice.pastry.transport.NodeHandleAdapter;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;

public class BasicNetworkSimulator<Identifier, MessageType> extends EventSimulator implements 
    GenericNetworkSimulator<Identifier, MessageType> {
  class Tupel {
    Identifier i;
    DirectTransportLayer<Identifier, MessageType> tl;
    NodeRecord record;
    
    public Tupel(Identifier i, DirectTransportLayer<Identifier, MessageType> tl, NodeRecord record) {
      super();
      this.i = i;
      this.tl = tl;
      this.record = record;
    }
  }
  
  /**
   * This maps to the next highest transport layer
   */
  Map<Identifier, Tupel> nodes = Collections.synchronizedMap(new HashMap<Identifier, Tupel>());


  // to notify of send/receive
  NetworkSimulator<Identifier, MessageType> sim; 
  
  protected int MIN_DELAY = 1;
    
  protected final int maxDiameter;
  protected final int minDelay;
  
  public BasicNetworkSimulator(Environment env, RandomSource random, NetworkSimulator<Identifier, MessageType> sim) {
    super(env, random, env.getLogManager().getLogger(BasicNetworkSimulator.class, null));
    this.sim = sim;
    manager.useLoopListeners(false);
    Parameters params = env.getParameters();
    maxDiameter = params.getInt("pastry_direct_max_diameter");
    minDelay = params.getInt("pastry_direct_min_delay");
    start();
  }

  private void addTask(TimerTask dtt) {
    if (logger.level <= Logger.FINE) logger.log("addTask("+dtt+")");
//    System.out.println("addTask("+dtt+")");
//    synchronized(taskQueue) {
//      taskQueue.add(dtt);
//    }
    manager.getTimer().schedule(dtt);
//    start();
//    if (!manager.isSelectorThread()) Thread.yield();
  }
  
  public CancellableTask enqueueDelivery(Delivery d, int delay) {
    long time = timeSource.currentTimeMillis()+delay;
    if (logger.level <= Logger.FINE)      
      logger.log("BNS: enqueueDelivery " + d+":"+time);
    DeliveryTimerTask dtt = null;
    dtt = new DeliveryTimerTask(d, time, d.getSeq());
    addTask(dtt);
    return dtt;
  }
  
  /**
   * node should always be a local node, because this will be delivered instantly
   */
  public Cancellable deliverMessage(MessageType msg, Identifier node, Identifier from) {
    return deliverMessage(msg, node, from, 0);
  }

  public Cancellable deliverMessage(MessageType msg, Identifier node, Identifier from,
      int delay) {
    if (delay > 0) {
      sim.notifySimulatorListenersSent(msg, from, node, delay);
    }
    return deliverMessage(msg, node, from, delay, 0);
  }

  void notifySimulatorListenersReceived(MessageType m, Identifier from, Identifier to) {
    sim.notifySimulatorListenersReceived(m, from, to);    
  }

  public Cancellable deliverMessageFixedRate(MessageType msg,
      Identifier node, Identifier from, int delay, int period) {
    return deliverMessage(msg, node, from, delay, period);
  }
  
  public Cancellable deliverMessage(MessageType msg, Identifier node, Identifier from, int delay, int period) {
    if (logger.level <= Logger.FINE)
      logger.log("BNS: deliver " + msg + " to " + node);
    
    DirectTimerTask dtt = null;
    
    if (from == null || isAlive(from)) {
      MessageDelivery<Identifier, MessageType> md = new MessageDelivery<Identifier, MessageType>(msg, node, from, null, this);
      dtt = new DirectTimerTask(md, timeSource.currentTimeMillis()+ delay,period);
      addTask(dtt);
    }
    return dtt;
  }

  
  /**
   * set the liveliness of a NodeId
   * 
   * @param nid the NodeId being set
   * @param alive the value being set
   */
//  public void destroy(TLPastryNode node) {
//    node.destroy();
//    // NodeRecord nr = (NodeRecord) nodeMap.get(nid);
//    //
//    // if (nr == null) {
//    // throw new Error("setting node alive for unknown node");
//    // }
//    //
//    // if (nr.alive != alive) {
//    // nr.alive = alive;
//    //
//    // DirectNodeHandle[] handles = (DirectNodeHandle[]) nr.handles.toArray(new
//    // DirectNodeHandle[0]);
//    //
//    // for (int i = 0; i < handles.length; i++) {
//    // if (alive) {
//    // handles[i].notifyObservers(NodeHandle.DECLARED_LIVE);
//    // } else {
//    // handles[i].notifyObservers(NodeHandle.DECLARED_DEAD);
//    // }
//    // }
//    // }
//  }


  public void registerIdentifier(Identifier i, DirectTransportLayer<Identifier, MessageType> dtl, NodeRecord record) {
    //logger.log("registerIdentifier("+i+") on thread "+Thread.currentThread());
    nodes.put(i, new Tupel(i, dtl, record));
  }

  public void remove(Identifier i) {
    if (!environment.getSelectorManager().isSelectorThread()) throw new IllegalStateException("Operation not permitted on non-selector thread.");
    nodes.remove(i);
    notifyLivenessListeners(i, LivenessListener.LIVENESS_DEAD, null);
  }

  public Environment getEnvironment() {
    return environment;
  }

  public Environment getEnvironment(Identifier i) {
    return nodes.get(i).tl.getEnvironment();
  }
  
  public RandomSource getRandomSource() {
    return random;
  }

  public boolean isAlive(Identifier i) {
    return nodes.containsKey(i);
  }
  
  public DirectTransportLayer<Identifier, MessageType> getTL(Identifier i) {
    Tupel t = nodes.get(i);
    if (t == null) return null;
    return t.tl;
//    NodeHandleAdapter nha = (NodeHandleAdapter)t.tl;
//    return (DirectTransportLayer<Identifier, MessageType>)nha.getTL();
//    return (DirectTransportLayer<Identifier, MessageType>)nodes.get(i).tl;
  }
  
  /**
   * computes the one-way distance between two NodeIds
   * 
   * @param a the first NodeId
   * @param b the second NodeId
   * @return the proximity between the two input NodeIds
   */
  public float networkDelay(Identifier a, Identifier b) {
    Tupel ta = nodes.get(a);
    Tupel tb = nodes.get(b);
    if (ta == null) {
      throw new RuntimeException("asking about node proximity for unknown node "+a);
    }
      
    if (tb == null) {
      throw new RuntimeException("asking about node proximity for unknown node "+b);      
    }
    
    NodeRecord nra = ta.record;
    NodeRecord nrb = tb.record;
    
    return nra.networkDelay(nrb);
  }

  /**
   * computes the rtt between two NodeIds
   * 
   * @param a the first NodeId
   * @param b the second NodeId
   * @return the proximity between the two input NodeIds
   */
  public float proximity(Identifier a, Identifier b) {
    Tupel ta = nodes.get(a);
    Tupel tb = nodes.get(b);
    if (ta == null) {
      throw new RuntimeException("asking about node proximity for unknown node "+a+" "+b);
    }
      
    if (tb == null) {
      throw new RuntimeException("asking about node proximity for unknown node "+b+" "+a);      
    }
    
    NodeRecord nra = ta.record;
    NodeRecord nrb = tb.record;
    
    return nra.proximity(nrb);
  }

  public NodeRecord getNodeRecord(DirectNodeHandle handle) {
    Tupel t = nodes.get(handle);
    if (t == null) return null;
    return t.record;
  }

  List<LivenessListener<Identifier>> livenessListeners = new ArrayList<LivenessListener<Identifier>>();
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
    if (logger.level <= Logger.FINER) logger.log("notifyLivenessListeners("+i+","+liveness+"):"+livenessListeners.get(0));
    List<LivenessListener<Identifier>> temp;
    synchronized(livenessListeners) {
      temp = new ArrayList<LivenessListener<Identifier>>(livenessListeners);
    }
    for (LivenessListener<Identifier> listener : temp) {
      listener.livenessChanged(i, liveness, options);
    }
  }

  public boolean checkLiveness(Identifier i, Map<String, Object> options) {
    return false;
  }

  public int getLiveness(Identifier i, Map<String, Object> options) {
    if (nodes.containsKey(i)) {
      return LivenessListener.LIVENESS_ALIVE;
    }
    return LivenessListener.LIVENESS_DEAD;
  }

  public void clearState(Identifier i) {
    throw new IllegalStateException("not implemented");
  }
}
