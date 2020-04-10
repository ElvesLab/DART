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
package rice.pastry.standard;

import java.io.IOException;
import java.util.*;

import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.TimeSource;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.TimerWeakHashMap;
import rice.pastry.*;
import rice.pastry.client.PastryAppl;
import rice.pastry.leafset.BroadcastLeafSet;
import rice.pastry.leafset.InitiateLeafSetMaintenance;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.LeafSetProtocol;
import rice.pastry.leafset.LeafSetProtocolAddress;
import rice.pastry.leafset.RequestLeafSet;
import rice.pastry.messaging.*;
import rice.pastry.routing.RoutingTable;
import rice.selector.TimerTask;

/**
 * An implementation of a periodic-style leafset protocol
 * 
 * @version $Id: PeriodicLeafSetProtocol.java 4654 2009-01-08 16:33:07Z jeffh $
 * 
 * @author Alan Mislove
 */
public class PeriodicLeafSetProtocol extends PastryAppl implements ReadyStrategy, NodeSetListener, Observer, LeafSetProtocol {

  protected NodeHandle localHandle;

  protected PastryNode localNode;

  protected LeafSet leafSet;

  protected RoutingTable routeTable;

  /**
   * NodeHandle -> Long remembers the TIME when we received a BLS from that
   * NodeHandle
   */
  protected Map<NodeHandle, Long> lastTimeReceivedBLS; // the leases you have
  protected Map<NodeHandle, Long> lastTimeSentBLS; // the leases you have issued

  /**
   * Related to rapidly determining direct neighbor liveness.
   */
  public final int PING_NEIGHBOR_PERIOD; // 20 sec

  public final int LEASE_PERIOD; // 30 sec
  
  public final int BLS_THROTTLE; // 10 sec

  ScheduledMessage pingNeighborMessage;

  RandomSource random;
  
  TimeSource timeSource;
  
  public static class PLSPMessageDeserializer extends PJavaSerializedDeserializer {

    public PLSPMessageDeserializer(PastryNode pn) {
      super(pn); 
    }
    
    public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
      switch (type) {
        case RequestLeafSet.TYPE:
          return new RequestLeafSet(sender, buf);
        case BroadcastLeafSet.TYPE:
          return new BroadcastLeafSet(buf, pn);
      }
      return null;
    }
     
  }
  
  /**
   * Builds a periodic leafset protocol
   * 
   */
  public PeriodicLeafSetProtocol(PastryNode ln, NodeHandle local,
      LeafSet ls, RoutingTable rt) {
    super(ln, null, LeafSetProtocolAddress.getCode(), new PLSPMessageDeserializer(ln));    
    this.localNode = ln;
    ln.addObserver(this); // to call start() if we are the seed node
    
    Parameters params = ln.getEnvironment().getParameters();
    if (params.contains("pastry_periodic_leafset_protocol_use_own_random")
        && params.getBoolean("pastry_periodic_leafset_protocol_use_own_random")) {
      if (params.contains("pastry_periodic_leafset_protocol_random_seed")
          && !params.getString("pastry_periodic_leafset_protocol_random_seed").equalsIgnoreCase(
              "clock")) {
        this.random = new SimpleRandomSource(params
            .getLong("pastry_periodic_leafset_protocol_random_seed"), ln.getEnvironment().getLogManager(),
            "socket");
      } else {
        this.random = new SimpleRandomSource(ln.getEnvironment().getLogManager(), "periodic_leaf_set");
      }
    } else {
      this.random = ln.getEnvironment().getRandomSource();
    }
    
    this.timeSource = ln.getEnvironment().getTimeSource();
    this.localHandle = local;
    
    // make sure to register all the existing leafset entries
    this.leafSet = ls;
    Iterator<NodeHandle> i = this.leafSet.asList().iterator();
    while(i.hasNext()) {
      NodeHandle nh = (NodeHandle)i.next(); 
      nh.addObserver(this, 50);
    }

    this.routeTable = rt;
    this.lastTimeReceivedBLS = new TimerWeakHashMap<NodeHandle, Long>(ln.getEnvironment().getSelectorManager().getTimer(), 300000);
    this.lastTimeSentBLS = new TimerWeakHashMap<NodeHandle, Long>(ln.getEnvironment().getSelectorManager().getTimer(), 300000);
    Parameters p = ln.getEnvironment().getParameters();
    PING_NEIGHBOR_PERIOD = p.getInt("pastry_protocol_periodicLeafSet_ping_neighbor_period"); // 20 seconds
    LEASE_PERIOD = p.getInt("pastry_protocol_periodicLeafSet_lease_period");  // 30 seconds
    BLS_THROTTLE = p.getInt("pastry_protocol_periodicLeafSet_request_lease_throttle");// 10 seconds
    this.lastTimeRenewedLease = new TimerWeakHashMap<NodeHandle, Long>(ln.getEnvironment().getSelectorManager().getTimer(), LEASE_PERIOD*2);

    // Removed after meeting on 5/5/2005 Don't know if this is always the
    // appropriate policy.
    // leafSet.addObserver(this);
    pingNeighborMessage = localNode.scheduleMsgAtFixedRate(
        new InitiatePingNeighbor(), PING_NEIGHBOR_PERIOD, PING_NEIGHBOR_PERIOD);
  }

  private void updateRecBLS(NodeHandle from, long time) {
    if (time == 0) return;
    Long oldTime = (Long) lastTimeReceivedBLS.get(from);
    if ((oldTime == null) || (oldTime.longValue() < time)) {
      lastTimeReceivedBLS.put(from, new Long(time));      
      long leaseTime = time+LEASE_PERIOD-timeSource.currentTimeMillis();
      if (logger.level <= Logger.FINE) {
        logger.log("PLSP.updateRecBLS("+from+","+time+"):"+leaseTime);
      } else {
        if (leaseTime < 10 && logger.level <= Logger.INFO) {
          logger.log("PLSP.updateRecBLS("+from+","+time+"):"+leaseTime);
        }
      }              
        
      // need to do this so that nodes are notified
      if (hasSetStrategy)
        isReady();
    } 
  }
  
  /**
   * Receives messages.
   * 
   * @param msg the message.
   */
  public void receiveMessage(Message msg) {
    if (msg instanceof BroadcastLeafSet) {
      // receive a leafset from another node
      BroadcastLeafSet bls = (BroadcastLeafSet) msg;

      // if we have now successfully joined the ring, set the local node ready
      if (bls.type() == BroadcastLeafSet.JoinInitial) {
        // merge the received leaf set into our own
        leafSet.merge(bls.leafSet(), bls.from(), routeTable, false,
            null);

        // localNode.setReady();
        broadcastAll();
      } else {
        // first check for missing entries in their leafset
        NodeSet set = leafSet.neighborSet(Integer.MAX_VALUE);

        // don't need to remove any nodes that we already found faulty, because leafset.merge does not accept them
        
        // if we find any missing entries, check their liveness
        for (int i = 0; i < set.size(); i++)
          if (bls.leafSet().test(set.get(i)))
            set.get(i).checkLiveness();

        // now check for assumed-dead entries in our leafset
        set = bls.leafSet().neighborSet(Integer.MAX_VALUE);

        // if we find any missing entries, check their liveness
        for (int i = 0; i < set.size(); i++)
          if (!set.get(i).isAlive())
            set.get(i).checkLiveness();

        // merge the received leaf set into our own
        leafSet.merge(bls.leafSet(), bls.from(), routeTable, false,
            null);
      }
      // do this only if you are his proper neighbor !!!
      if ((bls.leafSet().get(1) == localHandle) ||
          (bls.leafSet().get(-1) == localHandle)) {
        updateRecBLS(bls.from(), bls.getTimeStamp());
      }
      
    } else if (msg instanceof RequestLeafSet) {
      // request for leaf set from a remote node
      RequestLeafSet rls = (RequestLeafSet) msg;

      if (rls.getTimeStamp() > 0) { // without a timestamp, this is just a normal request, not a lease request
        // remember that we gave out a lease, and go unReady() if the node goes faulty
        lastTimeRenewedLease.put(rls.returnHandle(),new Long(timeSource.currentTimeMillis()));

        // it's important to put the node in the leafset so that we don't accept messages for a node that
        // we issued a lease to
        // it's also important to record issuing the lease before putting into the leafset, so we can 
        // call removeFromLeafsetIfPossible() in leafSetChange()
        leafSet.put(rls.returnHandle());
                
        // nodes were never coming back alive when we called them faulty incorrectly
        if (!rls.returnHandle().isAlive()) {
          if (logger.level <= Logger.INFO) logger.log("Issued lease to dead node:"+rls.returnHandle()+" initiating checkLiveness()");
          rls.returnHandle().checkLiveness();
          
        }
      }
      
      // I don't like that we're sending the message after setting lastTimeRenewedLease, becuase there
      // could be a delay between here and above, but this should still be fine, because with
      // the assumption that the clocks are both advancing normally, we still should not get inconsistency
      // because the other node will stop receiving 30 seconds after he requested the lease, and we
      // wont receive until 30 seconds after the lastTimeRenewedLease.put() above
      thePastryNode.send(rls.returnHandle(),
          new BroadcastLeafSet(localHandle, leafSet, BroadcastLeafSet.Update, rls.getTimeStamp()),null, options);
      
    } else if (msg instanceof InitiateLeafSetMaintenance) {
      // perform leafset maintenance
      NodeSet set = leafSet.neighborSet(Integer.MAX_VALUE);

      if (set.size() > 1) {
        NodeHandle handle = set.get(random.nextInt(set.size() - 1) + 1);
        thePastryNode.send(handle,
            new RequestLeafSet(localHandle, timeSource.currentTimeMillis()),null, options);
        thePastryNode.send(handle,
            new BroadcastLeafSet(localHandle, leafSet, BroadcastLeafSet.Update, 0),null, options);

        NodeHandle check = set.get(random
            .nextInt(set.size() - 1) + 1);
        check.checkLiveness();
      }
    } else if (msg instanceof InitiatePingNeighbor) {
      // IPN every 20 seconds
      NodeHandle left = leafSet.get(-1);
      NodeHandle right = leafSet.get(1);

      // send BLS to left/right neighbor
      // ping if we don't currently have a lease
      if (left != null) {
        sendBLS(left, !hasLease(left));
      }
      if (right != null) {
        sendBLS(right, !hasLease(right));
      }
    }
  }

  /**
   * Broadcast the leaf set to all members of the local leaf set.
   * 
   * @param type the type of broadcast message used
   */
  protected void broadcastAll() {
    BroadcastLeafSet bls = new BroadcastLeafSet(localHandle, leafSet,
        BroadcastLeafSet.JoinAdvertise, 0);
    NodeSet set = leafSet.neighborSet(Integer.MAX_VALUE);

    for (int i = 1; i < set.size(); i++)
      thePastryNode.send(set.get(i), bls,null, options);
  }

  // Ready Strategy
  boolean hasSetStrategy = false;

  public void start() {
    if (!hasSetStrategy) {
      if (logger.level <= Logger.INFO) logger.log("PLSP.start(): Setting self as ReadyStrategy");
      localNode.setReadyStrategy(this);
      hasSetStrategy = true;
      localNode.addLeafSetListener(this);
      // to notify listeners now if we have proper leases
      isReady();
    }
  }
  
  public void stop() {
    if (hasSetStrategy) {
      if (logger.level <= Logger.INFO) logger.log("PLSP.start(): Removing self as ReadyStrategy");
      hasSetStrategy = false; 
      localNode.deleteLeafSetListener(this);
    }
  }
  
  /**
   * Called when the leafset changes
   */
  NodeHandle lastLeft;
  NodeHandle lastRight;
  public void nodeSetUpdate(NodeSetEventSource nodeSetEventSource, NodeHandle handle, boolean added) {
//    if ((!added) && (
//      handle == lastLeft ||
//      handle == lastRight
//      )) {
//      // check to see if we have an existing lease
//      long curTime = localNode.getEnvironment().getTimeSource().currentTimeMillis();
//      long leaseOffset = curTime-LEASE_PERIOD;
//
//      Long time = (Long)lastTimeRenewedLease.get(handle);
//      if (time != null
//          && (time.longValue() >= leaseOffset)) {
//        // we gave out a lease too recently
//        TimerTask deadLease = 
//        new TimerTask() {        
//          public void run() {
//            deadLeases.remove(this);
//            isReady();            
//          }        
//        };
//        deadLeases.add(deadLease);
//        localNode.getEnvironment().getSelectorManager().getTimer().schedule(deadLease,
//            time.longValue()-leaseOffset);
//        isReady();
//      }      
//    }
    NodeHandle newLeft = leafSet.get(-1);
    if (newLeft != null && (lastLeft != newLeft)) {
      lastLeft = newLeft;
      sendBLS(lastLeft,true);
    }
    NodeHandle newRight = leafSet.get(1);
    if (newRight != null && (lastRight != newRight)) {
      lastRight = newRight;
      sendBLS(lastRight,true);
    }
  }
  
  boolean ready = false;
  public void setReady(boolean r) {
    if (ready != r) {
      synchronized(thePastryNode) {
        ready = r; 
      }
      thePastryNode.notifyReadyObservers();
    }
  }
  
  /**
   * 
   *
   */
  public boolean isReady() {
    // check to see if we've heard from the left/right neighbors recently enough
    boolean shouldBeReady = shouldBeReady(); // temp
//    if (!shouldBeReady) {      
//      // I suspect this is unnecessary because this timer should be well underway of sorting this out
//      receiveMessage(new InitiatePingNeighbor());
//    }
    
    if (shouldBeReady != ready) {
      thePastryNode.setReady(shouldBeReady); // will call back in to setReady() and notify the observers
    }
    
//    logger.log("isReady() = "+shouldBeReady);
    return shouldBeReady;
  }
  
//  HashSet deadLeases = new HashSet();
  
  public boolean shouldBeReady() {
    
    NodeHandle left = leafSet.get(-1);
    NodeHandle right = leafSet.get(1);
    
    // do it this way so we get going on both leases if need be
    boolean ret = true;
    
    // see if received BLS within past LEASE_PERIOD seconds from left neighbor
    if (!hasLease(left)) {
      ret = false;
      sendBLS(left, true); 
    }
    
    // see if received BLS within past LEASE_PERIOD seconds from right neighbor
    if (!hasLease(right)) {
      ret = false;
      sendBLS(right, true); 
    }
    
//    if (deadLeases.size() > 0) return false;
    return ret;
  }
  
  /**
   * Do we have a lease from this node?
   * 
   * Returns true if nh is null.
   * 
   * @param nh the NodeHandle we are interested if we have a lease from
   * @return if we have a lease from the NodeHandle
   */
  public boolean hasLease(NodeHandle nh) {
    long curTime = timeSource.currentTimeMillis();
    long leaseOffset = curTime-LEASE_PERIOD;

    if (nh != null) {
      Long time = (Long) lastTimeReceivedBLS.get(nh);
      if (time == null
          || (time.longValue() < leaseOffset)) {
        // we don't have a lease
        return false;
      }      
    }     
    return true;
  }
  
  /**
   * 
   * @param sendTo
   * @return true if we sent it, false if we didn't because of throttled
   */
  private boolean sendBLS(NodeHandle sendTo, boolean checkLiveness) {
    Long time = (Long) lastTimeSentBLS.get(sendTo);
    long currentTime = timeSource.currentTimeMillis();
    if (time == null
        || (time.longValue() < (currentTime - BLS_THROTTLE))) {
      if (logger.level <= Logger.FINE) // only log if not throttled
        logger.log("PeriodicLeafSetProtocol: Checking liveness on neighbor:"+ sendTo+" "+time+" cl:"+checkLiveness);
      lastTimeSentBLS.put(sendTo, new Long(currentTime));

      thePastryNode.send(sendTo, new BroadcastLeafSet(localHandle, leafSet, BroadcastLeafSet.Update, 0), null, options);
      thePastryNode.send(sendTo, new RequestLeafSet(localHandle, currentTime), null, options);
      if (checkLiveness) {
        sendTo.checkLiveness();
      }
      return true;
    } 
    return false;
  }
  
  /**
   * NodeHandle -> time
   * 
   * Leases we have issued.  We cannot remove the node from the leafset until this expires.
   * 
   * If this node is found faulty (and you took over the leafset), must go non-ready until lease expires
   */
  Map<NodeHandle, Long> lastTimeRenewedLease;
  
  /**
   * Used to kill self if leafset shrunk by too much. NOTE: PLSP is not
   * registered as an observer.
   * 
   */
  // public void update(Observable arg0, Object arg1) {
  // NodeSetUpdate nsu = (NodeSetUpdate)arg1;
  // if (!nsu.wasAdded()) {
  // if (localNode.isReady() && !leafSet.isComplete() && leafSet.size() <
  // (leafSet.maxSize()/2)) {
  // // kill self
  // localNode.getEnvironment().getLogManager().getLogger(PeriodicLeafSetProtocol.class,
  // null).log(Logger.SEVERE,
  // "PeriodicLeafSetProtocol:
  // "+localNode.getEnvironment().getTimeSource().currentTimeMillis()+" Killing
  // self due to leafset collapse. "+leafSet);
  // localNode.resign();
  // }
  // }
  // }
  /**
   * Should not be called becasue we are overriding the receiveMessage()
   * interface anyway.
   */
  public void messageForAppl(Message msg) {
    throw new RuntimeException("Should not be called.");
  }

  /**
   * We always want to receive messages.
   */
  public boolean deliverWhenNotReady() {
    return true;
  }

  boolean destroyed = false;
  public void destroy() {
    if (logger.level <= Logger.INFO)
      logger.log("PLSP: destroy() called");
    destroyed = true;
    if (pingNeighborMessage != null)
      pingNeighborMessage.cancel();
    pingNeighborMessage = null;
    lastLeft = null;
    lastRight = null;
    lastTimeReceivedBLS.clear();
    lastTimeRenewedLease.clear();
    lastTimeSentBLS.clear();
//    deadLeases.clear();
  }

  @Override
  public void leafSetChange(NodeHandle nh, boolean wasAdded) {
    super.leafSetChange(nh, wasAdded);
//    logger.log("leafSetChange("+nh+","+wasAdded+")");
    if (wasAdded) {
      nh.addObserver(this, 50); 
      if (!nh.isAlive()) {
        // nh.checkLiveness(); // this is done in all of the calling code (in this Protocol)
        removeFromLeafsetIfPossible(nh); 
      }
    } else {
      if (logger.level <= Logger.FINE) logger.log("Removed "+nh+" from the LeafSet.");
      nh.deleteObserver(this); 
    }
  }

  /**
   * Only remove the item if you did not give a lease.
   */
  public void update(final Observable o, final Object arg) {
    if (destroyed) return;
//    logger.log("update("+o+","+arg+")");
    if (o instanceof NodeHandle) {      
      if (arg == NodeHandle.DECLARED_DEAD) {
        removeFromLeafsetIfPossible((NodeHandle)o);
      }
      return;
    }    
    
    // this is if we are the "seed" node
    if (o instanceof PastryNode) {
      if (arg instanceof Boolean) { // could also be a JoinFailedException
        Boolean rdy = (Boolean)arg;
        if (rdy.equals(Boolean.TRUE)) {
          localNode.deleteObserver(this);
          start();
        }
      }
    }
  }

  public void removeFromLeafsetIfPossible(final NodeHandle nh) {
    if (nh.isAlive()) return;
    Long l_time = (Long)lastTimeRenewedLease.get(nh);
    if (l_time == null) {
      // there is no lease on record
      leafSet.remove(nh);
    } else {
      // verify doesn't have a current lease
      long leaseExpiration = l_time.longValue()+LEASE_PERIOD;
      long now = timeSource.currentTimeMillis();
      if (leaseExpiration > now) {
        if (logger.level <= Logger.INFO) logger.log("Removing "+nh+" from leafset later."+(leaseExpiration-now));
        // remove it later when lease expries
        thePastryNode.getEnvironment().getSelectorManager().getTimer().schedule(new TimerTask() {          
          @Override
          public void run() {
            if (logger.level <= Logger.FINE) logger.log("removeFromLeafsetIfPossible("+nh+")");
            // do this recursively in case we issue a new lease
            removeFromLeafsetIfPossible(nh);
          }          
        }, leaseExpiration-now);
      } else {
        // lease has expired
        leafSet.remove(nh);
      }      
    }            
  }
  
}
