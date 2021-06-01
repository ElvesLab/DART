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
import java.net.InetSocketAddress;
import java.util.*;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.pastry.*;
import rice.pastry.join.JoinRequest;
import rice.pastry.routing.*;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.selector.Timer;
import rice.selector.TimerTask;

/**
 * The PartitionHandler does two things:  it collects a list of candidate nodes 
 * that may be in another partition, and it periodically tries to reach these 
 * nodes to heal a partition.  I can refactor the code to expose a method that 
 * heals a partition tomorrow for you, but I'll paste the code below for the 
 * moment.
 * 
 * The basic theory of healing a partition is to send a join request via a node 
 * that may not be in your partition.  Join requests get routed to your own key.
 * If the request comes back to you, then that node is in your partition and 
 * nothing needs to be done.  If the request ends up at another node, it will 
 * discover your existence and begin integrating the two rings, just as if you 
 * were creating your local node for the first time.
 * 
 * The way we look for candidate nodes is we choose a mix of the original 
 * bootstraps (as a last resort fallback) and nodes that we once believed were 
 * alive but are no longer in contact with.  The idea is we may have declared 
 * those nodes dead due to losing connectivity to them but at some later point 
 * we again may be able to contact them.
 * 
 * The reason that we make the PartitionHandler a periodic process is that in 
 * general you can't tell that a partition has happened.
 * 
 * @author jstewart
 *
 */
public class PartitionHandler extends TimerTask implements NodeSetListener {

  protected PastryNode pastryNode;

  // maybe move this to a subclass
  protected InetSocketAddress[] bootstraps;
  
  protected SocketPastryNodeFactory factory;
  
  protected Logger logger;
  
  protected double bootstrapRate;
  protected int maxGoneSize;
  protected int maxGoneAge;
  // map from Id's -> NodeHandle's to keep things unique per NodeId
  protected Map<rice.p2p.commonapi.Id,GoneSetEntry> gone;
  
  protected Environment env;
  
  /**
   * Constructs a PartitionHandler.  This will register the handler with the
   * pastry node to begin collecting candidates nodes that may be in a
   * partition, but it will not initiate probing.  You must call the start()
   * method to do that.
   * 
   * @param pn the local pastry node
   * @param factory the SocketPastryNodeFactory 
   * @param bootstraps
   */
  public PartitionHandler(PastryNode pn, SocketPastryNodeFactory factory, InetSocketAddress[] bootstraps) {
    pastryNode = pn;
    this.factory = factory;
    this.bootstraps = bootstraps;
    env = pastryNode.getEnvironment();
    gone = new HashMap<rice.p2p.commonapi.Id, GoneSetEntry>();
    this.logger = pn.getEnvironment().getLogManager().getLogger(PartitionHandler.class,"");
    
    maxGoneSize = env.getParameters().getInt("partition_handler_max_history_size");
    maxGoneAge = env.getParameters().getInt("partition_handler_max_history_age");
    bootstrapRate = env.getParameters().getDouble("partition_handler_bootstrap_check_rate");
    
    pastryNode.getLeafSet().addNodeSetListener(this);
    pastryNode.getRoutingTable().addNodeSetListener(this);
  }
  
  private synchronized void doGoneMaintainence() {
    Iterator<GoneSetEntry> it = gone.values().iterator();
    long now = env.getTimeSource().currentTimeMillis();

    if (logger.level <= Logger.FINE) logger.log("Doing maintainence in PartitionHandler "+now);

    if (logger.level <= Logger.FINER) logger.log("gone size 1 is "+gone.size()+" of "+maxGoneSize);

    while (it.hasNext()) {
      GoneSetEntry g = (GoneSetEntry)it.next();
      if (now - g.timestamp > maxGoneAge) { // toss if too old
        if (logger.level <= Logger.FINEST) logger.log("Removing "+g+" from gone due to expiry");
        it.remove();
      } else if (g.nh.getLiveness() > NodeHandle.LIVENESS_DEAD) { // toss if epoch changed
        if (logger.level <= Logger.FINEST) logger.log("Removing "+g+" from gone due to death");
        it.remove();
      }
    }
    
    if (logger.level <= Logger.FINER) logger.log("gone size 2 is "+gone.size()+" of "+maxGoneSize);

    while (gone.size() > maxGoneSize) {
      Object key = gone.keySet().iterator().next();
      gone.remove(key);
    }
    if (logger.level <= Logger.FINER) logger.log("gone size 3 is "+gone.size()+" of "+maxGoneSize);
  }

  private List<NodeHandle> getRoutingTableAsList() {
    RoutingTable rt = pastryNode.getRoutingTable();
    List<NodeHandle> rtHandles = new ArrayList<NodeHandle>(rt.numEntries());

    for (int r = 0; r < rt.numRows(); r++) {
      RouteSet[] row = rt.getRow(r);
      for (int c = 0; c < rt.numColumns(); c++) {
        RouteSet entry = row[c];
        if (entry != null) {
          for (int i = 0; i < entry.size(); i++) {
            NodeHandle nh = entry.get(i);
            if (!nh.equals(pastryNode.getLocalHandle())) {
              rtHandles.add(nh);
            }
          }
        }
      }
    }

    return rtHandles;
  }
  
  /**
   * This method randomly returns a node that was once in the LeafSet or in
   * the routing table but has since been removed.  The idea is that the node
   * may have been removed because it suffered a network outage that left it
   * in a partition of the ring.
   * 
   * This method may also return a node from the current routing table if it
   * doesn't know of sufficient nodes that have left to pick a good one.
   * 
   * @return a NodeHandle that may be in another partition, or null if no nodes
   * have left the routing table or LeafSet, and the routing table is empty.
   */
  public NodeHandle getCandidateNode() {
    synchronized (this) {
      int which = env.getRandomSource().nextInt(maxGoneSize);
      if (logger.level <= Logger.FINEST) logger.log("getGone choosing node "+which+" from gone or routing table");
      
      Iterator<GoneSetEntry> it = gone.values().iterator();
      while (which>0 && it.hasNext()) {
        which--;
        it.next();
      }
  
      if (it.hasNext()) {
        // assert which==0;
        if (logger.level <= Logger.FINEST) logger.log("getGone chose node from gone "+which);
        return ((GoneSetEntry)it.next()).nh;
      }
    }

    List rtHandles = getRoutingTableAsList();
    
    if (rtHandles.isEmpty()) {
      if (logger.level <= Logger.INFO) logger.log("getGone returning null; routing table is empty!");
      return null;
    }
    
    int which = env.getRandomSource().nextInt(rtHandles.size());

    if (logger.level <= Logger.FINEST) logger.log("getGone choosing node "+which+" from routing table");

    return (NodeHandle)rtHandles.get(which);
  }
  
  // possibly make this abstract
  private void getNodeHandleToProbe(Continuation<NodeHandle, Exception> c) {
    if (env.getRandomSource().nextDouble() > bootstrapRate) {
      NodeHandle nh = getCandidateNode();
      if (logger.level <= Logger.FINEST) logger.log("getGone chose "+nh);
      if (nh != null) {
        c.receiveResult(nh);
        return;
      }
    }
    if (logger.level <= Logger.FINEST) logger.log("getNodeHandleToProbe choosing bootstrap");
    
    factory.getNodeHandle(bootstraps, c);
  }
  
  public void run() {
    if (logger.level <= Logger.INFO) logger.log("running partition handler");
    doGoneMaintainence();
    
    getNodeHandleToProbe(new Continuation<NodeHandle, Exception>() {

      public void receiveResult(NodeHandle result) {
        if (result != null) {
          rejoin((NodeHandle)result);
        } else {
          if (logger.level <= Logger.INFO) logger.log("getNodeHandleToProbe returned null");
        }
      }

      public void receiveException(Exception result) {
        // oh well
        if (logger.level <= Logger.INFO) logger.logException("exception in PartitionHandler",result);
      }
      
    });
  }

  public void nodeSetUpdate(NodeSetEventSource nodeSetEventSource, NodeHandle handle, boolean added) {
    if (nodeSetEventSource.equals(pastryNode.getLeafSet())) {
      if (added) {
        synchronized(this) {
          gone.remove(handle.getId());
        }
      }
    }
    if (!added) {
      synchronized (this) {
        if (handle.getLiveness() == NodeHandle.LIVENESS_DEAD) {
          if (gone.containsKey(handle.getId())) {
            if (logger.level <= Logger.FINEST) logger.log("PartitionHandler updating node "+handle);
            ((GoneSetEntry)gone.get(handle.getId())).nh = handle;
          } else {
            GoneSetEntry g = new GoneSetEntry(handle, env.getTimeSource().currentTimeMillis());
            if (logger.level <= Logger.FINEST) logger.log("PartitionHandler adding node "+g);
            gone.put(handle.getId(),g);
          }
        }
      }
    }
  }
  
  static private class GoneSetEntry {
    public NodeHandle nh;
    public long timestamp;
    
    public GoneSetEntry(NodeHandle nh, long timestamp) {
      this.nh = nh;
      this.timestamp = timestamp;
    }
    
    public boolean equals(Object o) {
      GoneSetEntry other = (GoneSetEntry)o;
      return other.nh.getId().equals(nh.getId());
    }

    public String toString() {
      return nh.toString() + " "+timestamp;
    }    
  }

  /**
   * This method starts the PartitionHandler's probing of candidate nodes.
   * It should be called after the handler is constructed.
   * 
   * @param timer the timer used to schedule partition checks (normally from the environment)
   */
  public void start(Timer timer) {
    if (logger.level <= Logger.INFO) logger.log("installing partition handler");
    timer.schedule(this, env.getParameters().getInt("partition_handler_check_interval"), 
        env.getParameters().getInt("partition_handler_check_interval"));
  }

  /**
   * Manually kicks off a probe to a given target node.  This can be used to
   * manually heal a partition if you know via some external mechanism that one
   * occurred.
   * 
   * @param target the node to rejoin through
   */
  public void rejoin(NodeHandle target) {
    JoinRequest jr = new JoinRequest(pastryNode.getLocalHandle(), pastryNode
        .getRoutingTable().baseBitLength());
 
    RouteMessage rm = new RouteMessage(pastryNode.getLocalHandle().getNodeId(), 
        jr, null, null,
        (byte)env.getParameters().getInt("pastry_protocol_router_routeMsgVersion"));

    rm.setPrevNode(pastryNode.getLocalHandle());
    rm.getOptions().setRerouteIfSuspected(false);
    NodeHandle nh = pastryNode.coalesce(target);
    try {
      nh.bootstrap(rm);
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.logException("Error bootstrapping.",ioe); 
    }
  }

}
