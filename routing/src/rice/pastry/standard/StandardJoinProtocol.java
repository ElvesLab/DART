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

import rice.Continuation;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.*;
import rice.pastry.*;
import rice.pastry.messaging.*;
import rice.pastry.leafset.*;
import rice.pastry.routing.*;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;
import rice.pastry.client.PastryAppl;
import rice.pastry.join.*;

import java.io.IOException;
import java.util.*;

/**
 * An implementation of a simple join protocol.
 * 
 * Overview:
 * initiateJoin() causes InitiateJoinMsg to be sent periodically on the joiner
 * causes handleInitiateJoin() constructs a RouteMessage(JoinRequest) and sends it to the boostrap
 * causes handleIntermediateHop() on each node along the route
 * causes respondToJoiner() on last hop
 * causes completeJoin() on joiner
 * 
 * @version $Id: StandardJoinProtocol.java 4222 2008-05-26 09:14:30Z jeffh $
 * 
 * @author Peter Druschel
 * @author Andrew Ladd
 * @author Rongmei Zhang
 * @author Y. Charlie Hu
 * @author Jeff Hoye
 */

public class StandardJoinProtocol extends PastryAppl implements JoinProtocol {
  protected NodeHandle localHandle;

  protected RoutingTable routeTable;

  protected LeafSet leafSet;
  
  public static class SJPDeserializer extends PJavaSerializedDeserializer {
    public SJPDeserializer(PastryNode pn) {
      super(pn);
    }

    @Override
    public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
      switch(type) {
        case JoinRequest.TYPE:
          return new JoinRequest(buf,pn, (NodeHandle)sender, pn);
      }      
      return null;
    }
  }
  
  /**
   * Constructor.
   * 
   * @param lh the local node handle.
   * @param sm the Pastry security manager.
   */

  public StandardJoinProtocol(PastryNode ln, NodeHandle lh,RoutingTable rt, LeafSet ls) {
    this(ln, lh, rt, ls, null);
  }
  
  public StandardJoinProtocol(PastryNode ln, NodeHandle lh,RoutingTable rt, LeafSet ls, MessageDeserializer md) {
    super(ln, null, JoinAddress.getCode(), md == null ? new SJPDeserializer(ln) : md);
    localHandle = lh;

    routeTable = rt;
    leafSet = ls;    
  }

  /**
   * Get address.
   * 
   * @return gets the address.
   */
  public int getAddress() {
    return JoinAddress.getCode();
  }

  // join retransmission stuff
  protected ScheduledMessage joinEvent;

  public void initiateJoin(Collection<NodeHandle> bootstrap) {
    if (logger.level <= Logger.CONFIG)
      logger.log("initiateJoin(" + bootstrap + ")");
    if (bootstrap == null || bootstrap.isEmpty()) {
      // no bootstrap node, so ready immediately
      thePastryNode.setReady();
    } else {
      // schedule (re-)transmission of the join message at an exponential backoff
      joinEvent = new ExponentialBackoffScheduledMessage(
          thePastryNode, new InitiateJoin(bootstrap),
          thePastryNode.getEnvironment().getSelectorManager().getTimer(),
          0, 2000, 2, 60000);
    }
  }

  /**
   * Receives a message from the outside world.
   * 
   * @param msg the message that was received.
   */
  public void receiveMessage(Message msg) {
    if (msg instanceof JoinRequest) {
      JoinRequest jr = (JoinRequest) msg;
      handleJoinRequest(jr);
    } else if (msg instanceof RouteMessage) { // a join request message at an intermediate node
      RouteMessage rm = (RouteMessage) msg;
      handleIntermediateHop(rm);
    } else if (msg instanceof InitiateJoin) { // request from the local node to join
      InitiateJoin ij = (InitiateJoin) msg;
      handleInitiateJoin(ij);
    }
  }

  protected void handleInitiateJoin(InitiateJoin ij) {
    final NodeHandle nh = ij.getHandle();

    if (nh == null) {
      if (logger.level <= Logger.SEVERE) logger.log(
          "ERROR: Cannot join ring.  All bootstraps are faulty."+ij); 
      thePastryNode.joinFailed(new JoinFailedException("Cannot join ring.  All bootstraps are faulty."+ij));
    } else {
      if (logger.level <= Logger.INFO) logger.log("InitiateJoin attempting to join:"+nh+" liveness:"+nh.getLiveness());
//      if (nh.isAlive() == true) { // this was already done in ij.getHandle()
        getJoinRequest(nh, new Continuation<JoinRequest, Exception>() {
        
          public void receiveResult(JoinRequest jr) {
            RouteMessage rm = new RouteMessage(localHandle.getNodeId(), jr, null, null,
                (byte)thePastryNode.getEnvironment().getParameters().getInt("pastry_protocol_router_routeMsgVersion"));

            rm.getOptions().setRerouteIfSuspected(false);
            rm.setPrevNode(localHandle);
            thePastryNode.send(nh, rm, null, getOptions(jr, options));
          }
        
          public void receiveException(Exception exception) {
            // TODO Auto-generated method stub
        
          }
        
        });

//      }
    }    
  }
  
  protected void getJoinRequest(NodeHandle bootstrap, Continuation<JoinRequest, Exception> deliverJRToMe) {
    JoinRequest jr = new JoinRequest(localHandle, thePastryNode
        .getRoutingTable().baseBitLength(), thePastryNode.getEnvironment().getTimeSource().currentTimeMillis());    
    deliverJRToMe.receiveResult(jr); 
  }

  protected void handleIntermediateHop(RouteMessage rm) {
    try {
      JoinRequest jr = (JoinRequest) rm.unwrap(deserializer);

      Id localId = localHandle.getNodeId();
      NodeHandle jh = jr.getHandle();
      Id nid = jh.getNodeId();

      if (!jh.equals(localHandle)) {
        int base = thePastryNode.getRoutingTable().baseBitLength();
  
        int msdd = localId.indexOfMSDD(nid, base);
        int last = jr.lastRow();
  
        for (int i = last - 1; msdd > 0 && i >= msdd; i--) {
          RouteSet[] row = routeTable.getRow(i);
  
          jr.pushRow(row);
        }
        
        rm.setRouteMessageNotification(new RouteMessageNotification(){          
          public void sendSuccess(RouteMessage message, NodeHandle nextHop) {
            if (logger.level <= Logger.CONFIG) logger.log("sendSuccess("+message+"):"+nextHop);
          }
        
          public void sendFailed(RouteMessage message, Exception e) {
            if (logger.level <= Logger.CONFIG) logger.logException("sendFailed("+message+") ",e);
          }          
        });
        
        rm.setTLOptions(getOptions(jr, rm.getTLOptions()));
  
        if (logger.level <= Logger.CONFIG) logger.log("Routing "+rm);
        thePastryNode.getRouter().route(rm);
      }      
    } catch (IOException ioe) {
      if (logger.level <= Logger.SEVERE) logger.logException("StandardJoinProtocol.receiveMessage()",ioe); 
    }        
  }
  
  protected void handleJoinRequest(final JoinRequest jr) {
    if (jr.accepted() == false) {
      respondToJoiner(jr);
    } else { // this is the node that initiated the join request in the first
              // place
      completeJoin(jr);
    }
  }

  protected void respondToJoiner(final JoinRequest jr) {
    NodeHandle joiner = jr.getHandle();

    // this is the terminal node on the request path
    // leafSet.put(joiner);
    if (thePastryNode.isReady()) {
      if (logger.level <= Logger.CONFIG) logger.log("acceptJoin "+jr);
      jr.acceptJoin(localHandle, leafSet);
      thePastryNode.send(joiner,jr,new PMessageNotification(){          
        public void sent(PMessageReceipt msg) {
          if (logger.level <= Logger.CONFIG) logger.log("acceptJoin.sent("+msg+"):"+jr);
        }          
        public void sendFailed(PMessageReceipt msg, Exception reason2) {
          Throwable reason = reason2;
          if (logger.level <= Logger.CONFIG) {
            logger.logException("acceptJoin.sendFailed("+msg+"):"+jr, reason);
            while (reason.getCause() != null) {
              reason = reason.getCause();
              logger.logException("because", reason);
            }
          }
        }
      }, getOptions(jr, options));
    } else {
      if (logger.level <= Logger.INFO) logger.log(
          "NOTE: Dropping incoming JoinRequest " + jr
              + " because local node is not ready!");
    }
  }
  
  protected Map<String, Object> getOptions(JoinRequest jr, Map<String, Object> existing) {
    return existing;
  }
  /** called on the joiner */
  protected void completeJoin(JoinRequest jr) {
    NodeHandle jh = jr.getJoinHandle(); // the node we joined to.

    if (jh.getId().equals(localHandle.getId()) && !jh.equals(localHandle)) {
      if (logger.level <= Logger.WARNING) logger.log(
          "NodeId collision, unable to join: " + localHandle + ":" + jh);
    } else if (jh.isAlive() == true) { // the join handle is alive
      routeTable.put(jh);
      // add the num. closest node to the routing table

      // now update the local leaf set
      BroadcastLeafSet bls = new BroadcastLeafSet(jh, jr.getLeafSet(),
          BroadcastLeafSet.JoinInitial, 0);
      thePastryNode.receiveMessage(bls);

      // update local RT, then broadcast rows to our peers
      broadcastRows(jr);

      // we have now successfully joined the ring, set the local node ready
      setReady();
    }
  }
  
  /**
   * Can be overloaded to do additional things before going ready. For example,
   * verifying that other nodes are aware of us, so that consistent routing is
   * guaranteed.
   */
  protected void setReady() {
    if (joinEvent != null) joinEvent.cancel();
    joinEvent = null;

    thePastryNode.setReady();
  }

  /**
   * Broadcasts the route table rows.
   * 
   * @param jr the join row.
   */

  public void broadcastRows(JoinRequest jr) {
    // NodeId localId = localHandle.getNodeId();
    int n = jr.numRows();

    // send the rows to the RouteSetProtocol on the local node
    for (int i = jr.lastRow(); i < n; i++) {
      RouteSet row[] = jr.getRow(i);

      if (row != null) {
        BroadcastRouteRow brr = new BroadcastRouteRow(localHandle, row);

        thePastryNode.receiveMessage(brr);
      }
    }

    // now broadcast the rows to our peers in each row

    for (int i = jr.lastRow(); i < n; i++) {
      RouteSet row[] = jr.getRow(i);

      BroadcastRouteRow brr = new BroadcastRouteRow(localHandle, row);

      for (int j = 0; j < row.length; j++) {
        RouteSet rs = row[j];
        if (rs == null)
          continue;

        // send to closest nodes only

        NodeHandle nh = rs.closestNode();
        if (nh != null)
          thePastryNode.send(nh,brr, null, options);

        /*
         * int m = rs.size(); for (int k=0; k<m; k++) { NodeHandle nh =
         * rs.get(k);
         * 
         * nh.receiveMessage(brr); }
         */
      }
    }
  }

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
}
