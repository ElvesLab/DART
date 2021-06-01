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
package rice.pastry.client;

import java.io.IOException;

import rice.environment.logging.Logger;
import rice.pastry.*;
import rice.pastry.messaging.*;
import rice.pastry.routing.*;

/**
 * CommonAPIAppl is an abstract class that all new applications should extend.
 * It provides the common KBR API defined in
 * 
 * "Towards a Common API for Structured Peer-to-Peer Overlays." Frank Dabek, Ben
 * Zhao, Peter Druschel, John Kubiatowicz and Ion Stoica. In Proceedings of the
 * 2nd International Workshop on Peer-to-peer Systems (IPTPS'03) , Berkeley, CA,
 * February 2003.
 * 
 * @version $Id: CommonAPIAppl.java 3801 2007-07-18 15:04:40Z jeffh $
 * 
 * @author Peter Druschel
 */

public abstract class CommonAPIAppl extends PastryAppl {

  /**
   * Constructor.
   * 
   * @param pn the pastry node that the application attaches to.
   */
  public CommonAPIAppl(PastryNode pn) {
    super(pn);
  }

  /**
   * Constructor.
   * 
   * @param pn the pastry node that the application attaches to.
   */
  public CommonAPIAppl(PastryNode pn, String instance) {
    super(pn, instance);
  }

  // API methods to be invoked by applications

  /**
   * This operation forwards a message towards the root of key. The optional
   * hint argument specifies a node that should be used as a first hop in
   * routing the message. A good hint, e.g. one that refers to the key's current
   * root, can result in the message being delivered in one hop; a bad hint adds
   * at most one extra hop to the route. Either K or hint may be NULL, but not
   * both. The operation provides a best-effort service: the message may be
   * lost, duplicated, corrupted, or delayed indefinitely.
   * 
   * 
   * @param key the key
   * @param msg the message to deliver.
   * @param hint the hint
   */
  public void route(Id key, Message msg, NodeHandle hint) {
    if (logger.level <= Logger.FINER) logger.log( 
        "[" + thePastryNode + "] route " + msg + " to " + key);

    RouteMessage rm = new RouteMessage(key, msg, hint, 
        (byte)thePastryNode.getEnvironment().getParameters().getInt("pastry_protocol_router_routeMsgVersion"));
    rm.setTLOptions(options);
    thePastryNode.getRouter().route(rm);
  }

  /**
   * This method produces a list of nodes that can be used as next hops on a
   * route towards key, such that the resulting route satisfies the overlay
   * protocol's bounds on the number of hops taken. If safe is true, the
   * expected fraction of faulty nodes in the list is guaranteed to be no higher
   * than the fraction of faulty nodes in the overlay; if false, the set may be
   * chosen to optimize performance at the expense of a potentially higher
   * fraction of faulty nodes. This option allows applications to implement
   * routing in overlays with byzantine node failures. Implementations that
   * assume fail-stop behavior may ignore the safe argument. The fraction of
   * faulty nodes in the returned list may be higher if the safe parameter is
   * not true because, for instance, malicious nodes have caused the local node
   * to build a routing table that is biased towards malicious
   * nodes~\cite{Castro02osdi}.
   * 
   * @param key the message's key
   * @param num the maximal number of next hops nodes requested
   * @param safe
   * @return the nodehandle set
   */

  public NodeSet localLookup(Id key, int num, boolean safe) {
    // safe ignored until we have the secure routing support

    // get the nodes from the routing table
    return getRoutingTable().alternateRoutes(key, num);
  }

  /**
   * This method produces an unordered list of nodehandles that are neighbors of
   * the local node in the ID space. Up to num node handles are returned.
   * 
   * @param num the maximal number of nodehandles requested
   * @return the nodehandle set
   */

  public NodeSet neighborSet(int num) {
    return getLeafSet().neighborSet(num);
  }

  /**
   * This method returns an ordered set of nodehandles on which replicas of the
   * object with key can be stored. The call returns nodes with a rank up to and
   * including max_rank. If max_rank exceeds the implementation's maximum
   * replica set size, then its maximum replica set is returned. The returned
   * nodes may be used for replicating data since they are precisely the nodes
   * which become roots for the key when the local node fails.
   * 
   * @param key the key
   * @param max_rank the maximal number of nodehandles returned
   * @return the replica set
   */

  public NodeSet replicaSet(Id key, int max_rank) {
    return getLeafSet().replicaSet(key, max_rank);
  }

  /**
   * This method provides information about ranges of keys for which the node n
   * is currently a r-root. The operations returns null if the range could not
   * be determined. It is an error to query the range of a node not present in
   * the neighbor set as returned by the update upcall or the neighborSet call.
   * 
   * Some implementations may have multiple, disjoint ranges of keys for which a
   * given node is responsible (Pastry has two). The parameter key allows the
   * caller to specify which range should be returned. If the node referenced by
   * n is the r-root for key, then the resulting range includes key. Otherwise,
   * the result is the nearest range clockwise from key for which n is
   * responsible.
   * 
   * @param n nodeHandle of the node whose range is being queried
   * @param r the rank
   * @param key the key
   * @param cumulative if true, returns ranges for which n is an i-root for 0 <i
   *          <=r
   * @return the range of keys, or null if range could not be determined for the
   *         given node and rank
   */

  public IdRange range(NodeHandle n, int r, Id key, boolean cumulative) {

    if (cumulative)
      return getLeafSet().range(n, r);

    IdRange ccw = getLeafSet().range(n, r, false);
    IdRange cw = getLeafSet().range(n, r, true);

    if (cw == null || ccw.contains(key)
        || key.isBetween(cw.getCW(), ccw.getCCW()))
      return ccw;
    else
      return cw;

  }

  /**
   * This method provides information about ranges of keys for which the node n
   * is currently a r-root. The operations returns null if the range could not
   * be determined. It is an error to query the range of a node not present in
   * the neighbor set as returned by the update upcall or the neighborSet call.
   * 
   * Some implementations may have multiple, disjoint ranges of keys for which a
   * given node is responsible (Pastry has two). The parameter key allows the
   * caller to specify which range should be returned. If the node referenced by
   * n is the r-root for key, then the resulting range includes key. Otherwise,
   * the result is the nearest range clockwise from key for which n is
   * responsible.
   * 
   * @param n nodeHandle of the node whose range is being queried
   * @param r the rank
   * @param key the key
   * @return the range of keys, or null if range could not be determined for the
   *         given node and rank
   */

  public IdRange range(NodeHandle n, int r, Id key) {
    return range(n, r, key, false);
  }

  /*
   * upcall methods, to be overridden by the derived application object
   * 
   * Applications process messages by executing code in upcall methods that are
   * invoked by the KBR routing system at nodes along a message's path and at
   * its root. To permit event-driven implementations, upcall handlers must not
   * block and should not perform long-running computations.
   */

  /**
   * Called by pastry when a message arrives for this application.
   * 
   * @param msg the message that is arriving.
   */

  public abstract void deliver(Id key, Message msg);

  /**
   * Called by pastry when a message is enroute and is passing through this
   * node. If this method is not overridden, the default behaviour is to let the
   * message pass through.
   * 
   * @param msg the message that is passing through.
   * @param key the key
   * @param nextHop the default next hop for the message.
   *  
   */

  public void forward(RouteMessage msg) {
    return;
  }

  /**
   * Called by pastry when the neighbor set changes.
   * 
   * @param nh the handle of the node that was added or removed.
   * @param wasAdded true if the node was added, false if the node was removed.
   */

  public void update(NodeHandle nh, boolean joined) {
  }

  /**
   * Invoked when the Pastry node has joined the overlay network and is ready to
   * send and receive messages
   */

  public void notifyReady() {
  }

  /*
   * internal methods
   */

  // hide defunct methods from PastryAppl
  public final void messageForAppl(Message msg) {
  }

  /**
   * Called by pastry when the leaf set changes.
   * 
   * @param nh the handle of the node that was added or removed.
   * @param wasAdded true if the node was added, false if the node was removed.
   */

  public final void leafSetChange(NodeHandle nh, boolean wasAdded) {
    update(nh, wasAdded);
  }

  /**
   * Called by pastry to deliver a message to this client. Not to be overridden.
   * 
   * @param msg the message that is arriving.
   */

  public void receiveMessage(Message msg) {
    if (logger.level <= Logger.FINER) logger.log(
        "[" + thePastryNode + "] recv " + msg);

    if (msg instanceof RouteMessage) {
      RouteMessage rm = (RouteMessage) msg;

      // call application
      forward(rm);

      if (rm.getNextHop() != null) {
        NodeHandle nextHop = rm.getNextHop();

        // if the message is for the local node, deliver it here
        if (getNodeId().equals(nextHop.getNodeId())) {
          try {
            deliver(rm.getTarget(), rm.unwrap(deserializer));
          } catch (IOException ioe) {
            throw new RuntimeException("Error deserializing message "+rm,ioe); 
          }
        } else {
          // route the message
          thePastryNode.getRouter().route(rm);
        }

      }
    } else {
      // if the message is not a RouteMessage, then it is for the local node and
      // was sent with a PastryAppl.routeMsgDirect(); we deliver it for backward
      // compatibility
      deliver(null, msg);
    }
  }

}

