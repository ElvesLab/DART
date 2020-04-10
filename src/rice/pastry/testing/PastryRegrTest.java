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
package rice.pastry.testing;

import rice.Continuation;
import rice.Continuation.*;
import rice.environment.Environment;
import rice.p2p.commonapi.RangeCannotBeDeterminedException;
import rice.pastry.*;
import rice.pastry.direct.*;
import rice.pastry.messaging.*;
import rice.pastry.routing.*;
import rice.pastry.leafset.*;

import java.util.*;

/**
 * PastryRegrTest
 * 
 * a regression test suite for pastry. abstract class.
 * 
 * @version $Id: PastryRegrTest.java 4654 2009-01-08 16:33:07Z jeffh $
 * 
 * @author andrew ladd
 * @author peter druschel
 */

public abstract class PastryRegrTest {
  protected PastryNodeFactory factory;

  public Vector pastryNodes;

  /**
   * of Id
   */
  public SortedMap pastryNodesSorted;
  public SortedMap pastryNodesSortedReady;

  public Vector pastryNodesLastAdded;

  public boolean inConcJoin;

  private Vector rtApps;

  public Message lastMsg;

  public Id.Distance lastDist;

  public Id lastNode;

//  int msgCount = 0;

  // abstract methods

  /**
   * get a node handle to bootstrap from.
   */
  protected abstract NodeHandle getBootstrap(boolean firstNode);

  /**
   * wire protocol specific handling of the application object e.g., RMI may
   * launch a new thread
   * 
   * @param pn pastry node
   * @param app newly created application
   */
  protected abstract void registerapp(PastryNode pn, RegrTestApp app);

  /**
   * send one simulated message, or return false for a real wire protocol.
   */
  protected abstract boolean simulate();

  /**
   * determine whether this node is really alive.
   */
  protected abstract boolean isReallyAlive(NodeHandle nh);

  /**
   * kill a given node.
   */
  protected abstract void killNode(PastryNode pn);

  protected Environment environment;
  
  /**
   * constructor
   */
  protected PastryRegrTest(Environment env) {
    this.environment = env;
    pastryNodes = new Vector();
    pastryNodesSorted = new TreeMap();
    pastryNodesSortedReady = new TreeMap();
    pastryNodesLastAdded = new Vector();
    inConcJoin = false;
    rtApps = new Vector();
  }

  /**
   * make a new pastry node
   */

  private void makePastryNode() {
    NodeHandle bootstrap = getBootstrap(pastryNodes.size() == 0);
    PastryNode pn = generateNode(bootstrap);

    pastryNodes.addElement(pn);
    pastryNodesSorted.put(pn.getNodeId(), pn);
    pastryNodesLastAdded.clear();
    pastryNodesLastAdded.addElement(pn);

    RegrTestApp rta = new RegrTestApp(pn, this);
    rtApps.addElement(rta);

    registerapp(pn, rta);

//    int msgCount = 0;

//    if (bootstrap != null)
//      while (simulate())
//        msgCount++;

    //System.out.println("created " + pn + " messages: " + msgCount);

    checkLeafSet(rta);
    checkRoutingTable(rta);
    //System.out.println("");
  }

  protected PastryNode generateNode(NodeHandle bootstrap) {
    PastryNode pn = factory.newNode(bootstrap);

    return pn;
  }

  /**
   * make a set of num new pastry nodes, concurrently this tests concurrent node
   * joins
   * 
   * @param num the number of nodes in a set
   */

  private void makePastryNode(int num) {
    RegrTestApp rta[] = new RegrTestApp[num];

    pause(1000);
    pastryNodesLastAdded.clear();
    inConcJoin = true;

    int n = pastryNodes.size(); // n will be a multiple of num
    if (n == 0)
      num = 1;
    for (int i = 0; i < num; i++) {

      NodeHandle bootstrap;
      bootstrap = getBootstrap(n == 0);

      PastryNode pn = generateNode(bootstrap);
      pastryNodes.addElement(pn);
      pastryNodesSorted.put(pn.getNodeId(), pn);
      pastryNodesLastAdded.addElement(pn);

      rta[i] = new RegrTestApp(pn, this);
      rtApps.addElement(rta[i]);

      registerapp(pn, rta[i]);

      if (bootstrap != null)
        if (n == 0) {
          // we have to join the first batch of nodes
          // sequentially, else we create multiple rings
//          while (simulate())
//            msgCount++;

          // ADDED FOR WIRE PROTOCOL...
          synchronized(pn) {
            while (!pn.isReady()) {                          
              try {
                pn.wait(500);
              } catch (InterruptedException ie) {}
            }
          }
        }
    }

//    int msgCount = 0;

    // now simulate concurrent joins
//    while (simulate())
//      msgCount++;

    // ADDED FOR WIRE PROTOCOL...
    // wait until all nodes are ready
    for (int i = 0; i < pastryNodesLastAdded.size(); i++) {
      PastryNode pn = (PastryNode) pastryNodesLastAdded.get(i);
      synchronized(pn) {
        while (!pn.isReady()) {                          
          try {
            pn.wait(500);
          } catch (InterruptedException ie) {}
        }
      }
    }
    pause(2500);

    inConcJoin = false;

    for (int i = 0; i < num; i++) {
      System.out.println("created " + rta[i].getNodeId());
      PastryNode pn = rta[i].getPastryNode();
      pastryNodesSortedReady.put(pn.getNodeId(), pn);

      checkLeafSet(rta[i]);
      checkRoutingTable(rta[i]);
    }

//    System.out.println("messages: " + msgCount);

    for (int i = 0; i < rtApps.size(); i++) {
      //checkLeafSet((RegrTestApp)rtApps.get(i));
      //checkRoutingTable((RegrTestApp)rtApps.get(i));
    }

  }

  // wait for a specified amount of time if we're distributed
  public abstract void pause(int ms);

  /**
   * Send messages among random message pairs. In each round, one message is
   * sent from a random source node to a random destination; then, a second
   * message is sent from a random source node with a random key (key is not
   * necessaily the Id of an existing node)
   * 
   * @param k the number of rounds
   */

  public void sendPings(int k) {
    int n = rtApps.size();

    for (int i = 0; i < k; i++) {
      int from = environment.getRandomSource().nextInt(n);
      int to = environment.getRandomSource().nextInt(n);
      byte[] keyBytes = new byte[Id.IdBitLength / 8];
      environment.getRandomSource().nextBytes(keyBytes);
      Id key = Id.build(keyBytes);

      RegrTestApp rta = (RegrTestApp) rtApps.get(from);
      PastryNode pn = (PastryNode) pastryNodes.get(to);

      // send to a random node
      rta.sendTrace(pn.getNodeId());
//      while (simulate());

      // send to a random key
      rta.sendTrace(key);
//      while (simulate());

      //System.out.println("-------------------");
    }
  }

  /**
   * verify the correctness of the leaf set
   */

  private void checkLeafSet(RegrTestApp rta) {
    LeafSet ls = rta.getLeafSet();
    Id localId = rta.getNodeId();

    // check size
    if (ls.size() < ls.maxSize()
        && (pastryNodesSorted.size() - 1) * 2 != ls.size())
      System.out.println("checkLeafSet: incorrect size " + rta.getNodeId()
          + " ls.size()=" + ls.size() + " total nodes="
          + pastryNodesSorted.size() + "\n" + ls);

    // check for correct leafset range
    // ccw half
    for (int i = -ls.ccwSize(); i < 0; i++) {
      NodeHandle nh = ls.get(i);

      if (!nh.isAlive())
        System.out.println("checkLeafSet: dead node handle " + nh.getNodeId()
            + " in leafset at " + rta.getNodeId() + "\n" + ls);

      Id nid = ls.get(i).getNodeId();
      int inBetween;

      if (localId.compareTo(nid) > 0) // local > nid ?
        inBetween = pastryNodesSorted.subMap(nid, localId).size();
      else
        inBetween = pastryNodesSorted.tailMap(nid).size()
            + pastryNodesSorted.headMap(localId).size();

      if (inBetween != -i)
        System.out.println("checkLeafSet: failure at" + rta.getNodeId() + "i="
            + i + " inBetween=" + inBetween + "\n" + ls);
    }

    // cw half
    for (int i = 1; i <= ls.cwSize(); i++) {
      NodeHandle nh = ls.get(i);

      if (!nh.isAlive())
        System.out.println("checkLeafSet: dead node handle " + nh.getNodeId()
            + " in leafset at " + rta.getNodeId() + "\n" + ls);

      Id nid = ls.get(i).getNodeId();
      int inBetween;

      if (localId.compareTo(nid) < 0) // localId < nid?
        inBetween = pastryNodesSorted.subMap(localId, nid).size();
      else
        inBetween = pastryNodesSorted.tailMap(localId).size()
            + pastryNodesSorted.headMap(nid).size();

      if (inBetween != i)
        System.out.println("checkLeafSet: failure at" + rta.getNodeId() + "i="
            + i + " inBetween=" + inBetween + "\n" + ls);
    }

    // now check the replicaSet and range method

    // a comparator that orders nodeIds by distance from the localId
    class DistComp implements Comparator<Id> {
      Id id;

      public DistComp(Id id) {
        this.id = id;
      }

      public int compare(Id o1, Id o2) {
        Id nid1 = (Id) o1;
        Id nid2 = (Id) o2;
        return nid1.distance(id).compareTo(nid2.distance(id));
      }
    }

    for (int k = -ls.ccwSize(); k <= ls.cwSize(); k++) {
      // for each id in the leafset
      Id id = ls.get(k).getNodeId();
      TreeSet distanceSet = new TreeSet(new DistComp(id));

      // compute a representation of the leafset, sorted by distance from the id
      for (int i = -ls.ccwSize(); i <= ls.cwSize(); i++) {
        NodeHandle nh = ls.get(i);
        distanceSet.add(nh.getNodeId());
      }

      int expectedSize = distanceSet.headSet(ls.get(-ls.ccwSize()).getNodeId())
          .size() + 1;
      int expectedSize1 = distanceSet.headSet(ls.get(ls.cwSize()).getNodeId())
          .size() + 1;
      if (expectedSize1 < expectedSize)
        expectedSize = expectedSize1;
      if (ls.overlaps())
        expectedSize = distanceSet.size();

      NodeSet rs = ls.replicaSet(id, expectedSize);
      // now verify the replicaSet
      for (int i = 0; i < rs.size(); i++) {
        NodeHandle nh = rs.get(i);
        Id nid = nh.getNodeId();
        int inBetween = distanceSet.subSet(id, nid).size();

        if (inBetween != i)
          System.out.println("checkLeafSet: replicaSet failure at"
              + rta.getNodeId() + " i=" + i + " k=" + k + " inBetween="
              + inBetween + "\n" + rs + "\n" + ls);
      }

      // check size
      if (rs.size() != expectedSize)
        System.out.println("checkLeafSet: replicaSet size failure at "
            + rta.getNodeId() + " k=" + k + " expectedSize=" + expectedSize
            + " " + "\n" + rs + "\n" + ls + " distanceSet:" + distanceSet);

      // now check the range method
      int maxRank;
      if (ls.overlaps() || ls.size() == 0)
        maxRank = rs.size() - 1;
      else
        maxRank = (k > 0) ? (ls.cwSize() - k - 1) : (ls.ccwSize() + k - 1);

      int nearestPos;
      Id nearest = null;

      for (int j = -1; j <= maxRank * 2 + 1; j++) {
        IdRange range = null;

        if (j < 0)
          // check the cumulative range
          try {
            range = rta.range(ls.get(k), maxRank, nearest, true);
          } catch (IllegalArgumentException iae) {
          } catch (RangeCannotBeDeterminedException rcbde) {
          } catch (NullPointerException npe) {}
        else
          // check the individual rank ranges
          try {
            range = rta.range(ls.get(k), j / 2, nearest, false);
          } catch (NullPointerException npe) {
          } catch (RangeCannotBeDeterminedException rcbde) {}

        //System.out.println("j=" + j + " maxRank=" + maxRank + " " + range);

        //if ( (range != null) != (rs.size() >= 1) ) {
        if (range == null) {
          if (maxRank >= 0) {
            System.out.println("checkLeafSet: range size failure at "
                + rta.getNodeId() + " k=" + k + " maxRank=" + maxRank + "\n"
                + rs + "\n" + ls);
          }
          continue;
        }

        //nearestPos = ls.mostSimilar(range.getCCW());
        //nearest = ls.get(nearestPos).getNodeId();
        nearest = ls.get(k).getNodeId();
        nearestPos = k;

        // fetch the replicaSet of the key at the ccw edge of the range
        NodeSet cs = ls.replicaSet(range.getCCW(), maxRank + 1);

        if ((j >= 0 && cs.get(j / 2) == null)
            || (j >= 0 && !cs.get(j / 2).getNodeId().equals(nearest))
            || (j < 0 && !ls.overlaps() && !cs.get(maxRank).getNodeId().equals(
                nearest)) || (j < 0 && ls.overlaps() && !range.isFull())) {
          System.out.println("checkLeafSet: range failure 1 at "
              + rta.getNodeId() + " k=" + k + " j=" + j + " maxRank=" + maxRank
              + "\n" + cs + "\n" + ls + "\n" + range + "\nnearest=" + nearest);

          System.out.println("dist(nearest)="
              + nearest.distance(range.getCCW()));
          if (ls.get(nearestPos - 1) != null)
            System.out.println("dist(nearest-1)="
                + ls.get(nearestPos - 1).getNodeId().distance(range.getCCW()));
          if (ls.get(nearestPos + 1) != null)
            System.out.println("dist(nearest+1)="
                + ls.get(nearestPos + 1).getNodeId().distance(range.getCCW()));
        }

        //nearestPos = ls.mostSimilar(range.getCW());
        //nearest = ls.get(nearestPos).getNodeId();

        // fetch the replicaSet of the key at the cw edge of the range
        cs = ls.replicaSet(range.getCW().getCCW(), maxRank + 1);

        if ((j >= 0 && cs.get(j / 2) == null)
            || (j >= 0 && !cs.get(j / 2).getNodeId().equals(nearest))
            || (j < 0 && !ls.overlaps() && !cs.get(maxRank).getNodeId().equals(
                nearest)) || (j < 0 && ls.overlaps() && !range.isFull())) {
          System.out.println("checkLeafSet: range failure 2 at "
              + rta.getNodeId() + " k=" + k + " j=" + j + " maxRank=" + maxRank
              + "\n" + cs + "\n" + ls + "\n" + range + "\nnearest=" + nearest);

          System.out
              .println("dist(nearest)=" + nearest.distance(range.getCW()));
          if (ls.get(nearestPos - 1) != null)
            System.out.println("dist(nearest-1)="
                + ls.get(nearestPos - 1).getNodeId().distance(range.getCW()));
          if (ls.get(nearestPos + 1) != null)
            System.out.println("dist(nearest+1)="
                + ls.get(nearestPos + 1).getNodeId().distance(range.getCW()));
        }

        nearest = range.getCW();

      }

    }

  }

  /**
   * verify the correctness of the routing table
   */

  protected void checkRoutingTable(final RegrTestApp rta) {
    RoutingTable rt = rta.getRoutingTable();
    // check routing table

    for (int i = rt.numRows() - 1; i >= 0; i--) {
      // next row
      for (int j = 0; j < rt.numColumns(); j++) {
        // next column

        // skip if local Id digit
        // if (j == rta.getNodeId().getDigit(i,rt.baseBitLength())) continue;

        final RouteSet rs = rt.getRouteSet(i, j);

        Id domainFirst = rta.getNodeId().getDomainPrefix(i, j, 0,
            rt.baseBitLength());
        Id domainLast = rta.getNodeId().getDomainPrefix(i, j, -1,
            rt.baseBitLength());
        //System.out.println("prefixes " + rta.getNodeId() + domainFirst +
        // domainLast
        //     + "compareTo=" + domainFirst.compareTo(domainLast));

        if (rs == null || rs.size() == 0) {
          // no entry

          // check if no nodes with appropriate prefix exist
          int inBetween = pastryNodesSorted.subMap(domainFirst, domainLast)
              .size()
              + (pastryNodesSorted.containsKey(domainLast) ? 1 : 0);

          if (inBetween > 0) {

            System.out.println("checkRoutingTable: missing RT entry at"
                + rta.getNodeId() + "row=" + i + " column=" + j + " inBetween="
                + inBetween);
            //System.out.println("prefixes " + rta.getNodeId() + domainFirst +
            // domainLast);
          }
        } else {
          // check entries
          NodeHandle nh = (NodeHandle)rs.closestNode();          
          
          int bestProximity = Integer.MAX_VALUE;
          if (nh != null) {
            bestProximity = rta.getPastryNode().proximity(nh);
            if (bestProximity == Integer.MAX_VALUE) {
              System.out.println("checkRoutingTable failure 0, row=" + i
                  + " column=" + j);
            }
          }

          for (int k = 0; k < rs.size(); k++) {

            // check for correct proximity ordering
            if (rs.get(k).isAlive() && rta.getPastryNode().proximity(rs.get(k)) < bestProximity) {
              System.out.println("checkRoutingTable failure 1, row=" + i
                  + " column=" + j + " rank=" + k);

            }

            NodeHandle nh2 = rs.get(k);
            Id id = nh2.getNodeId();
            
            // check if node exists
            if (!pastryNodesSorted.containsKey(id)) {
              if (isReallyAlive(nh2))
                System.out.println("checkRoutingTable failure 2, row=" + i
                    + " column=" + j + " rank=" + k);
            } else // check if node has correct prefix
            if (!pastryNodesSorted.subMap(domainFirst, domainLast).containsKey(
                id)
                && !domainLast.equals(id))
              System.out.println("checkRoutingTable failure 3, row=" + i
                  + " column=" + j + " rank=" + k);
          }
        }
      }
    }

    //System.out.println(rt);
  }

  /**
   * initiate leafset maintenance
   */
  private void initiateLeafSetMaintenance() {

    for (int i = 0; i < pastryNodes.size(); i++) {
      PastryNode pn = (PastryNode) pastryNodes.get(i);
      pn.receiveMessage(new InitiateLeafSetMaintenance());
      while (simulate())
        ;
    }

  }

  /**
   * initiate routing table maintenance
   */
  private void initiateRouteSetMaintenance() {

    for (int i = 0; i < pastryNodes.size(); i++) {
      PastryNode pn = (PastryNode) pastryNodes.get(i);
      pn.receiveMessage(new InitiateRouteSetMaintenance());
      while (simulate())
        ;
    }

  }

  /**
   * kill a given number of nodes, randomly chosen
   * 
   * @param num the number of nodes to kill
   */

  private void killNodes(int num) {
    for (int i = 0; i < num; i++) {
      int n = environment.getRandomSource().nextInt(pastryNodes.size());

      PastryNode pn = (PastryNode) pastryNodes.get(n);
      pastryNodes.remove(n);
      rtApps.remove(n);
      pastryNodesSorted.remove(pn.getNodeId());
      pastryNodesSortedReady.remove(pn.getNodeId());
      killNode(pn);
      System.out.println("Killed " + pn.getNodeId());
      try {
        environment.getTimeSource().sleep(2000);
      } catch (InterruptedException ie) {}
    }
  }

  /**
   * main
   */

  protected static void mainfunc(PastryRegrTest pt, String args[], int n,
      int d, int k, int m, int numConcJoins) {

    Date old = new Date();

    while (pt.pastryNodes.size() < n) {
      //for (int i=0; i<n; i += numConcJoins) {
      int remaining = n - pt.pastryNodes.size();
      if (remaining > numConcJoins)
        remaining = numConcJoins;

      pt.makePastryNode(remaining);

      if (pt.pastryNodes.size() % m == 0) {
        //if ((i + numConcJoins) % m == 0) {
        Date now = new Date();
        System.out.println(pt.pastryNodes.size() + " "
            + (now.getTime() - old.getTime()));// + " " + pt.msgCount);
//        pt.msgCount = 0;
        old = now;
      }

      pt.sendPings(k);
    }

    System.out.println(pt.pastryNodes.size() + " nodes constructed");

    System.out.println("starting RT and leafset check");
    // check all routing tables, leaf sets
    for (int j = 0; j < pt.rtApps.size(); j++) {
      pt.checkLeafSet((RegrTestApp) pt.rtApps.get(j));
      pt.checkRoutingTable((RegrTestApp) pt.rtApps.get(j));
    }
    System.out.println("finished RT and leafset check");

    // kill some nodes
    pt.killNodes(d);

    System.out.println(d + " nodes killed");

    // send messages
    pt.sendPings((n - d) * k);
    System.out.println((n - d) * k + " messages sent");

    System.out.println("starting leafset/RT maintenance");

    // initiate maint.
    pt.initiateLeafSetMaintenance();
    pt.initiateRouteSetMaintenance();

    System.out.println("finished leafset/RT maintenance");

    // send messages
    pt.sendPings((n - d) * k);
    System.out.println((n - d) * k + " messages sent");

    // Dist: wait until everyone detects failed nodes
    pt.pause(5000);

    // print all nodeIds, sorted
    //Iterator it = pt.pastryNodesSorted.keySet().iterator();
    //while (it.hasNext())
    //System.out.println(it.next());

    System.out.println("starting RT and leafset check");

    // check all routing tables, leaf sets
    for (int i = 0; i < pt.rtApps.size(); i++) {
      pt.checkLeafSet((RegrTestApp) pt.rtApps.get(i));
      pt.checkRoutingTable((RegrTestApp) pt.rtApps.get(i));
    }

    //pt.sendPings(k);

    for (int i = 0; i < 4; i++) {

      System.out.println("Starting leafset/RT maintenance, round " + (i + 2));

      // initiate maint.
      pt.initiateLeafSetMaintenance();
      pt.initiateRouteSetMaintenance();

      System.out.println("finished leafset/RT maintenance, round " + (i + 2));

      // send messages
      pt.sendPings((n - d) * k);
      System.out.println((n - d) * k + " messages sent");

      System.out.println("starting RT and leafset check, round " + (i + 2));

      // check all routing tables, leaf sets
      for (int j = 0; j < pt.rtApps.size(); j++) {
        pt.checkLeafSet((RegrTestApp) pt.rtApps.get(j));
        pt.checkRoutingTable((RegrTestApp) pt.rtApps.get(j));
      }

      pt.pause(1000);
    }

    pt.pause(5000);
    System.out.println("finished, exiting...");
    System.exit(0);
  }
}
