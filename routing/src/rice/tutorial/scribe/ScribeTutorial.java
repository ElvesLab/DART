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
package rice.tutorial.scribe;

import java.io.IOException;
import java.net.*;
import java.util.*;

import rice.environment.Environment;
import rice.p2p.commonapi.NodeHandle;
import rice.pastry.*;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.pastry.transport.TransportPastryNodeFactory;

/**
 * This tutorial shows how to use Scribe.
 * 
 * @author Jeff Hoye
 */
public class ScribeTutorial {

  /**
   * this will keep track of our Scribe applications
   */
  Vector<MyScribeClient> apps = new Vector<MyScribeClient>();

  /**
   * Based on the rice.tutorial.lesson4.DistTutorial
   * 
   * This constructor launches numNodes PastryNodes. They will bootstrap to an
   * existing ring if one exists at the specified location, otherwise it will
   * start a new ring.
   * 
   * @param bindport the local port to bind to
   * @param bootaddress the IP:port of the node to boot from
   * @param numNodes the number of nodes to create in this JVM
   * @param env the Environment
   */
  public ScribeTutorial(int bindport, InetSocketAddress bootaddress,
      int numNodes, Environment env) throws Exception {
    
    // Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);

    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env);

    // loop to construct the nodes/apps
    for (int curNode = 0; curNode < numNodes; curNode++) {
      // construct a new node
      PastryNode node = factory.newNode();
      
      // construct a new scribe application
      MyScribeClient app = new MyScribeClient(node);
      apps.add(app);
      
      node.boot(bootaddress);
      
      // the node may require sending several messages to fully boot into the ring
      synchronized(node) {
        while(!node.isReady() && !node.joinFailed()) {
          // delay so we don't busy-wait
          node.wait(500);
          
          // abort if can't join
          if (node.joinFailed()) {
            throw new IOException("Could not join the FreePastry ring.  Reason:"+node.joinFailedReason()); 
          }
        }       
      }
      
      System.out.println("Finished creating new node: " + node);
    }

    // for the first app subscribe then start the publishtask
    Iterator<MyScribeClient> i = apps.iterator();
    MyScribeClient app = (MyScribeClient) i.next();
    app.subscribe();
    app.startPublishTask();
    // for all the rest just subscribe
    while (i.hasNext()) {
      app = (MyScribeClient) i.next();
      app.subscribe();
    }

    // now, print the tree
    env.getTimeSource().sleep(5000);
    printTree(apps);
  }

  /**
   * Note that this function only works because we have global knowledge. Doing
   * this in an actual distributed environment will take some more work.
   * 
   * @param apps Vector of the applicatoins.
   */
  public static void printTree(Vector<MyScribeClient> apps) {
    // build a hashtable of the apps, keyed by nodehandle
    Hashtable<NodeHandle, MyScribeClient> appTable = new Hashtable<NodeHandle, MyScribeClient>();
    Iterator<MyScribeClient> i = apps.iterator();
    while (i.hasNext()) {
      MyScribeClient app = (MyScribeClient) i.next();
      appTable.put(app.endpoint.getLocalNodeHandle(), app);
    }
    NodeHandle seed = ((MyScribeClient) apps.get(0)).endpoint
        .getLocalNodeHandle();

    // get the root
    NodeHandle root = getRoot(seed, appTable);

    // print the tree from the root down
    recursivelyPrintChildren(root, 0, appTable);
  }

  /**
   * Recursively crawl up the tree to find the root.
   */
  public static NodeHandle getRoot(NodeHandle seed, Hashtable<NodeHandle, MyScribeClient> appTable) {
    MyScribeClient app = (MyScribeClient) appTable.get(seed);
    if (app.isRoot())
      return seed;
    NodeHandle nextSeed = app.getParent();
    return getRoot(nextSeed, appTable);
  }

  /**
   * Print's self, then children.
   */
  public static void recursivelyPrintChildren(NodeHandle curNode,
      int recursionDepth, Hashtable<NodeHandle, MyScribeClient> appTable) {
    // print self at appropriate tab level
    String s = "";
    for (int numTabs = 0; numTabs < recursionDepth; numTabs++) {
      s += "  ";
    }
    s += curNode.getId().toString();
    System.out.println(s);

    // recursively print all children
    MyScribeClient app = (MyScribeClient) appTable.get(curNode);
    NodeHandle[] children = app.getChildren();
    for (int curChild = 0; curChild < children.length; curChild++) {
      recursivelyPrintChildren(children[curChild], recursionDepth + 1, appTable);
    }
  }

  /**
   * Usage: java [-cp FreePastry- <version>.jar]
   * rice.tutorial.lesson6.ScribeTutorial localbindport bootIP bootPort numNodes
   * example java rice.tutorial.DistTutorial 9001 pokey.cs.almamater.edu 9001
   */
  public static void main(String[] args) throws Exception {
    // Loads pastry configurations
    Environment env = new Environment();

    // disable the UPnP setting (in case you are testing this on a NATted LAN)
    env.getParameters().setString("nat_search_policy","never");
    
    try {
      // the port to use locally
      int bindport = Integer.parseInt(args[0]);

      // build the bootaddress from the command line args
      InetAddress bootaddr = InetAddress.getByName(args[1]);
      int bootport = Integer.parseInt(args[2]);
      InetSocketAddress bootaddress = new InetSocketAddress(bootaddr, bootport);

      // the port to use locally
      int numNodes = Integer.parseInt(args[3]);

      // launch our node!
      ScribeTutorial dt = new ScribeTutorial(bindport, bootaddress, numNodes,
          env);
    } catch (Exception e) {
      // remind user how to use
      System.out.println("Usage:");
      System.out
          .println("java [-cp FreePastry-<version>.jar] rice.tutorial.scribe.ScribeTutorial localbindport bootIP bootPort numNodes");
      System.out
          .println("example java rice.tutorial.scribe.ScribeTutorial 9001 pokey.cs.almamater.edu 9001 10");
      throw e;
    }
  }
}