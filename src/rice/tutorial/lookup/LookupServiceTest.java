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
package rice.tutorial.lookup;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.NodeHandleSet;
import rice.pastry.*;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * This is an example of using the LookupService, based on the code for tutorial 4.
 * 
 * @author jstewart
 *
 */
public class LookupServiceTest {

  /**
   * Liberally copied from lesson4's DistTutorial.
   * 
   * @param bindport
   *                the local port to bind to
   * @param bootaddress
   *                the IP:port of the node to boot from
   * @param env
   *                the environment for these nodes
   */
  public LookupServiceTest(int bindport, InetSocketAddress bootaddress, int numNodes, Environment env) throws Exception {

    ArrayList<LookupService> lookups = new ArrayList<LookupService>();

    // Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);

    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env);

    // loop to construct the nodes/apps
    for (int curNode = 0; curNode < numNodes; curNode++) {
      // construct a node, passing the null boothandle on the first loop will
      // cause the node to start its own ring
      PastryNode node = factory.newNode();

      // create the lookup service
      LookupService ls = new LookupService(node);

      lookups.add(ls);

      // boot the node
      node.boot(bootaddress);
      
      // the node may require sending several messages to fully boot into the
      // ring
      synchronized (node) {
        while (!node.isReady() && !node.joinFailed()) {
          // delay so we don't busy-wait
          node.wait(500);

          // abort if can't join
          if (node.joinFailed()) {
            throw new IOException("Could not join the FreePastry ring.  Reason:" + node.joinFailedReason());
          }
        }
      }

      System.out.println("Finished creating new node " + node);

    }

    // wait 10 seconds
    env.getTimeSource().sleep(5000);

    for (LookupService ls : lookups) {
      final Id id = nidFactory.generateNodeId();
      System.out.println("Requesting id "+id);
      ls.requestNodeHandles(id, 3, new Continuation<NodeHandleSet,Exception>() {
        public void receiveException(Exception exception) {
          if (exception instanceof LookupService.NodeLookupTimeoutException) {
            System.out.println("Request for "+id+" timed out");
          } else {
            System.out.println("Exception requesting "+id+": "+exception.getMessage());
            exception.printStackTrace();
          }
        }

        public void receiveResult(NodeHandleSet result) {
          System.out.println("ReplicaSet for "+id+": "+result);
        }
      });
    }
  }

  /**
   * Usage: java [-cp FreePastry-<version>.jar]
   * rice.tutorial.lookup.LookupServiceTest localbindport bootIP bootPort
   * numNodes example java rice.tutorial.lookup.LookupServiceTest 9001
   * pokey.cs.almamater.edu 9001 10
   * 
   * @throws Exception
   *                 when something goes wrong
   */
  public static void main(String[] args) throws Exception {
    // Loads pastry settings
    Environment env = new Environment();

    // disable the UPnP setting (in case you are testing this on a NATted LAN)
    env.getParameters().setString("nat_search_policy", "never");

    try {
      // the port to use locally
      int bindport = Integer.parseInt(args[0]);

      // build the bootaddress from the command line args
      InetAddress bootaddr = InetAddress.getByName(args[1]);
      int bootport = Integer.parseInt(args[2]);
      InetSocketAddress bootaddress = new InetSocketAddress(bootaddr, bootport);

      // the number of nodes to use
      int numNodes = Integer.parseInt(args[3]);

      // launch our node!
      LookupServiceTest lt = new LookupServiceTest(bindport, bootaddress, numNodes, env);
    } catch (Exception e) {
      // remind user how to use
      System.out.println("Usage:");
      System.out
          .println("java [-cp FreePastry-<version>.jar] rice.tutorial.lookup.LookupServiceTest localbindport bootIP bootPort numNodes");
      System.out.println("example java rice.tutorial.lookup.LookupServiceTest 9001 pokey.cs.almamater.edu 9001 10");
      throw e;
    }
  }
}
