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

import rice.environment.Environment;
import rice.pastry.*;
import rice.pastry.direct.*;
import rice.pastry.standard.*;

import java.io.IOException;
import java.util.*;

/**
 * PastryRegrTest
 * 
 * a regression test suite for pastry.
 * 
 * @version $Id: DirectPastryRegrTest.java 3801 2007-07-18 15:04:40Z jeffh $
 * 
 * @author andrew ladd
 * @author peter druschel
 * @author sitaram iyer
 */

public class DirectPastryRegrTest extends PastryRegrTest {
  private NetworkSimulator simulator;

  /**
   * constructor
   */
  private DirectPastryRegrTest() throws IOException {
    super(Environment.directEnvironment());
    simulator = new SphereNetwork(environment);
    factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(environment), 
        simulator,
        environment);
  }

  /**
   * Get pastryNodes.last() to bootstrap with, or return null.
   */
  protected NodeHandle getBootstrap(boolean firstNode) {
    NodeHandle bootstrap = null;
    try {
      PastryNode lastnode = (PastryNode) pastryNodes.lastElement();
      bootstrap = lastnode.getLocalHandle();
    } catch (NoSuchElementException e) {
    }
    return bootstrap;
  }

  /**
   * wire protocol specific handling of the application object e.g., RMI may
   * launch a new thread
   * 
   * @param pn pastry node
   * @param app newly created application
   */
  protected void registerapp(PastryNode pn, RegrTestApp app) {
  }

  /**
   * send one simulated message
   */
  protected boolean simulate() {
    try { Thread.sleep(300); } catch (InterruptedException ie) {}
    return false;
//    boolean res = simulator.simulate();
//    if (res)
//      msgCount++;
//    return res;
  }

  // do nothing in the simulated world
  public void pause(int ms) {
  }

  /**
   * get authoritative information about liveness of node.
   */
  protected boolean isReallyAlive(NodeHandle nh) {
    return simulator.isAlive((DirectNodeHandle)nh);
  }

  /**
   * murder the node. comprehensively.
   */
  protected void killNode(PastryNode pn) {
    pn.destroy();
//    NetworkSimulator enet = (NetworkSimulator) simulator;
//    enet.setAlive(pn.getNodeId(), false);
  }

  protected void checkRoutingTable(final RegrTestApp rta) {
    PastryNode temp = DirectPastryNode.setCurrentNode(rta.getPastryNode());
    try {
      super.checkRoutingTable(rta);
    } finally {
      DirectPastryNode.setCurrentNode(temp);
    }
  }  
  /**
   * main. just create the object and call PastryNode's main.
   */
  public static void main(String args[]) throws IOException {
    DirectPastryRegrTest pt = new DirectPastryRegrTest();
    mainfunc(pt, args, 500 /* n */, 100/* d */, 10/* k */, 100/* m */, 1/* conc */);
  }
}

