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
import rice.environment.params.simple.SimpleParameters;
import rice.environment.time.simulated.DirectTimeSource;
import rice.pastry.*;
import rice.pastry.direct.*;
import rice.pastry.standard.*;
import rice.pastry.join.*;

import java.io.IOException;
import java.util.*;

/**
 * Pastry test.
 * 
 * a simple test for pastry.
 * 
 * @version $Id: PastryTest.java 4654 2009-01-08 16:33:07Z jeffh $
 * 
 * @author andrew ladd
 * @author sitaram iyer
 */
@SuppressWarnings("unchecked")
public class PastryTest {
  private DirectPastryNodeFactory factory;

  private NetworkSimulator simulator;

  private Vector pastryNodes;

  private Vector pingClients;

  private Environment environment;

  public PastryTest(Environment env) {
    environment = env;
    simulator = new EuclideanNetwork(env);
    factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(env), simulator,
        env);

    pastryNodes = new Vector();
    pingClients = new Vector();
  }

  private NodeHandle getBootstrap() {
    NodeHandle bootstrap = null;
    try {
      PastryNode lastnode = (PastryNode) pastryNodes.lastElement();
      bootstrap = lastnode.getLocalHandle();
    } catch (NoSuchElementException e) {
    }
    return bootstrap;
  }

  public void makePastryNode() {
    PastryNode pn = factory.newNode(getBootstrap());
    pastryNodes.addElement(pn);

    PingClient pc = new PingClient(pn);
    pingClients.addElement(pc);
    
    synchronized (pn) {
      while(!pn.isReady()) {
        try {
          pn.wait();
        } catch (InterruptedException ie) {}
      }
    }
    
  }

  public void sendPings(int k) {
    int n = pingClients.size();

    for (int i = 0; i < k; i++) {
      int from = environment.getRandomSource().nextInt(n);
      int to = environment.getRandomSource().nextInt(n);

      PingClient pc = (PingClient) pingClients.get(from);
      PastryNode pn = (PastryNode) pastryNodes.get(to);

      pc.sendTrace(pn.getNodeId());

      while (simulate())
        ;

      System.out.println("-------------------");
    }
  }

  public boolean simulate() {
    try { Thread.sleep(300); } catch (InterruptedException ie) {}    
    return false;
//    return simulator.simulate();
  }

  public static void main(String args[]) throws IOException {
    PastryTest pt = new PastryTest(Environment.directEnvironment());

    int n = 4000;
    int m = 100;
    int k = 10;

    int msgCount = 0;

    Date old = new Date();

    for (int i = 0; i < n; i++) {
      pt.makePastryNode();
      if(i%100 == 0)
        System.out.println("Created node "+i+"/"+n);
//      while (pt.simulate())
        msgCount++;

      if ((i + 1) % m == 0) {
        Date now = new Date();
        System.out.println((i + 1) + " " + (now.getTime() - old.getTime())
            + " " + msgCount);
        msgCount = 0;
        old = now;
      }
    }

    System.out.println(n + " nodes constructed");

    pt.sendPings(k);
  }
}
