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
import rice.pastry.join.*;
import rice.pastry.routing.*;

import java.util.*;
import java.io.*;
import java.lang.*;

/**
 * SinglePingTest
 * 
 * A performance test suite for pastry.
 * 
 * @version $Id: SinglePingTest.java 4654 2009-01-08 16:33:07Z jeffh $
 * 
 * @author Rongmei Zhang
 */

@SuppressWarnings("unchecked")
public class SinglePingTest {
  private DirectPastryNodeFactory factory;

  private NetworkSimulator simulator;

  private TestRecord testRecord;

  private Vector pastryNodes;

  private Vector pingClients;

  private Environment environment;

  public SinglePingTest(TestRecord tr, Environment env) {
    environment = env;
    simulator = new EuclideanNetwork(env); //SphereNetwork();
    factory = new DirectPastryNodeFactory(new RandomNodeIdFactory(environment), simulator, env);
    simulator.setTestRecord(tr);
    testRecord = tr;

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

  public PastryNode makePastryNode() {
    PastryNode pn = factory.newNode(getBootstrap());
    pastryNodes.addElement(pn);

    Ping pc = new Ping(pn);
    pingClients.addElement(pc);

    long start = System.currentTimeMillis();
    synchronized (pn) {
      while(!pn.isReady()) {
        try {
          pn.wait(300);
//          if (!pn.isReady()){
//            System.out.println("Still waiting for "+pn+ " to be ready.");
//          }
        } catch (InterruptedException ie) {}
      }
    }    
    long now = System.currentTimeMillis();
    if (now-start > 10000) 
      System.out.println("Took "+(now-start)+" to create node "+pn);
    
    return pn;
  }

  public void sendPings(int k) {    
    int n = pingClients.size();

    for (int i = 0; i < k; i++) {
      int from = environment.getRandomSource().nextInt(n);
      int to = environment.getRandomSource().nextInt(n);

      Ping pc = (Ping) pingClients.get(from);
      PastryNode pn = (PastryNode) pastryNodes.get(to);

      pc.sendPing(pn.getNodeId());
      while (simulate());
    }
  }

  public boolean simulate() {
    return false;
//    return simulator.simulate();
  }

  public void checkRoutingTable() {
    int i;
    Date prev = new Date();

    for (i = 0; i < testRecord.getNodeNumber(); i++) {
      PastryNode pn = makePastryNode();
      while (simulate());
      System.out.println(pn.getLeafSet());

      if (i != 0 && i % 1000 == 0)
        System.out.println(i + " nodes constructed");
    }
    System.out.println(i + " nodes constructed");

    Date curr = new Date();
    long msec = curr.getTime() - prev.getTime();
    System.out.println("time used " + (msec / 60000) + ":"
        + ((msec % 60000) / 1000) + ":" + ((msec % 60000) % 1000));

    //  simulator.checkRoutingTable();
  }

  public void test() {
    int i;
    long prev = environment.getTimeSource().currentTimeMillis();

    System.out.println("-------------------------");
    for (i = 0; i < testRecord.getNodeNumber(); i++) {
      PastryNode pn = makePastryNode();
//      while (simulate());
//      System.out.println(pn.getLeafSet());

      synchronized (pn) {
        while(!pn.isReady()) {
          try {
            pn.wait();
          } catch (InterruptedException ie) {}
        }
      }
      
      if (i != 0 && i % 100 == 0)
        System.out.println(i + " nodes constructed");
    }
    System.out.println(i + " nodes constructed");

    long curr = environment.getTimeSource().currentTimeMillis();
    long msec = curr - prev;
    System.out.println("time used " + (msec / 60000) + ":"
        + ((msec % 60000) / 1000) + ":" + ((msec % 60000) % 1000));
    prev = curr;

    sendPings(testRecord.getTestNumber());
    System.out.println(testRecord.getTestNumber() + " lookups done");

    curr = environment.getTimeSource().currentTimeMillis();
    msec = curr - prev;
    System.out.println("time used " + (msec / 60000) + ":"
        + ((msec % 60000) / 1000) + ":" + ((msec % 60000) % 1000));

    testRecord.doneTest();
  }
}

