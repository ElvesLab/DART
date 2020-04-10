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
package rice.tutorial.deterministicsimulator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.Observable;
import java.util.Observer;
import java.util.Vector;

import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.JoinFailedException;
import rice.pastry.NodeHandle;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.direct.*;
import rice.pastry.leafset.LeafSet;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.selector.TimerTask;

/**
 * This tutorial shows how to setup a FreePastry node using the Socket Protocol.
 * 
 * @author Jeff Hoye
 */
public class DirectTutorial {

  // this will keep track of our applications
  Vector apps = new Vector();
  
  /**
   * This constructor launches numNodes PastryNodes.  They will bootstrap 
   * to an existing ring if one exists at the specified location, otherwise
   * it will start a new ring.
   * 
   * @param bindport the local port to bind to 
   * @param bootaddress the IP:port of the node to boot from
   * @param numNodes the number of nodes to create in this JVM
   * @param env the environment for these nodes
   */
  public DirectTutorial(int numNodes, Environment env) throws Exception {
    
    // Generate the NodeIds Randomly
    final NodeIdFactory nidFactory = new RandomNodeIdFactory(env);
    
    // create a new network
    NetworkSimulator<DirectNodeHandle,RawMessage> simulator = new EuclideanNetwork<DirectNodeHandle,RawMessage>(env);

    // stop the simulator to schedule events
    simulator.stop();
    
    // construct the PastryNodeFactory, this is how we use rice.pastry.direct, with a Euclidean Network
    PastryNodeFactory factory = new DirectPastryNodeFactory(nidFactory, simulator, env);

    scheduleBootTask(numNodes, nidFactory, factory, env);
        
    simulator.start();
  }

  public void scheduleBootTask(final int numNodes, final NodeIdFactory nidFactory, final PastryNodeFactory factory, final Environment env) {
    env.getSelectorManager().getTimer().schedule(new TimerTask() {    
      // create the handle to boot off of
      NodeHandle bootHandle = null;

      // counter to construct nodes/apps
      int curNode = 0;
      
      @Override
      public void run() {
        try {
          // create the node (note, this does not boot the node)
          final PastryNode node = factory.newNode();
          System.out.println("Creating new node "+node+" at "+env.getTimeSource().currentTimeMillis());

          // create the application
          MyApp app = new MyApp(node);        
          apps.add(app);                  
          
          // increment the counter
          curNode++;

          // store curNode to start the next task
          final int myCurNode = curNode;
          
          // track when the node is finished booting
          node.addObserver(new Observer() {          
            public void update(Observable o, Object arg) {
              try {
                if (arg instanceof Boolean) {
                  System.out.println("Finished creating new node "+node+" at "+env.getTimeSource().currentTimeMillis());
                  
                  // see if we're done booting nodes
                  if (myCurNode == numNodes) {
                    // start the delivery task
                    scheduleDeliveryTask(nidFactory, env);
                  }
                } else if (arg instanceof JoinFailedException) {
                  JoinFailedException jfe = (JoinFailedException)arg;
                  jfe.printStackTrace();
                  throw new RuntimeException(jfe); // kill selector
                }
              } finally {
                // stop observing after initial try
                o.deleteObserver(this);
              }
            }          
          });
          
          // boot the node
          node.boot(bootHandle);
          
          // store the bootHandle
          bootHandle = node.getLocalHandle();
          
          // stop the task when created numNodes
          if (curNode >= numNodes) cancel();
          
        } catch (IOException ioe) {
          // kill the selector
          throw new RuntimeException(ioe);
        }
      }    
    }, 0, 1000);    
  }
  
  public void scheduleDeliveryTask(final NodeIdFactory nidFactory, Environment env) {
    // wait 10 seconds, then repeat every second
    env.getSelectorManager().getTimer().schedule(new TimerTask() {    
      // counter to construct nodes/apps
      int i = 0;
      
      @Override
      public void run() {
        // for each app
        Iterator<MyApp> appIterator = apps.iterator();
        while(appIterator.hasNext()) {
          MyApp app = (MyApp)appIterator.next();
          
          // pick a key at random
          Id randId = nidFactory.generateNodeId();
          
          // send to that key
          app.routeMyMsg(randId);
        }
        i++;
        if (i >= 10) cancel();
      }
    }, 10000, 1000);
    
    // wait 20 seconds, then repeat every second
    env.getSelectorManager().getTimer().schedule(new TimerTask() {    
      // counter to construct nodes/apps
      Iterator<MyApp> appIterator = apps.iterator();
      
      @Override
      public void run() {
        // for each app
        // for each app
        if (appIterator.hasNext()) {
          MyApp app = (MyApp)appIterator.next();
          PastryNode node = (PastryNode)app.getNode();
          
          // send directly to my leafset
          LeafSet leafSet = node.getLeafSet();
          
          // this is a typical loop to cover your leafset.  Note that if the leafset
          // overlaps, then duplicate nodes will be sent to twice
          for (int i=-leafSet.ccwSize(); i<=leafSet.cwSize(); i++) {
            if (i != 0) { // don't send to self
              // select the item
              NodeHandle nh = leafSet.get(i);
              
              // send the message directly to the node
              app.routeMyMsgDirect(nh);   
            }
          }              
        } else {
          cancel();
        }
      }
    }, 20000, 1000);    
  }
  
  /**
   * Usage: 
   * java [-cp FreePastry-<version>.jar] rice.tutorial.direct.DirectTutorial numNodes randSeed
   * example java rice.tutorial.direct.DirectTutorial 100 5
   */
  public static void main(String[] args) throws Exception {
    // Loads pastry settings, and sets up the Environment for simulation
    Environment env; 
    if (args.length > 1) {
      int randSeed = Integer.parseInt(args[1]);
      env = Environment.directEnvironment(randSeed);
    } else {
      env = Environment.directEnvironment();      
    }
    
    try {
      // the number of nodes to use
      int numNodes = Integer.parseInt(args[0]);    
      
      // launch our node!
      DirectTutorial dt = new DirectTutorial(numNodes, env);
    } catch (Exception e) {
      // remind user how to use
      System.out.println("Usage:"); 
      System.out.println("java [-cp FreePastry-<version>.jar] rice.tutorial.direct.DirectTutorial numNodes");
      System.out.println("example java rice.tutorial.direct.DirectTutorial 100");
      throw e; 
    }
  }
}
