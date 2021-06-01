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
package org.mpisws.p2p.testing.transportlayer.replay;

import java.io.IOException;
import java.io.PrintStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.direct.EventSimulator;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactoryImpl;
import org.mpisws.p2p.transport.peerreview.history.stub.NullHashProvider;
import org.mpisws.p2p.transport.peerreview.replay.BasicEntryDeserializer;
import org.mpisws.p2p.transport.peerreview.replay.playback.ReplayLayer;
import org.mpisws.p2p.transport.peerreview.replay.record.RecordLayer;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.simpleidentity.InetSocketAddressSerializer;
import org.mpisws.p2p.transport.util.Serializer;

import rice.environment.Environment;
import rice.environment.logging.LogManager;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.processing.Processor;
import rice.environment.processing.sim.SimProcessor;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.*;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.ProximityNeighborSelector;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.pastry.transport.NodeHandleAdapter;
import rice.pastry.transport.TransportPastryNodeFactory;
import rice.selector.SelectorManager;

/**
 * This tutorial shows how to use Scribe.
 * 
 * @author Jeff Hoye
 */
public class Recorder implements MyEvents {

  /**
   * this will keep track of our Scribe applications
   */
  ArrayList<MyScribeClient> apps = new ArrayList<MyScribeClient>();

  final Map<Id, Long> storedRandSeed = new HashMap<Id, Long>();

  final Map<Node, RecordLayer<InetSocketAddress>> recorders = new HashMap<Node, RecordLayer<InetSocketAddress>>();

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
  public Recorder(int bindport, final InetSocketAddress bootaddress,
      int numNodes, Environment env) throws Exception {
    
    final Parameters params = env.getParameters();
    
    // Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);
    
    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    final SocketPastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env) {
      
      @Override
      public rice.pastry.NodeHandle getLocalHandle(PastryNode pn, NodeHandleFactory nhf) {
        SocketNodeHandle ret = (SocketNodeHandle)super.getLocalHandle(pn, nhf);
        if (logger.level <= Logger.FINE) logger.log("getLocalHandle():"+ret.toStringFull());
        return ret;
      }
      
      @Override
      protected RandomSource cloneRandomSource(Environment rootEnvironment, Id nodeId, LogManager lman) {
        long randSeed = rootEnvironment.getRandomSource().nextLong();
        if (logger.level <= Logger.FINE) logger.log("RandSeed for "+nodeId+" "+randSeed);
        
        storedRandSeed.put(nodeId, randSeed); 
        
        return new SimpleRandomSource(randSeed, lman);    
      }

//      @Override
//      protected PriorityTransportLayer<MultiInetSocketAddress> getPriorityTransportLayer(TransportLayer<MultiInetSocketAddress, ByteBuffer> trans, LivenessProvider<MultiInetSocketAddress> liveness, ProximityProvider<MultiInetSocketAddress> prox, PastryNode pn) {
//        // get rid of the priorityLayer
//        if (params.getBoolean("org.mpisws.p2p.testing.transportlayer.replay.use_priority")) {
//          return super.getPriorityTransportLayer(trans, liveness, prox, pn);
//        } else {
//          return trans;
//        }
//      }

      @Override
      protected TransportLayer<InetSocketAddress, ByteBuffer> getWireTransportLayer(InetSocketAddress innermostAddress, PastryNode pn) throws IOException {
        // record here
        
        RecordLayer<InetSocketAddress> ret = new RecordLayer<InetSocketAddress>(
            super.getWireTransportLayer(innermostAddress, pn), "0x"+pn.getNodeId().toStringBare(), 
            new InetSocketAddressSerializer(), pn.getEnvironment());
        recorders.put(pn, ret);
        return ret;
      }
      
      @Override
      @SuppressWarnings("unchecked")
      protected Bootstrapper getBootstrapper(final PastryNode pn, NodeHandleAdapter tl, NodeHandleFactory handleFactory, ProximityNeighborSelector pns) {
        final Bootstrapper internal = super.getBootstrapper(pn, tl, handleFactory, pns);
        Bootstrapper ret = new Bootstrapper() {        
          public void boot(Collection bootaddresses) {
            try {
              recorders.get(pn).logEvent(EVT_BOOT);
            } catch (IOException ioe) {
              pn.getEnvironment().getLogManager().getLogger(Bootstrapper.class, null).logException("Error recording EVT_BOOT",ioe);
            }
            internal.boot(bootaddresses);
          }        
        };
        
        return ret;
      }      
    };
//    PastryNodeFactory factory = new TransportPastryNodeFactory(nidFactory, bindport, env);
    
    // loop to construct the nodes/apps
    for (int curNode = 0; curNode < numNodes; curNode++) {
      
      // construct a node, passing the null boothandle on the first loop will
      // cause the node to start its own ring
      final ArrayList<PastryNode> nodeContainer = new ArrayList<PastryNode>(1); 
      env.getSelectorManager().invoke(new Runnable() {
      
        public void run() {
          PastryNode node = (PastryNode)factory.newNode();
          nodeContainer.add(node);
          // construct a new scribe application
          MyScribeClient app = new MyScribeClient(node);
          apps.add(app);

          node.getBootstrapper().boot(Collections.singleton(bootaddress));
        }      
      });
      
      // this is an example of th enew way
//      PastryNode node = factory.newNode(nidFactory.generateNodeId());
//      node.getBootstrapper().boot(Collections.singleton(bootaddress));
      
      // the node may require sending several messages to fully boot into the ring
      synchronized(nodeContainer) {
        while(nodeContainer.isEmpty()) {
          // delay so we don't busy-wait
          nodeContainer.wait(500);
        }
      }
      
      PastryNode node = nodeContainer.get(0);

      synchronized(node) {
        while (!node.isReady() && !node.joinFailed()) {
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
    env.getSelectorManager().invoke(new SubscribeInvokation(app));
    env.getSelectorManager().invoke(new PublishInvokation(app));
    // for all the rest just subscribe
    while (i.hasNext()) {
      app = (MyScribeClient) i.next();
      env.getSelectorManager().invoke(new SubscribeInvokation(app));
    }

    // now, print the tree
    env.getTimeSource().sleep(5000);
    printTree(apps);

    env.getTimeSource().sleep(15000);

    env.destroy();
    
    System.out.println("done recording");
    Thread.sleep(1000);

    int playbackCtr = 0;
    Iterator<MyScribeClient> mscI = apps.iterator();
    while(playbackCtr < params.getInt("org.mpisws.p2p.testing.transportlayer.replay.num_playbacks") && mscI.hasNext()) {
      app = mscI.next();
      Endpoint endpoint = app.endpoint;
      
      System.out.println("playing back "+app.node);

      
      if (params.getBoolean("org.mpisws.p2p.testing.transportlayer.replay.Recorder_printlog")) 
        printLog("0x"+endpoint.getId().toStringFull().substring(0,6), new Environment());
      
      SocketNodeHandle snh = (SocketNodeHandle)endpoint.getLocalNodeHandle();
      try {
        Replayer.replayNode((rice.pastry.Id)snh.getId(), snh.getInetSocketAddress(), bootaddress, snh.getEpoch(), storedRandSeed.get(snh.getId()));
      } catch (InterruptedException ie) {
       
      }
      System.out.println("finished playing "+app.node);
      playbackCtr++;
    }
  }
  
  public abstract class AppInvokation implements Runnable {
    MyScribeClient app;
    public AppInvokation(MyScribeClient app) {
      this.app = app;
    }

    public abstract void doIt() throws IOException;
    
    public void run() {
      try {
        doIt();
      } catch (IOException ioe) {
        app.node.getEnvironment().getLogManager().getLogger(Bootstrapper.class, null).logException("Error recording event",ioe);
      }      
    }        
  }
  
  public class SubscribeInvokation extends AppInvokation {
    public SubscribeInvokation(MyScribeClient app) {
      super(app);
    }

    @Override
    public void doIt() throws IOException {
      recorders.get(app.node).logEvent(EVT_SUBSCRIBE);
      app.subscribe();
    }    
  }

  public class PublishInvokation extends AppInvokation {
    public PublishInvokation(MyScribeClient app) {
      super(app);
    }

    @Override
    public void doIt() throws IOException {
      recorders.get(app.node).logEvent(EVT_PUBLISH);
      app.startPublishTask();
    }    
  }

  public void printLog(String arg, Environment env) throws IOException {
    BasicEntryDeserializer.printLog(arg, new MyEntryDeserializer(new InetSocketAddressSerializer()), env); 
  }
    
  /**
   * Note that this function only works because we have global knowledge. Doing
   * this in an actual distributed environment will take some more work.
   * 
   * @param apps Vector of the applicatoins.
   */
  public static void printTree(ArrayList<MyScribeClient> apps) {
    // build a hashtable of the apps, keyed by nodehandle
    Hashtable<NodeHandle,MyScribeClient> appTable = new Hashtable<NodeHandle,MyScribeClient>();
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
  public static NodeHandle getRoot(NodeHandle seed, Hashtable<NodeHandle,MyScribeClient> appTable) {
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
      int recursionDepth, Hashtable<NodeHandle,MyScribeClient> appTable) {
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
    Parameters params = new SimpleParameters(Environment.defaultParamFileArray,null);

    if (params.getBoolean("org.mpisws.p2p.testing.transportlayer.replay.Recorder_logtofile")) {
      System.setOut(new PrintStream("replay.txt"));
      System.setErr(System.out);
    }
    
    // Loads pastry configurations
    Environment env = RecordLayer.generateEnvironment();
    params = env.getParameters();
    
    
    params.setInt("pastry_socket_scm_max_open_sockets", params.getInt("org.mpisws.p2p.testing.transportlayer.replay_pastry_socket_scm_max_open_sockets"));

    params.setBoolean("pastry_socket_use_own_random",false);
//    env.getParameters().setInt("rice.environment.random_loglevel", Logger.FINER);

    // disable the UPnP setting (in case you are testing this on a NATted LAN)
    params.setString("nat_search_policy","never");
    
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
      Recorder dt = new Recorder(bindport, bootaddress, numNodes,
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