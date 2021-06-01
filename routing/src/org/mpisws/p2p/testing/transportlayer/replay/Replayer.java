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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
import org.mpisws.p2p.transport.peerreview.replay.EventCallback;
import org.mpisws.p2p.transport.peerreview.replay.playback.ReplayLayer;
import org.mpisws.p2p.transport.peerreview.replay.playback.ReplaySM;
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
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeIdFactory;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.selector.SelectorManager;

public class Replayer implements MyEvents, EventCallback {
  InetSocketAddress bootaddress;
  PastryNode node;
  MyScribeClient app;
  Logger logger;
  
  public Replayer(final Id id, final InetSocketAddress addr, InetSocketAddress bootaddress, final long startTime, final long randSeed) throws Exception {
    this.bootaddress = bootaddress;
    Environment env = ReplayLayer.generateEnvironment(id.toString(), startTime, randSeed, null);
    
    final Parameters params = env.getParameters();
    
    params.setInt("pastry_socket_scm_max_open_sockets", params.getInt("org.mpisws.p2p.testing.transportlayer.replay_pastry_socket_scm_max_open_sockets"));

    params.setBoolean("pastry_socket_use_own_random",false);
//    env.getParameters().setInt("rice.environment.random_loglevel", Logger.FINER);

    logger = env.getLogManager().getLogger(Replayer.class, null);

//    env.getParameters().setInt("org.mpisws.p2p.transport.peerreview.replay_loglevel", Logger.FINER);
    
    final Logger simLogger = env.getLogManager().getLogger(EventSimulator.class, null);
    
    final List<ReplayLayer<InetSocketAddress>> replayers = new ArrayList<ReplayLayer<InetSocketAddress>>();
    
    
    SocketPastryNodeFactory factory = new SocketPastryNodeFactory(new NodeIdFactory() {    
      public Id generateNodeId() {
        return id;
      }    
    },addr.getPort(),env) {
  
//      @Override
//      protected TransportLayer<MultiInetSocketAddress, ByteBuffer> getPriorityTransportLayer(TransportLayer<MultiInetSocketAddress, ByteBuffer> trans, LivenessProvider<MultiInetSocketAddress> liveness, ProximityProvider<MultiInetSocketAddress> prox, PastryNode pn) {
//        // get rid of the priorityLayer
//        if (params.getBoolean("org.mpisws.p2p.testing.transportlayer.replay.use_priority")) {
//          return super.getPriorityTransportLayer(trans, liveness, prox, pn);
//        } else {
//          return trans;
//        }
//      }

      @Override
      public NodeHandle getLocalHandle(PastryNode pn, NodeHandleFactory nhf) {
        SocketNodeHandle ret = (SocketNodeHandle)super.getLocalHandle(pn, nhf);
        logger.log(ret.toStringFull());
        return ret;
      }
      
      @Override
      protected RandomSource cloneRandomSource(Environment rootEnvironment, Id nodeId, LogManager lman) {
        return rootEnvironment.getRandomSource();    
      }
      
      @Override
      protected TransportLayer<InetSocketAddress, ByteBuffer> getWireTransportLayer(InetSocketAddress innermostAddress, PastryNode pn) throws IOException {
        Serializer<InetSocketAddress> serializer = new InetSocketAddressSerializer();
        
        HashProvider hashProv = new NullHashProvider();
        SecureHistoryFactory shFactory = new SecureHistoryFactoryImpl(hashProv, pn.getEnvironment());
        String logName = "0x"+id.toStringFull().substring(0,6);
        SecureHistory hist = shFactory.open(logName, "r");
        
        ReplayLayer<InetSocketAddress> replay = new ReplayLayer<InetSocketAddress>(serializer,hashProv,hist,addr,pn.getEnvironment());
        replay.registerEvent(Replayer.this, EVT_BOOT, EVT_SUBSCRIBE, EVT_PUBLISH);
        replayers.add(replay);
        return replay;
      }
      
    };
    
    // construct a node, passing the null boothandle on the first loop will
    // cause the node to start its own ring
    node = (PastryNode)factory.newNode();
    app = new MyScribeClient(node);
    
    ReplaySM sim = (ReplaySM)env.getSelectorManager();
    ReplayLayer<InetSocketAddress> replay = replayers.get(0);
    replay.makeProgress(); // get rid of INIT event
    sim.setVerifier(replay);
    
    
    sim.start();

//    // this is an example of th enew way
//    //PastryNode node = factory.newNode(nidFactory.generateNodeId());
//    //node.getBootstrapper().boot(Collections.singleton(bootaddress));
//    
//    // the node may require sending several messages to fully boot into the ring
//    synchronized(node) {
//      while(!node.isReady() && !node.joinFailed()) {
//        // delay so we don't busy-wait
//        node.wait(500);
//        
//        // abort if can't join
//        if (node.joinFailed()) {
//          throw new IOException("Could not join the FreePastry ring.  Reason:"+node.joinFailedReason()); 
//        }
//      }       
//    }
//    
//    System.out.println("Finished creating new node: " + node);
//    
//    // construct a new scribe application
//    MyScribeClient app = new MyScribeClient(node);
//  
//    // for all the rest just subscribe
//    app.subscribe();
  
    // now, print the tree
//    env.getTimeSource().sleep(5000);
    
    try {
      env.getTimeSource().sleep(55000);
    } catch (InterruptedException ie) {
      return;
    }
    env.destroy();    

  }
  
  public static void replayNode(final Id id, final InetSocketAddress addr, InetSocketAddress bootaddress, final long startTime, final long randSeed) throws Exception {
    Environment env = new Environment();
    if (env.getParameters().getBoolean("org.mpisws.p2p.testing.transportlayer.replay.Replayer_printlog")) 
      printLog("0x"+id.toStringFull().substring(0,6), env);

    
//  Environment env = Environment.directEnvironment();
    System.out.println(id.toStringFull()+" "+addr.getAddress().getHostAddress()+" "+addr.getPort()+" "+bootaddress.getPort()+" "+startTime+" "+randSeed);
    env.destroy();
    new Replayer(id, addr, bootaddress, startTime, randSeed);
  }

  public static void printLog(String arg, Environment env) throws IOException {
    BasicEntryDeserializer.printLog(arg, new MyEntryDeserializer(new InetSocketAddressSerializer()), env); 
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
//    System.out.println(args[0]+" "+args[1]+" "+args[2]+" "+args[3]);
    String hex = args[0];
    InetAddress a = InetAddress.getByName(args[1]);
    int startPort = Integer.decode(args[2]).intValue();
    int bootPort = Integer.decode(args[3]).intValue();
    InetSocketAddress addr = new InetSocketAddress(a,startPort);
    InetSocketAddress bootaddress = new InetSocketAddress(a,bootPort);
    long startTime = Long.decode(args[4]).longValue();
    long randSeed = Long.decode(args[5]).longValue();
    
    replayNode(Id.build(hex), addr, bootaddress, startTime, randSeed);

  }

  public void replayEvent(short type, InputBuffer entry) {
    if (logger.level <= Logger.FINE) logger.log("replayEvent("+type+")");
    switch (type) {
    case EVT_BOOT:
      node.getBootstrapper().boot(Collections.singletonList(bootaddress));
      break;
    case EVT_SUBSCRIBE:
      app.subscribe();
      break;
    case EVT_PUBLISH:
      app.startPublishTask();
      break;
    }
  }

}
