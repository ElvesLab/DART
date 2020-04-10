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
package rice.pastry.direct;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.direct.Delivery;
import org.mpisws.p2p.transport.direct.DirectTransportLayer;
import org.mpisws.p2p.transport.direct.GenericNetworkSimulator;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;

import rice.environment.Environment;
import rice.environment.params.Parameters;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.ScheduledMessage;
import rice.pastry.messaging.Message;

public class NetworkSimulatorImpl<Identifier, MessageType> implements NetworkSimulator<Identifier, MessageType> {
  protected BasicNetworkSimulator<Identifier, MessageType> simulator;
  protected RandomSource random;
  protected ProximityGenerator generator;
  protected LivenessProvider<Identifier> livenessProvider;
  
  // TODO: add listener to top level tl, to notify simulator listeners
  public NetworkSimulatorImpl(Environment env, ProximityGenerator generator) {
    Parameters params = env.getParameters();
    if (params.contains("pastry_direct_use_own_random")
        && params.getBoolean("pastry_direct_use_own_random")) {

      if (params.contains("pastry_direct_random_seed")
          && !params.getString("pastry_direct_random_seed").equalsIgnoreCase(
              "clock")) {
        this.random = new SimpleRandomSource(params
            .getLong("pastry_direct_random_seed"), env.getLogManager(),
            "direct");
      } else {
        this.random = new SimpleRandomSource(env.getLogManager(), "direct");
      }
    } else {
      this.random = env.getRandomSource();
    }
    generator.setRandom(random);
    this.generator = generator;
    simulator = new BasicNetworkSimulator<Identifier, MessageType>(env, random, this);
    livenessProvider = simulator;
  }

  // ****************** passtrhougs to simulator ***************
  public Environment getEnvironment() {
    return simulator.getEnvironment();
  }

  public void setFullSpeed() {
    simulator.setFullSpeed();
  }

  public void setMaxSpeed(float rate) {
    simulator.setMaxSpeed(rate);
  }

  public void start() {
    simulator.start();
  }

  public void stop() {
    simulator.stop();
  }
  
//  /**
//   * find the closest NodeId to an input NodeId out of all NodeIds in the
//   * network
//   * 
//   * @param nid the input NodeId
//   * @return the NodeId closest to the input NodeId in the network
//   */
//  public DirectNodeHandle getClosest(DirectNodeHandle nh) {
//    Iterator<DirectNodeHandle> it = simulator.nodes.keySet().iterator();
//    DirectNodeHandle bestHandle = null;
//    float bestProx = Float.MAX_VALUE;
//    Id theId;
//
//    while (it.hasNext()) {
//      DirectPastryNode theNode = (DirectPastryNode) it.next();
//      float theProx = theNode.record.proximity(nh.getRemote().record);
//      theId = theNode.getNodeId();
//      if (!theNode.isAlive() || !theNode.isReady()
//          || theId.equals(nh.getNodeId())) {
//        continue;
//      }
//
//      if (theProx < bestProx) {
//        bestProx = theProx;
//        bestHandle = (DirectNodeHandle) theNode.getLocalHandle();
//      }
//    }
//    return bestHandle;
//  }
//
//
  // ************************* What is this? ********************** 
  private TestRecord testRecord;
  /**
   * get TestRecord
   * 
   * @return the returned TestRecord
   */
  public TestRecord getTestRecord() {
    return testRecord;
  }

  /**
   * set TestRecord
   * 
   * @param tr input TestRecord
   */
  public void setTestRecord(TestRecord tr) {
    testRecord = tr;
  }

  
  /************** SimulatorListeners handling *******************/
  List<GenericSimulatorListener<Identifier, MessageType>> listeners = new ArrayList<GenericSimulatorListener<Identifier, MessageType>>();  
  public boolean addSimulatorListener(GenericSimulatorListener<Identifier, MessageType> sl) {
    synchronized(listeners) {
      if (listeners.contains(sl)) return false;
      listeners.add(sl);
      return true;
    }
  }

  public boolean removeSimulatorListener(GenericSimulatorListener<Identifier, MessageType> sl) {
    synchronized(listeners) {
      return listeners.remove(sl);
    }
  }

  public void notifySimulatorListenersSent(MessageType m, Identifier from, Identifier to, int delay) {
    List<GenericSimulatorListener<Identifier, MessageType>> temp;
    
    // so we aren't holding a lock while iterating/calling
    synchronized(listeners) {
       temp = new ArrayList<GenericSimulatorListener<Identifier, MessageType>>(listeners);
    }
  
    for(GenericSimulatorListener<Identifier, MessageType> listener : temp) {
      listener.messageSent(m, from, to, delay);
    }
  }

  public void notifySimulatorListenersReceived(MessageType m, Identifier from, Identifier to) {
    List<GenericSimulatorListener<Identifier, MessageType>> temp;
    
    // so we aren't holding a lock while iterating/calling
    synchronized(listeners) {
       temp = new ArrayList<GenericSimulatorListener<Identifier, MessageType>>(listeners);
    }
  
    for(GenericSimulatorListener<Identifier, MessageType> listener : temp) {
      listener.messageReceived(m, from, to);
    }
  }

//  public ScheduledMessage deliverMessage(Message msg, PastryNode node, DirectNodeHandle from, int delay) {
//    node.deliverMess
//    return new ScheduledMessage(node, msg, simulator.deliverMessage(msg, (DirectNodeHandle)node.getLocalHandle(), from, delay));
//  }
//
//  public ScheduledMessage deliverMessage(Message msg, DirectPastryNode node, DirectNodeHandle from, int delay, int period) {
//    return new ScheduledMessage(node, msg, simulator.deliverMessage(msg, (DirectNodeHandle)node.getLocalHandle(), from, delay, period));
//  }
//
//  public ScheduledMessage deliverMessage(Message msg, DirectPastryNode node) {
//    return new ScheduledMessage(node, msg, simulator.deliverMessage(msg, (DirectNodeHandle)node.getLocalHandle()));
//  }
//
//  public ScheduledMessage deliverMessageFixedRate(Message msg, DirectPastryNode node, DirectNodeHandle from, int delay, int period) {
//    return new ScheduledMessage(node, msg, simulator.deliverMessageFixedRate(msg, (DirectNodeHandle)node.getLocalHandle(), from, delay, period));    
//  }

  public void destroy(DirectPastryNode dpn) {
    // TODO Auto-generated method stub
    
  }

  public CancellableTask enqueueDelivery(Delivery del, int delay) {
    // TODO Auto-generated method stub
    return null;
  }

  public NodeRecord generateNodeRecord() {
    return generator.generateNodeRecord();
  }

  public DirectNodeHandle getClosest(DirectNodeHandle nh) {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean isAlive(Identifier nh) {
    return simulator.isAlive(nh);
  }

  public float networkDelay(Identifier a, Identifier b) {
    return simulator.networkDelay(a, b);
  }

  public float proximity(Identifier a, Identifier b) {
    return simulator.proximity(a, b);
  }

  public void removeNode(PastryNode node) {
    // TODO Auto-generated method stub
    
  }

  public NodeRecord getNodeRecord(DirectNodeHandle handle) {
    return simulator.getNodeRecord(handle);
  }

  public LivenessProvider<Identifier> getLivenessProvider() {
    return livenessProvider;
  }

  public GenericNetworkSimulator<Identifier, MessageType> getGenericSimulator() {
    return simulator;
  }

//  public void registerNode(PastryNode dpn, NodeRecord nr) {
//  simulator.registerIdentifier(dpn.getLocalHandle(), dpn.getTL(), nr);
  public void registerNode(Identifier i, DirectTransportLayer<Identifier, MessageType> dtl, NodeRecord nr) {
    simulator.registerIdentifier(i, dtl, nr);
  }
}
