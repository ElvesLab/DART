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
package rice.pastry.peerreview;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.WitnessListener;
import org.mpisws.p2p.transport.peerreview.replay.Verifier;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketNodeHandleFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.socket.TransportLayerNodeHandle;
import rice.pastry.transport.NodeHandleAdapter;
import rice.pastry.transport.TLDeserializer;

public class PeerReviewCallbackImpl implements PeerReviewCallback<TransportLayerNodeHandle<MultiInetSocketAddress>, Id>,
    TransportLayer<TransportLayerNodeHandle<MultiInetSocketAddress>, ByteBuffer> {

  CallbackFactory nodeFactory;
  
  PastryNode pn;
  TransportLayer<TransportLayerNodeHandle<MultiInetSocketAddress>, ByteBuffer> tl;
  TransportLayerCallback<TransportLayerNodeHandle<MultiInetSocketAddress>, ByteBuffer>callback;

  FetchLeafsetApp fetchLeafSetApp;
  
  Logger logger;
  
  public PeerReviewCallbackImpl(PastryNode pn, TransportLayer<TransportLayerNodeHandle<MultiInetSocketAddress>, ByteBuffer> tl, CallbackFactory nodeFactory) {
    this(tl);
    this.pn = pn;
    this.nodeFactory = nodeFactory;
    this.logger = pn.getEnvironment().getLogManager().getLogger(getClass(), null);
    
    this.fetchLeafSetApp = new FetchLeafsetApp(pn,4);
    fetchLeafSetApp.register();
  }
  
  // only used in getReplayInstance
  protected PeerReviewCallbackImpl(TransportLayer<TransportLayerNodeHandle<MultiInetSocketAddress>, ByteBuffer> tl) {
    this.tl = tl;    
  }

  /**
   * Construct a PastryNode down to my layer (figure out how to borrow the code from the PastryNodeFactory)
   * Construct a PeerReviewCallbackImpl with the PastryNode/Verifier
   * Construct the layers above self, attach them appropriately.
   */
  public PeerReviewCallback<TransportLayerNodeHandle<MultiInetSocketAddress>, Id> getReplayInstance(
      Verifier<TransportLayerNodeHandle<MultiInetSocketAddress>> v) {
    // defer the pastry node construction until we can get a checkpoint!
    return new PeerReviewCallbackImpl(v);
  }

  /**
   * Store rt/leafset
   */
  public void storeCheckpoint(OutputBuffer buffer) throws IOException {
    pn.getId().serialize(buffer); // redundant, but necessary for the way a PastryNode is constructed
    pn.getLocalHandle().serialize(buffer);
  }
  
  /**
   * Load rt/leafset
   */
  public boolean loadCheckpoint(InputBuffer buffer) throws IOException {
    Environment environment = ((Verifier<TransportLayerNodeHandle<MultiInetSocketAddress>>)tl).getEnvironment();
    Id nodeId = Id.build(buffer);
    pn = new PastryNode(nodeId, environment);
    NodeHandleFactory nodeHandleFactory = nodeFactory.getNodeHandleFactory(pn);
    NodeHandle handle = nodeHandleFactory.readNodeHandle(buffer);
    // put this in the localHandleTable, where it will be read/removed in the hacked PastryNodeFactory
    nodeFactory.localHandleTable.put(pn,handle); 
    nodeFactory.nodeHandleHelper(pn);

    // load rt/leafset, liveness state
    // register witness searching algorithm

    return true;
  }

  public Collection<TransportLayerNodeHandle<MultiInetSocketAddress>> getMyWitnessedNodes() {
    Collection<NodeHandle> foo = pn.getLeafSet().getUniqueSet(); // TODO: make this a smaller set
    ArrayList<TransportLayerNodeHandle<MultiInetSocketAddress>> ret = 
      new ArrayList<TransportLayerNodeHandle<MultiInetSocketAddress>>(foo.size());
    for (NodeHandle h : foo) {
      ret.add((TransportLayerNodeHandle<MultiInetSocketAddress>)h);
    }
    logger.log("myWitnessedNodes: "+ret);
    return ret;
  }
  
  public void init() {
    // TODO Auto-generated method stub
    
  }

  public void destroy() {
    // TODO Auto-generated method stub
    
  }

  public void notifyCertificateAvailable(Id id) {
    // TODO Auto-generated method stub
    
  }

  public void incomingSocket(P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> s) throws IOException {
    callback.incomingSocket(s);
  }

  public void messageReceived(TransportLayerNodeHandle<MultiInetSocketAddress> i, ByteBuffer m,
      Map<String, Object> options) throws IOException {
    callback.messageReceived(i, m, options);
  }

  public void notifyStatusChange(Id id, int newStatus) {
    // TODO Auto-generated method stub
    
  }

  public void acceptMessages(boolean b) {
    // TODO Auto-generated method stub
    
  }

  public void acceptSockets(boolean b) {
    // TODO Auto-generated method stub
    
  }

  public void getWitnesses(
      final Id subject,
      final WitnessListener<TransportLayerNodeHandle<MultiInetSocketAddress>, Id> callback) {
    fetchLeafSetApp.getNeighbors(subject,new Continuation<Collection<NodeHandle>, Exception>() {
    
      public void receiveResult(Collection<NodeHandle> result) {
        ArrayList<TransportLayerNodeHandle<MultiInetSocketAddress>> ret = new ArrayList<TransportLayerNodeHandle<MultiInetSocketAddress>>(result.size());
        for (NodeHandle nh : result) {
          if (!nh.getId().equals(subject)) ret.add((TransportLayerNodeHandle<MultiInetSocketAddress>)nh);
        }
        logger.log("returning witnesses for "+subject+" "+ret);
        callback.notifyWitnessSet(subject, ret);
      }
    
      public void receiveException(Exception exception) {
        throw new RuntimeException(exception);
      }
    
    });
  }

  public TransportLayerNodeHandle<MultiInetSocketAddress> getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  public SocketRequestHandle<TransportLayerNodeHandle<MultiInetSocketAddress>> openSocket(TransportLayerNodeHandle<MultiInetSocketAddress> i,
      SocketCallback<TransportLayerNodeHandle<MultiInetSocketAddress>> deliverSocketToMe, Map<String, Object> options) {
    return tl.openSocket(i, deliverSocketToMe, options);
  }

  public void setCallback(
      TransportLayerCallback<TransportLayerNodeHandle<MultiInetSocketAddress>, ByteBuffer> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<TransportLayerNodeHandle<MultiInetSocketAddress>> handler) {
    // TODO Auto-generated method stub
    
  }

  public MessageRequestHandle<TransportLayerNodeHandle<MultiInetSocketAddress>, ByteBuffer> sendMessage(
      TransportLayerNodeHandle<MultiInetSocketAddress> i,
      ByteBuffer m,
      MessageCallback<TransportLayerNodeHandle<MultiInetSocketAddress>, ByteBuffer> deliverAckToMe,
      Map<String, Object> options) {
    return tl.sendMessage(i, m, deliverAckToMe, options);
  }


}
