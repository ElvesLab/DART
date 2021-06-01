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
package rice.pastry.transport;

import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.proximity.ProximityListener;
import org.mpisws.p2p.transport.proximity.ProximityProvider;

import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.NodeHandle;
import rice.pastry.boot.Bootstrapper;

@SuppressWarnings("unchecked")
public class NodeHandleAdapter implements 
    TransportLayer<NodeHandle, RawMessage>, 
    LivenessProvider<NodeHandle>, 
    ProximityProvider<NodeHandle> {
  
  TransportLayer tl;
  LivenessProvider livenessProvider;
  ProximityProvider proxProvider;
  Bootstrapper boot;
  
  public NodeHandleAdapter(TransportLayer tl, LivenessProvider livenessProvider, ProximityProvider proxProvider) {
    this.tl = tl;
    this.livenessProvider = livenessProvider;
    this.proxProvider = proxProvider;
  }

  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }
  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }
  public NodeHandle getLocalIdentifier() {
    return (NodeHandle)tl.getLocalIdentifier();
  }
  public SocketRequestHandle<NodeHandle> openSocket(NodeHandle i, SocketCallback<NodeHandle> deliverSocketToMe, Map<String, Object> options) {
    return tl.openSocket(i, deliverSocketToMe, options);
  }
  
  public MessageRequestHandle<NodeHandle, RawMessage> sendMessage(NodeHandle i, RawMessage m, MessageCallback<NodeHandle, RawMessage> deliverAckToMe, Map<String, Object> options) {
    return tl.sendMessage(i, m, deliverAckToMe, options);
  }
  public void setCallback(TransportLayerCallback<NodeHandle, RawMessage> callback) {
    tl.setCallback(callback);
  }
  public void setErrorHandler(ErrorHandler<NodeHandle> handler) {
    tl.setErrorHandler(handler);
  }
  public void destroy() {
    tl.destroy();
  }
  
  public void addLivenessListener(LivenessListener<NodeHandle> name) {
    livenessProvider.addLivenessListener(name);
  }
  public boolean checkLiveness(NodeHandle i, Map<String, Object> options) {
    return livenessProvider.checkLiveness(i, options);
  }
  public int getLiveness(NodeHandle i, Map<String, Object> options) {
    return livenessProvider.getLiveness(i, options);
  }
  public boolean removeLivenessListener(LivenessListener<NodeHandle> name) {
    return livenessProvider.removeLivenessListener(name);
  }
  public void addProximityListener(ProximityListener<NodeHandle> listener) {
    proxProvider.addProximityListener(listener);
  }
  public int proximity(NodeHandle i, Map<String, Object> options) {
    return proxProvider.proximity(i, options);
  }
  public boolean removeProximityListener(ProximityListener<NodeHandle> listener) {
    return proxProvider.removeProximityListener(listener);
  }

  public TransportLayer getTL() {
    return tl;
  }

  public void clearState(NodeHandle i) {
    livenessProvider.clearState(i);
  }
}
