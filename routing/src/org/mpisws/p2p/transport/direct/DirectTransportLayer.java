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
package org.mpisws.p2p.transport.direct;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.CancellableTask;
import rice.pastry.direct.NetworkSimulator;
import rice.pastry.direct.NodeRecord;

public class DirectTransportLayer<Identifier, MessageType> implements TransportLayer<Identifier, MessageType> {
  protected boolean acceptMessages = true;
  protected boolean acceptSockets = true;

  protected Identifier localIdentifier;
  protected TransportLayerCallback<Identifier, MessageType> callback;
  protected GenericNetworkSimulator<Identifier, MessageType> simulator;
  protected ErrorHandler<Identifier> errorHandler;
  protected LivenessProvider<Identifier> livenessProvider;
  
  protected Environment environment;
  protected Logger logger;
  
  public DirectTransportLayer(Identifier local, 
      NetworkSimulator<Identifier, MessageType> simulator, 
      NodeRecord nr, Environment env) {
    this.localIdentifier = local;
    this.simulator = simulator.getGenericSimulator();
    this.livenessProvider = simulator.getLivenessProvider();
    
    this.environment = env;
    this.logger = environment.getLogManager().getLogger(DirectTransportLayer.class, null);
    simulator.registerNode(local, this, nr);

  }
  
  public void acceptMessages(boolean b) {
    acceptMessages = b;
  }

  public void acceptSockets(boolean b) {
    acceptSockets = b;
  }

  public Identifier getLocalIdentifier() {
    return localIdentifier;
  }
  
  static class CancelAndClose<Identifier, MessageType> implements Cancellable {
    DirectAppSocket<Identifier, MessageType> closeMe;
    Cancellable cancelMe;
    
    public CancelAndClose(DirectAppSocket<Identifier, MessageType> socket, CancellableTask task) {
      this.closeMe = socket;
      this.cancelMe = task;
    }

    public boolean cancel() {
      closeMe.connectorEndpoint.close();
      return cancelMe.cancel();
    }
    
  }

  public SocketRequestHandle<Identifier> openSocket(Identifier i, SocketCallback<Identifier> deliverSocketToMe, Map<String, Object> options) {
    SocketRequestHandleImpl<Identifier> handle = new SocketRequestHandleImpl<Identifier>(i,options, logger);

    
    if (simulator.isAlive(i)) {
      int delay = (int)Math.round(simulator.networkDelay(localIdentifier, i));
      DirectAppSocket<Identifier, MessageType> socket = new DirectAppSocket<Identifier, MessageType>(i, localIdentifier, deliverSocketToMe, simulator, handle, options);
      CancelAndClose<Identifier, MessageType> cancelAndClose = new CancelAndClose<Identifier, MessageType>(socket, simulator.enqueueDelivery(socket.getAcceptorDelivery(),
          delay));
      handle.setSubCancellable(cancelAndClose);
    } else {
      int delay = 5000;  // TODO: Make this configurable
      handle.setSubCancellable(
          simulator.enqueueDelivery(
              new ConnectorExceptionDelivery<Identifier>(deliverSocketToMe, handle, new SocketTimeoutException()),delay));
    }
    
    return handle;
  }

  public MessageRequestHandle<Identifier, MessageType> sendMessage(
      Identifier i, MessageType m, 
      MessageCallback<Identifier, MessageType> deliverAckToMe, 
      Map<String, Object> options) {
    if (!simulator.isAlive(localIdentifier)) return null; // just make this stop, the local node is dead, he shouldn't be doing anything
    MessageRequestHandleImpl<Identifier, MessageType> handle = new MessageRequestHandleImpl<Identifier, MessageType>(i, m, options);
    
    if (livenessProvider.getLiveness(i, null) >= LivenessListener.LIVENESS_DEAD) {
      if (logger.level <= Logger.FINE)
        logger.log("Attempt to send message " + m
            + " to a dead node " + i + "!");      
      
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(handle, new NodeIsFaultyException(i));
    } else {
      if (simulator.isAlive(i)) {
        int delay = (int)Math.round(simulator.networkDelay(localIdentifier, i));
//        simulator.notifySimulatorListenersSent(m, localIdentifier, i, delay);
        handle.setSubCancellable(simulator.deliverMessage(m, i, localIdentifier, delay));
        if (deliverAckToMe != null) deliverAckToMe.ack(handle);
      } else {
        // drop message because the node is dead
      }
    }
    return handle;
  }

  public void setCallback(TransportLayerCallback<Identifier, MessageType> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  public void destroy() {
    simulator.remove(getLocalIdentifier());
  }

  public boolean canReceiveSocket() {
    return acceptSockets;
  }

  public void finishReceiveSocket(P2PSocket<Identifier> acceptorEndpoint) {
    try {
      callback.incomingSocket(acceptorEndpoint);
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.logException("Exception in "+callback,ioe);
    }
  }

  public Logger getLogger() {
    return logger;
  }

  int seq = Integer.MIN_VALUE;
  
  public synchronized int getNextSeq() {
    return seq++;
  }
    
  public void incomingMessage(Identifier i, MessageType m, Map<String, Object> options) throws IOException {
    callback.messageReceived(i, m, options);
  }
  
  public Environment getEnvironment() {
    return environment;
  }

  public void clearState(Identifier i) {
    // do nothing
  }
}
