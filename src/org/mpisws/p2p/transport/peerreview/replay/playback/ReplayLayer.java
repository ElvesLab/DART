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
package org.mpisws.p2p.transport.peerreview.replay.playback;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.Serializer;

import rice.environment.Environment;
import rice.environment.logging.CloneableLogManager;
import rice.environment.logging.LogManager;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.processing.Processor;
import rice.environment.processing.sim.SimProcessor;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.util.MathUtils;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;

public class ReplayLayer<Identifier> extends ReplayVerifier<Identifier> {

  TransportLayerCallback<Identifier, ByteBuffer> callback;
  Map<Integer, ReplaySocket<Identifier>> sockets = new HashMap<Integer, ReplaySocket<Identifier>>();

  public ReplayLayer(Serializer<Identifier> serializer, HashProvider hashProv, SecureHistory history, Identifier localHandle, Environment environment) throws IOException {
    super(serializer, hashProv, history, localHandle, (short)0, (short)0, 0, environment.getLogManager().getLogger(ReplayLayer.class, localHandle.toString()));
    this.environment = environment;
  }
  
  public SocketRequestHandle<Identifier> openSocket(final Identifier i, SocketCallback<Identifier> deliverSocketToMe, final Map<String, Object> options) {
    try {
      int socketId = openSocket(i);
//      logger.log("openSocket("+i+"):"+socketId);
      ReplaySocket<Identifier> socket = new ReplaySocket<Identifier>(i,socketId,this,options);
      socket.setDeliverSocketToMe(deliverSocketToMe);
      sockets.put(socketId, socket);
      return socket;
    } catch (IOException ioe) {      
      SocketRequestHandle<Identifier> ret = new SocketRequestHandle<Identifier>(){

        public Identifier getIdentifier() {
          return i;
        }

        public Map<String, Object> getOptions() {
          return options;
        }

        public boolean cancel() {
          return true;
        }      
      };
      
      deliverSocketToMe.receiveException(ret, ioe);
      return ret;
    }
  }
  
  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(Identifier i, ByteBuffer m, MessageCallback<Identifier, ByteBuffer> deliverAckToMe, Map<String, Object> options) {
    if (logger.level <= Logger.FINEST) {
      logger.logException("sendMessage("+i+","+m+"):"+MathUtils.toHex(m.array()), new Exception("Stack Trace"));      
    } else if (logger.level <= Logger.FINER) {
      logger.log("sendMessage("+i+","+m+"):"+MathUtils.toHex(m.array()));
    } else if (logger.level <= Logger.FINE) {
      logger.log("sendMessage("+i+","+m+")");      
    }
    MessageRequestHandleImpl<Identifier, ByteBuffer> ret = new MessageRequestHandleImpl<Identifier, ByteBuffer>(i, m, options);
    try {
      send(i, m, -1);
      if (deliverAckToMe != null) deliverAckToMe.ack(ret);
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.logException("", ioe);
      throw new RuntimeException(ioe);
    }
    return ret;
  }

  public Identifier getLocalIdentifier() {
    return localHandle;
  }

  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    // TODO Auto-generated method stub    
  }

  public void destroy() {
  }

  public void acceptMessages(boolean b) {
  }

  public void acceptSockets(boolean b) {    
  }

  Environment environment;

  @Override
  protected void receive(final Identifier from, final ByteBuffer msg) throws IOException {
    if (logger.level <= Logger.FINER) logger.log("receive("+from+","+msg+")");
        
//          if (logger.level <= Logger.FINE) logger.log("receive("+from+","+msg+","+timeToDeliver+")");
    callback.messageReceived(from, msg, null);
  }

  @Override
  protected void socketIO(int socketId, boolean canRead, boolean canWrite) throws IOException {
    sockets.get(socketId).notifyIO(canRead, canWrite);
  }

  @Override
  protected void incomingSocket(Identifier from, int socketId) throws IOException {
    ReplaySocket<Identifier> socket = new ReplaySocket<Identifier>(from, socketId, this, null);
    sockets.put(socketId, socket);
    callback.incomingSocket(socket);
  }
  
  public static Environment generateEnvironment(String name, long startTime, long randSeed, LogManager lm2) {
    Parameters params = new SimpleParameters(Environment.defaultParamFileArray,null);
    DirectTimeSource dts = new DirectTimeSource(startTime);
    
    LogManager lm;
    if ((lm2 != null) && (lm2 instanceof CloneableLogManager)) {      
      CloneableLogManager clm = (CloneableLogManager)lm2;
      lm = clm.clone(clm.getPrefix()+"-"+name, dts);      
    } else {
      lm = Environment.generateDefaultLogManager(dts,params);
    }
    RandomSource rs = new SimpleRandomSource(randSeed, lm);
    dts.setLogManager(lm);
    SelectorManager selector = new ReplaySM("Replay "+name, dts, lm);
    dts.setSelectorManager(selector);
    Processor proc = new SimProcessor(selector);
    Environment env = new Environment(selector,proc,rs,dts,lm,
        params, Environment.generateDefaultExceptionStrategy(lm));
    return env;
  }

  @Override
  protected void socketOpened(int socketId) throws IOException {
//    logger.log("socketOpened("+socketId+")");
    sockets.get(socketId).socketOpened();
  }

  @Override
  protected void socketException(int socketId, IOException ioe) throws IOException {
    //logger.log("socketException("+socketId+")");
//    sockets.get(socketId).receiveException(new IOException("Replay Exception"));
    sockets.get(socketId).receiveException(ioe);
    // TODO Auto-generated method stub
    
  }

  public Environment getEnvironment() {
    return environment;
  }
}
