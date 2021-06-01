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
package org.mpisws.p2p.transport.commonapi;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.mpisws.p2p.transport.priority.QueueOverflowException;
import org.mpisws.p2p.transport.util.DefaultCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.OptionsFactory;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

public class CommonAPITransportLayerImpl<Identifier extends NodeHandle> implements 
    CommonAPITransportLayer<Identifier>, 
    TransportLayerCallback<Identifier, ByteBuffer> /*,
    LivenessListener<Identifier>,
    ProximityListener<Identifier> */{
  
//  TransportLayerNodeHandle<Identifier> localAddress; 
  TransportLayer<Identifier, ByteBuffer> tl;
//  LivenessProvider<Identifier> livenessProvider;
//  ProximityProvider<Identifier> proximityProvider;
  TransportLayerCallback<Identifier, RawMessage> callback;
  ErrorHandler<Identifier> errorHandler;
  RawMessageDeserializer deserializer;
  IdFactory idFactory;
//  NodeHandleFactory<Identifier> nodeHandleFactory;
  Logger logger;
  
  public static final String MSG_CLASS = "commonapi_msg_class";
  public static final String MSG_STRING = "commonapi_msg_string";
  public static final String MSG_TYPE = "commonapi_msg_type";
  public static final String MSG_ADDR = "commonapi_msg_addr";
//  public static final String DESTINATION_IDENTITY = "commonapi_destination_identity";

  protected OptionsAdder optionsAdder;
  
  public CommonAPITransportLayerImpl(
      TransportLayer<Identifier, ByteBuffer> tl, 
      IdFactory idFactory,
      RawMessageDeserializer deserializer,
      OptionsAdder optionsAdder,
      ErrorHandler<Identifier> errorHandler,
      Environment env) {
    
    this.logger = env.getLogManager().getLogger(CommonAPITransportLayerImpl.class, null);
    this.tl = tl;
//    this.localAddress = localAddress;
    this.deserializer = deserializer; 
    this.optionsAdder = optionsAdder;
    if (this.optionsAdder == null) {
      this.optionsAdder = new OptionsAdder() {
      
        public Map<String, Object> addOptions(Map<String, Object> options,
            RawMessage m) {
          return OptionsFactory.addOption(options, MSG_STRING, m.toString(), MSG_TYPE, m.getType(), MSG_CLASS, m.getClass().getName());
        }      
      };
    }
    
    if (tl == null) throw new IllegalArgumentException("tl must be non-null");
//    if (localAddress == null) throw new IllegalArgumentException("localAddress must be non-null");
//    if (proximityProvider == null) throw new IllegalArgumentException("proximityProvider must be non-null");
//    if (livenessProvider == null) throw new IllegalArgumentException("livenessProvider must be non-null");
    if (idFactory == null) throw new IllegalArgumentException("idFactroy must be non-null");
//    if (nodeHandleFactory == null) throw new IllegalArgumentException("idFactroy must be non-null");
    if (deserializer == null) throw new IllegalArgumentException("deserializer must be non-null");
    
//    this.nodeHandleFactory = nodeHandleFactory;
//    this.livenessProvider = livenessProvider;
//    this.proximityProvider = proximityProvider;
//    proximityProvider.addProximityListener(this);
    this.idFactory = idFactory;
    this.errorHandler = errorHandler;
    
    if (this.callback == null) {
      this.callback = new DefaultCallback<Identifier, RawMessage>(env);
    }
    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<Identifier>(logger); 
    }
      
    
    tl.setCallback(this);
//    livenessProvider.addLivenessListener(this);
    
//    this.livenessListeners = new ArrayList<LivenessListener<TransportLayerNodeHandle<Identifier>>>();
  }
  
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

//  public void clearState(TransportLayerNodeHandle<Identifier> i) {
//    livenessProvider.clearState(i.getAddress());
//  }

  public MessageRequestHandle<Identifier, RawMessage> sendMessage(
      final Identifier i,
      final RawMessage m, 
      final MessageCallback<Identifier, RawMessage> deliverAckToMe,
      Map<String, Object> options) {
    if (destroyed) return null;
    
    if (logger.level <= Logger.FINE) logger.log("sendMessage("+i+","+m+")");

    final MessageRequestHandleImpl<Identifier, RawMessage> handle 
      = new MessageRequestHandleImpl<Identifier, RawMessage>(i, m, options);
    final ByteBuffer buf;
    
    // we only serialize the Id, we assume the underlieing layer got the address of the NodeHandle correct
//    SimpleOutputBuffer sob = new SimpleOutputBuffer(4+localAddress.getId().getByteArrayLength());
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      // TODO: maybe we should write my entire address to be compatible with the lower levels, why do we need to do this at all?  
      // Is the contract that the lower level's identifier is proper?  
      // What about the fact that the priorityTL will establish end to end connectivity?  
      // Perhaps this is wasteful.
      // cant put the PriorityTL above this because this is where we serialize
      
//      sob.writeLong(localAddress.getEpoch());
//      localAddress.getId().serialize(sob);
//      if (logger.level <= Logger.FINER) logger.log("sendMessage(): epoch:"+localAddress.getEpoch()+" id:"+localAddress.getId()+" hand:"+localAddress);
//      if (m.getType() == RouteMessage.TYPE) {
////        if (((RouteMessage)m).getInternalType() == PublishMessage.TYPE) {
//          logger.log(m.toString());
////       }
//      }
      deserializer.serialize(m, sob);
//      m.serialize(sob);
    } catch (IOException ioe) {
      if (ioe instanceof NodeIsFaultyException) {
        ioe = new NodeIsFaultyException(i,m, ioe); 
      }
      if (deliverAckToMe == null) {
        errorHandler.receivedException(i, ioe);
      } else {
        deliverAckToMe.sendFailed(handle, ioe);
      }
      return handle;
    }
    
    buf = ByteBuffer.wrap(sob.getBytes());
    if (logger.level <= Logger.FINEST) logger.log("sendMessage("+i+","+m+") serizlized:"+buf);

    handle.setSubCancellable(tl.sendMessage(
        i, 
        buf, 
        new MessageCallback<Identifier, ByteBuffer>() {
    
          public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
            if (logger.level <= Logger.FINER) logger.log("sendMessage("+i+","+m+").ack()");
            if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
            if (deliverAckToMe != null) deliverAckToMe.ack(handle);
          }
        
          public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception ex) {            
            if (ex instanceof NodeIsFaultyException) {
              ex = new NodeIsFaultyException(i, m, ex); 
            }
            if (ex instanceof QueueOverflowException) {
              ex = new QueueOverflowException(i, m, ex); 
            }
            if (logger.level <= Logger.CONFIG) logger.logException("sendFailed("+i+","+m+")",ex);
            if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
            if (deliverAckToMe == null) {
              errorHandler.receivedException(i, ex);
            } else {
              deliverAckToMe.sendFailed(handle, ex);
            }
          }
        }, 
//        OptionsFactory.addOption(options, MSG_STRING, m.toString(), DESTINATION_IDENTITY, i)));
//        OptionsFactory.addOption(options, MSG_STRING, m.toString(), MSG_TYPE, m.getType(), MSG_CLASS, m.getClass().getName())));
        optionsAdder.addOptions(options,m)));
    return handle;
  }

  public void messageReceived(Identifier i, ByteBuffer m, Map<String, Object> options) throws IOException {
//    if (logger.level <= Logger.FINE) logger.log("messageReceived("+i+","+m+")");
    SimpleInputBuffer buf = new SimpleInputBuffer(m.array(), m.position());
//    long epoch = buf.readLong();
//    Id id = idFactory.build(buf);
//    TransportLayerNodeHandle<Identifier> handle = nodeHandleFactory.getNodeHandle(i, epoch, id); 
//    if (logger.level <= Logger.FINER) logger.log("messageReceived(): epoch:"+epoch+" id:"+id+" hand:"+handle);
    RawMessage ret = deserializer.deserialize(buf, i);
    if (logger.level <= Logger.FINE) logger.log("messageReceived("+i+","+ret+")");
    callback.messageReceived(i, ret, options);
  }

  public void setCallback(
      TransportLayerCallback<Identifier, RawMessage> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  protected boolean destroyed = false;
  public void destroy() {
    destroyed = true;
    tl.destroy();
  }

  


//  List<PingListener<TransportLayerNodeHandle<Identifier>>> pingListeners;
//  public void addPingListener(PingListener<TransportLayerNodeHandle<Identifier>> name) {
//    synchronized(pingListeners) {
//      pingListeners.add(name);
//    }
//  }
//
//  public boolean removePingListener(PingListener<TransportLayerNodeHandle<Identifier>> name) {
//    synchronized(pingListeners) {
//      return pingListeners.remove(name);
//    }
//  }
//  
//  private void notifyPingListenersResponse(TransportLayerNodeHandle<Identifier> i, int rtt, Map<String, Object> options) {
//    List<PingListener<TransportLayerNodeHandle<Identifier>>> temp;
//    synchronized(pingListeners) {
//      temp = new ArrayList<PingListener<TransportLayerNodeHandle<Identifier>>>(pingListeners);
//    }
//    for (PingListener<TransportLayerNodeHandle<Identifier>> listener : temp) {
//      listener.pingResponse(i, rtt, null);
//    }
//  }
//
//  private void notifyPingListenersReceived(TransportLayerNodeHandle<Identifier> i, Map<String, Object> options) {
//    List<PingListener<TransportLayerNodeHandle<Identifier>>> temp;
//    synchronized(pingListeners) {
//      temp = new ArrayList<PingListener<TransportLayerNodeHandle<Identifier>>>(pingListeners);
//    }
//    for (PingListener<TransportLayerNodeHandle<Identifier>> listener : temp) {
//      listener.pingReceived(i, options);
//    }
//  }
//
//  public void pingReceived(Identifier i, Map<String, Object> options) {
//    notifyPingListenersReceived(nodeHandleFactory.lookupNodeHandle(i), options);   
//  }
//
//  public void pingResponse(Identifier i, int rtt, Map<String, Object> options) {
//    notifyPingListenersResponse(nodeHandleFactory.lookupNodeHandle(i), rtt, options);    
//  }

//  public boolean checkLiveness(TransportLayerNodeHandle<Identifier> i, Map<String, Object> options) {
//    return livenessProvider.checkLiveness(i.getAddress(), options);
//  }

//  public boolean ping(TransportLayerNodeHandle<Identifier> i, Map<String, Object> options) {
//    return tl.ping(i.getAddress(), options);
//  }

  public SocketRequestHandle<Identifier> openSocket(
      final Identifier i,
      final SocketCallback<Identifier> deliverSocketToMe, 
      Map<String, Object> options) {
    if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");

//    final SocketRequestHandleImpl<TransportLayerNodeHandle<Identifier>> handle = 
//      new SocketRequestHandleImpl<TransportLayerNodeHandle<Identifier>>(i, options);
    
    if (logger.level <= Logger.FINE) logger.log("openSocket("+i+")");
//    return tl.openSocket(i, deliverSocketToMe, OptionsFactory.addOption(options, DESTINATION_IDENTITY, i));
    return tl.openSocket(i, deliverSocketToMe, options);
    
    // we only serialize the Id, we assume the underlieing layer got the address of the NodeHandle correct
//    SimpleOutputBuffer sob = new SimpleOutputBuffer(8+localAddress.getId().getByteArrayLength());
//    try {
//      sob.writeLong(localAddress.getEpoch());
//      localAddress.getId().serialize(sob);
//    } catch (IOException ioe) {
//      deliverSocketToMe.receiveException(handle, ioe);
//      return null;
//    }
//    final ByteBuffer b = ByteBuffer.wrap(sob.getBytes());
//    
//    Identifier addr = i.getAddress();
//        
//    handle.setSubCancellable(tl.openSocket(addr, 
//        new SocketCallback<Identifier>(){    
//      
//      public void receiveResult(SocketRequestHandle<Identifier> c, P2PSocket<Identifier> result) {
//        if (c != handle.getSubCancellable()) throw new RuntimeException("c != cancellable.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+handle.getSubCancellable());
//        
//        if (logger.level <= Logger.FINER) logger.log("openSocket("+i+"):receiveResult("+result+")");
//        result.register(false, true, new P2PSocketReceiver<Identifier>() {        
//          public void receiveSelectResult(P2PSocket<Identifier> socket,
//              boolean canRead, boolean canWrite) throws IOException {
//            if (canRead || !canWrite) throw new IOException("Expected to write! "+canRead+","+canWrite);
//            
//            // do the work
//            socket.write(b);
//            
//            // keep working or pass up the new socket
//            if (b.hasRemaining()) {
//              socket.register(false, true, this); 
//            } else {
//              deliverSocketToMe.receiveResult(handle, 
//                  new SocketWrapperSocket<TransportLayerNodeHandle<Identifier>, Identifier>(
//                      i, socket, logger, socket.getOptions()));                 
//            }
//          }
//        
//          public void receiveException(P2PSocket<Identifier> socket,
//              IOException e) {
//            deliverSocketToMe.receiveException(handle, e);
//          }        
//        });         
//      }    
//      public void receiveException(SocketRequestHandle<Identifier> c, IOException exception) {
//        if (c != handle.getSubCancellable()) throw new RuntimeException("c != cancellable.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+handle.getSubCancellable());
//        deliverSocketToMe.receiveException(handle, exception);
//      }
//    }, options));
//    return handle;
  }

  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
    if (logger.level <= Logger.FINE) logger.log("incomingSocket("+s+")");
    callback.incomingSocket(s);
//    final SocketInputBuffer sib = new SocketInputBuffer(s,1024);
//    s.register(true, false, new P2PSocketReceiver<Identifier>() {
//    
//      public void receiveSelectResult(P2PSocket<Identifier> socket,
//          boolean canRead, boolean canWrite) throws IOException {
//        if (logger.level <= Logger.FINER) logger.log("incomingSocket("+socket+"):receiveSelectResult()");
//        if (canWrite || !canRead) throw new IOException("Expected to read! "+canRead+","+canWrite);
//        try {
//          long epoch = sib.readLong();
//          Id id = idFactory.build(sib);
//          if (logger.level <= Logger.FINEST) logger.log("Read epoch:"+epoch+" id:"+id+" from:"+socket.getIdentifier());
//          callback.incomingSocket(new SocketWrapperSocket<TransportLayerNodeHandle<Identifier>, Identifier>(
//              nodeHandleFactory.getNodeHandle(socket.getIdentifier(), epoch, id), socket, logger, socket.getOptions()));
//        } catch (InsufficientBytesException ibe) {
//          socket.register(true, false, this); 
//        } catch (IOException e) {
//          errorHandler.receivedException(nodeHandleFactory.getNodeHandle(socket.getIdentifier(), 0, null), e);
//        }
//      }
//    
//      public void receiveException(P2PSocket<Identifier> socket,IOException e) {
//        errorHandler.receivedException(nodeHandleFactory.getNodeHandle(socket.getIdentifier(), 0, null), e);
//      }          
//    });
  }
}
