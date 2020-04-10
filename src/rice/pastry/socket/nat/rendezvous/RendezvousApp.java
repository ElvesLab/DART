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
package rice.pastry.socket.nat.rendezvous;

import java.net.InetSocketAddress;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.rendezvous.RendezvousContact;
import org.mpisws.p2p.transport.rendezvous.RendezvousStrategy;
import org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayer;
import org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.mpisws.p2p.transport.wire.WireTransportLayer;

import rice.Continuation;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.MessageReceipt;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.util.AttachableCancellable;
import rice.pastry.PastryNode;
import rice.pastry.client.PastryAppl;
import rice.pastry.leafset.LeafSet;
import rice.pastry.messaging.Message;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RouteMessageNotification;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;
import rice.selector.SelectorManager;

/**
 * TODO: make not abstract
 * 
 * @author Jeff Hoye
 *
 */
public class RendezvousApp extends PastryAppl implements RendezvousStrategy<RendezvousSocketNodeHandle> {
  protected LeafSet leafSet;
  protected SelectorManager selectorManager;
  protected RendezvousTransportLayer<RendezvousSocketNodeHandle> tl;
  
  
  public RendezvousApp(PastryNode pn) {
    super(pn,null,0,null);
    setDeserializer(new MessageDeserializer() {
    
      public rice.p2p.commonapi.Message deserialize(InputBuffer buf, short type,
          int priority, NodeHandle sender) throws IOException {
        byte version;
        switch (type) {
        case ByteBufferMsg.TYPE:
          version = buf.readByte();
          if (version == 0) { // version 0
            NodeHandle originalSender = thePastryNode.readNodeHandle(buf);
            int length = buf.readInt();
            byte[] msg = new byte[length];
            buf.read(msg);
            return new ByteBufferMsg(ByteBuffer.wrap(msg),originalSender,priority,getAddress());
          } else {
            throw new IllegalArgumentException("Unknown version for ByteBufferMsg: "+version);
          }
        case PilotForwardMsg.TYPE:
          version = buf.readByte();
          if (version == 0) { // version 0            
            RendezvousSocketNodeHandle target = (RendezvousSocketNodeHandle)thePastryNode.readNodeHandle(buf);
            ByteBufferMsg subMsg = (ByteBufferMsg)deserialize(buf, ByteBufferMsg.TYPE, priority, sender);
            return new PilotForwardMsg(getAddress(), subMsg, target);
          } else {
            throw new IllegalArgumentException("Unknown version for PilotForwardMsg: "+version);
          }
        case OpenChannelMsg.TYPE:
          version = buf.readByte();
          if (version == 0) { // version 0            
            RendezvousSocketNodeHandle rendezvous = (RendezvousSocketNodeHandle)thePastryNode.readNodeHandle(buf);
            RendezvousSocketNodeHandle source = (RendezvousSocketNodeHandle)thePastryNode.readNodeHandle(buf);
            int uid = buf.readInt();
            return new OpenChannelMsg(getAddress(), rendezvous, source, uid);
          } else {
            throw new IllegalArgumentException("Unknown version for PilotForwardMsg: "+version);
          }
        default:
          throw new IllegalArgumentException("Unknown type: "+type);            
        }
      }    
    }); // this constructor doesn't auto-register
    leafSet = pn.getLeafSet();
    selectorManager = pn.getEnvironment().getSelectorManager();
  }
  
  /**
   * Can be called before you boot, will tell you if you are Firewalled.
   * Should send a message to the bootstrap, who forwards it to another node who sends you the request back.  Should
   * try UDP/TCP.
   * 
   * Returns your external address.
   * 
   * @param bootstrap
   * @param receiveResult
   */
  public void isNatted(NodeHandle bootstrap, Continuation<InetSocketAddress, Exception> receiveResult) {
    
  }

  /**
   * If this is going to cause an infinite loop, just drop the message.
   * 
   * The infinite loop is caused when the next hop is the destination of the message, and is NATted.
   * 
   * ... and we're not connected?
   * 
   */
//  @Override
//  public void receiveMessage(Message msg) {
//    if (msg instanceof RouteMessage) {
//      RouteMessage rm = (RouteMessage)msg;
//      try {
////        Message internalMsg = (Message)rm.unwrap(getDeserializer());
////      if (internalMsg instanceof OpenChannelMsg) {
////        OpenChannelMsg ocm = (OpenChannelMsg)internalMsg;
////        if (rm.getNextHop().equals(rm.getDestinationHandle())) {
////          // this isn't going to work, we are routing to open a connection, but directly to the node in question
////          throw new IllegalStateException("Routing "+rm+", next hop and destination are "+rm.getNextHop()+" "+thePastryNode);
////          
////          // there could also be some kind of oscillation between 2+ routes, which this wouldn't cover
////        }
////      }
//      // if the next hop is no good..., drop it
////      if (rm.getNextHop() )
////        rm.sendFailed(e);
//        super.receiveMessage(msg);
//      } catch (IOException ioe) {
//        // couldn't deserialize the internal message
//      }
//    } else {
//      super.receiveMessage(msg);
//    }
//  }
  
  @Override
  public void messageForAppl(Message msg) {
    if (msg instanceof ByteBufferMsg) {
      ByteBufferMsg bbm = (ByteBufferMsg)msg;
      if (logger.level <= Logger.FINE) logger.log("messageForAppl("+bbm+")");
      try {
        tl.messageReceivedFromOverlay((RendezvousSocketNodeHandle)bbm.getOriginalSender(), bbm.buffer, null);
      } catch (IOException ioe) {
        if (logger.level <= Logger.WARNING) logger.logException("dropping "+bbm, ioe);
      }
      // TODO: Get a reference to the TL... is this an interface?  Should it be?
      // TODO: Deliver this to the TL.
      
  //    tl.messageReceived();
  //    throw new RuntimeException("Not implemented.");
      return;
    }
    if (msg instanceof PilotForwardMsg) {
      PilotForwardMsg pfm = (PilotForwardMsg)msg;
      if (logger.level <= Logger.FINER) logger.log("Forwarding message "+pfm);
      thePastryNode.send(pfm.getTarget(), pfm.getBBMsg(), null, null);
      return;
    }
    if (msg instanceof OpenChannelMsg) {
      OpenChannelMsg ocm = (OpenChannelMsg)msg;
      // we're a NATted node who needs to open a channel
      tl.openChannel(ocm.getSource(),  ocm.getRendezvous(), ocm.getUid());
      return;
    }
  }

  public boolean deliverWhenNotReady() {
    return true;
  }  

  public Cancellable openChannel(final RendezvousSocketNodeHandle target, 
      final RendezvousSocketNodeHandle rendezvous, 
      final RendezvousSocketNodeHandle source,
      final int uid,
      final Continuation<Integer, Exception> deliverAckToMe,
      final Map<String, Object> options) {

    if (logger.level <= Logger.INFO) logger.log("openChannel()"+source+"->"+target+" via "+rendezvous+" uid:"+uid+","+deliverAckToMe+","+options);

    if (target.getLiveness() > LivenessListener.LIVENESS_DEAD) {
      // if he's dead forever (consider changing this to dead... but think of implications)
      if (logger.level <= Logger.INFO) logger.log("openChannel() attempted to open to dead_forever target. Dropping."+source+"->"+target+" via "+rendezvous+" uid:"+uid+","+deliverAckToMe+","+options);
      if (deliverAckToMe != null) deliverAckToMe.receiveException(new NodeIsFaultyException(target));
      return null;
    }
    
    if (target.canContactDirect()) {
      // this is a bug
      throw new IllegalArgumentException("Target must be firewalled.  Target:"+target);
    }

    // we don't want state changing, so this can only be called on the selector
    if (!selectorManager.isSelectorThread()) {
      final AttachableCancellable ret = new AttachableCancellable();
      selectorManager.invoke(new Runnable() {
        public void run() {
          ret.attach(openChannel(target, rendezvous, source, uid, deliverAckToMe, options));
        }
      });
      return ret;
    }

    OpenChannelMsg msg = new OpenChannelMsg(getAddress(), rendezvous, source, uid);
    if (logger.level <= Logger.FINE) logger.log("routing "+msg+" to "+target);
    final RouteMessage rm = 
      new RouteMessage(
          target.getNodeId(),
          msg,
        (byte)thePastryNode.getEnvironment().getParameters().getInt("pastry_protocol_router_routeMsgVersion"));    
    rm.setDestinationHandle(target);

    // don't reuse the incoming options, it will confuse things
//    rm.setTLOptions(null);
    
    if (logger.level <= Logger.FINER) logger.log("openChannel("+target+","+rendezvous+","+source+","+uid+","+deliverAckToMe+","+options+") sending via "+rm);      
    
    // TODO: make PastryNode have a router that does this properly, rather than receiveMessage
    final Cancellable ret = new Cancellable() {
      
      public boolean cancel() {
        if (logger.level <= Logger.FINE) logger.log("openChannel("+target+","+rendezvous+","+source+","+uid+","+deliverAckToMe+","+options+").cancel()");
        return rm.cancel();
      }
    };
    
    // NOTE: Installing this anyway if the LogLevel is high enough is kind of wild, but really useful for debugging
    if ((deliverAckToMe != null) || (logger.level <= Logger.INFO)) {
      rm.setRouteMessageNotification(new RouteMessageNotification() {
        public void sendSuccess(rice.pastry.routing.RouteMessage message, rice.pastry.NodeHandle nextHop) {
          if (logger.level <= Logger.FINER) logger.log("openChannel("+target+","+rendezvous+","+source+","+uid+","+deliverAckToMe+","+options+").sendSuccess():"+nextHop);
          if (deliverAckToMe != null) deliverAckToMe.receiveResult(uid);
        }    
        public void sendFailed(rice.pastry.routing.RouteMessage message, Exception e) {
          if (logger.level <= Logger.FINE) logger.log("openChannel("+target+","+rendezvous+","+source+","+uid+","+deliverAckToMe+","+options+").sendFailed("+e+")");
          if (deliverAckToMe != null) deliverAckToMe.receiveException(e);
        }
      });
    }
    
//    Map<String, Object> rOptions;
//    if (options == null) {
//      rOptions = new HashMap<String, Object>(); 
//    } else {
//      rOptions = new HashMap<String, Object>(options);
//    }
//    rOptions.put(PriorityTransportLayer.OPTION_PRIORITY, pm.getPriority());
////    logger.log("NumOptions = "+rOptions.size());
    
    rm.setTLOptions(options);
    
    thePastryNode.getRouter().route(rm);
    
    return ret;
  }

  public MessageRequestHandle<RendezvousSocketNodeHandle, ByteBuffer> sendMessage(
      final RendezvousSocketNodeHandle i, 
      final ByteBuffer m, 
      final MessageCallback<RendezvousSocketNodeHandle, ByteBuffer> deliverAckToMe, 
      Map<String, Object> ops) {
    if (logger.level <= Logger.FINE) logger.log("sendMessage("+i+","+m+","+deliverAckToMe+","+ops+")");
    // TODO: use the new method in PastryAppl
    
    // pull USE_UDP, because that's not going to happen
    final Map<String, Object> options = OptionsFactory.removeOption(ops, WireTransportLayer.OPTION_TRANSPORT_TYPE);
    
    int priority = 0;
    if (options.containsKey(PriorityTransportLayer.OPTION_PRIORITY)) {
      priority = ((Integer)options.get(PriorityTransportLayer.OPTION_PRIORITY));
    }
    ByteBufferMsg msg = new ByteBufferMsg(m, thePastryNode.getLocalHandle(), priority, getAddress());
    
    if (options.containsKey(RendezvousTransportLayerImpl.OPTION_USE_PILOT)) {
//      if (true) throw new RuntimeException("Not Implemented.");
      RendezvousSocketNodeHandle pilot = (RendezvousSocketNodeHandle)options.get(RendezvousTransportLayerImpl.OPTION_USE_PILOT);
      if (logger.level <= Logger.FINER) logger.log("sendMessage("+i+","+m+","+deliverAckToMe+","+options+") sending via "+pilot);      
      final MessageRequestHandleImpl<RendezvousSocketNodeHandle, ByteBuffer> ret = 
        new MessageRequestHandleImpl<RendezvousSocketNodeHandle, ByteBuffer>(i,m,options);
      ret.setSubCancellable(thePastryNode.send(pilot, new PilotForwardMsg(getAddress(),msg,i), new PMessageNotification(){      
        public void sent(PMessageReceipt msg) {
          if (deliverAckToMe != null) deliverAckToMe.ack(ret);
        }
        public void sendFailed(PMessageReceipt msg, Exception reason) {
          if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, reason);
        }      
      }, null));
      return ret;
    } else {
      
      final RouteMessage rm = 
        new RouteMessage(
            i.getNodeId(),
            msg,
          (byte)thePastryNode.getEnvironment().getParameters().getInt("pastry_protocol_router_routeMsgVersion"));    
  //    rm.setPrevNode(thePastryNode.getLocalHandle());
      rm.setDestinationHandle(i);
  
      rm.setTLOptions(options);
      
      if (logger.level <= Logger.FINER) logger.log("sendMessage("+i+","+m+","+deliverAckToMe+","+options+") sending via "+rm);      
      
      // TODO: make PastryNode have a router that does this properly, rather than receiveMessage
      final MessageRequestHandle<RendezvousSocketNodeHandle, ByteBuffer> ret = new MessageRequestHandle<RendezvousSocketNodeHandle, ByteBuffer>() {
        
        public boolean cancel() {
          if (logger.level <= Logger.FINE) logger.log("sendMessage("+i+","+m+","+deliverAckToMe+","+options+").cancel()");
          return rm.cancel();
        }
      
        public ByteBuffer getMessage() {
          return m;
        }
      
        public RendezvousSocketNodeHandle getIdentifier() {
          return i;
        }
  
        public Map<String, Object> getOptions() {
          return options;
        }    
      };
      
      // NOTE: Installing this anyway if the LogLevel is high enough is kind of wild, but really useful for debugging
      if ((deliverAckToMe != null) || (logger.level <= Logger.FINE)) {
        rm.setRouteMessageNotification(new RouteMessageNotification() {
          public void sendSuccess(rice.pastry.routing.RouteMessage message, rice.pastry.NodeHandle nextHop) {
            if (logger.level <= Logger.FINER) logger.log("sendMessage("+i+","+m+","+deliverAckToMe+","+options+").sendSuccess():"+nextHop);
            if (deliverAckToMe != null) deliverAckToMe.ack(ret);
          }    
          public void sendFailed(rice.pastry.routing.RouteMessage message, Exception e) {
            if (logger.level <= Logger.FINE) logger.log("sendMessage("+i+","+m+","+deliverAckToMe+","+options+").sendFailed("+e+")");
            if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, e);
          }
        });
      }
      
  //    Map<String, Object> rOptions;
  //    if (options == null) {
  //      rOptions = new HashMap<String, Object>(); 
  //    } else {
  //      rOptions = new HashMap<String, Object>(options);
  //    }
  //    rOptions.put(PriorityTransportLayer.OPTION_PRIORITY, pm.getPriority());
  ////    logger.log("NumOptions = "+rOptions.size());
      
      rm.setTLOptions(options);
      
      thePastryNode.getRouter().route(rm);
      
      return ret;
    }
  }

  public String toString() {
    return "RendezvousApp{"+thePastryNode+"}";
  }
  
  public void setTransportLayer(
      RendezvousTransportLayer<RendezvousSocketNodeHandle> tl) {
    this.tl = tl;
  }


  
  
  
}
