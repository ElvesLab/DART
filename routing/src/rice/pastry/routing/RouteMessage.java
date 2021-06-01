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
package rice.pastry.routing;

import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.util.rawserialization.SimpleInputBuffer;

import rice.pastry.*;
import rice.pastry.commonapi.PastryEndpointMessage;
import rice.pastry.messaging.*;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;

import java.io.*;
import java.util.Map;

/**
 * A route message contains a pastry message that has been wrapped to be sent to
 * another pastry node.
 * 
 * @version $Id: RouteMessage.java 4279 2008-07-15 11:13:33Z jeffh $
 * 
 * @author Andrew Ladd
 */

public class RouteMessage extends PRawMessage implements Serializable,
    rice.p2p.commonapi.RouteMessage {
  private static final long serialVersionUID = 3492981895989180093L;

  public static final short TYPE = -23525;

  private Id target;
  private NodeHandle destinationHandle;
  private transient byte version;

  private NodeHandle prevNode;

  private transient SendOptions opts;

  private int auxAddress;

  private transient NodeHandle nextHop;

  public Message internalMsg;
  // optimization to not use instanceof in the normal new case
  private transient PRawMessage rawInternalMsg = null;
  private transient InputBuffer serializedMsg;
  private transient PastryNode pn;
  
  boolean hasSender;
  byte internalPriority;
  short internalType;
  
  /**
   * This is used by the Rerouter to keep track of how many attempted reroutes of the message.
   */
  public transient int numRetries = 0;
  
  private RMDeserializer endpointDeserializer = new RMDeserializer();
  
  /**
   * Constructor.
   * 
   * @param target this is id of the node the message will be routed to.
   * @param msg the wrapped message.
   * @param cred the credentials for the message.
   */

  public RouteMessage(Id target, Message msg, byte serializeVersion) {
    this(target, msg, null, null, serializeVersion);
  }

  /**
   * Constructor.
   * 
   * @param target this is id of the node the message will be routed to.
   * @param msg the wrapped message.
   * @param cred the credentials for the message.
   * @param opts the send options for the message.
   */

  public RouteMessage(Id target, Message msg, SendOptions opts, byte serializeVersion) {
    this(target, msg, null, opts, serializeVersion);
  }

  /**
   * Constructor.
   * 
   * @param dest the node this message will be routed to
   * @param msg the wrapped message.
   * @param cred the credentials for the message.
   * @param opts the send options for the message.
   * @param aux an auxilary address which the message after each hop.
   */

  public RouteMessage(NodeHandle dest, Message msg,
      SendOptions opts, byte serializeVersion) {
    this(dest.getNodeId(), msg, dest, opts, serializeVersion);
  }

  /**
   * Constructor.
   * 
   * @param target this is id of the node the message will be routed to.
   * @param msg the wrapped message.
   * @param firstHop the nodeHandle of the first hop destination
   * @param aux an auxilary address which the message after each hop.
   */
  public RouteMessage(Id target, Message msg, NodeHandle firstHop, byte serializeVersion) {
    this(target, msg, firstHop, null, serializeVersion);
  }
  
  private static PRawMessage convert(Message msg) {
//    if (msg instanceof PastryEndpointMessage) {
//      msg = ((PastryEndpointMessage)msg).getMessage(); 
//    }
    if (msg instanceof PRawMessage) {
      PRawMessage prm = (PRawMessage)msg;
      if (prm.getType() == 0) 
        if (prm instanceof PJavaSerializedMessage) throw new RuntimeException("Cannot route a PJavaSerializedMessage, this is used internally in RouteMessage."+msg+" "+msg.getClass().getName());
      return prm;          
    }
    return new PJavaSerializedMessage(msg);
  }
  
  public RouteMessage(Id target, PRawMessage msg, NodeHandle firstHop, SendOptions opts, byte serializeVersion) {
    this(target, (Message)msg, firstHop, opts, serializeVersion);
    rawInternalMsg = msg;
    if (msg != null) internalType = msg.getType();
  }
  
  /**
   * Constructor.
   * 
   * @param target this is id of the node the message will be routed to.
   * @param msg the wrapped message.
   * @param firstHop the nodeHandle of the first hop destination
   * @param opts the send options for the message.
   * @param aux an auxiliary address which the message after each hop.
   */
  public RouteMessage(Id target, Message msg, NodeHandle firstHop, SendOptions opts, byte serializeVersion) {
    super(RouterAddress.getCode());
    this.version = serializeVersion;
    this.target = (Id) target;
    internalMsg = msg;
    nextHop = firstHop;
    this.opts = opts;
    if (this.opts == null) this.opts = new SendOptions();
    if (msg != null) // can be null on the deserialization, but that ctor properly sets auxAddress
      auxAddress = msg.getDestination();
  }
  
  

  
  /**
   * Gets the target node id of this message.
   * 
   * @return the target node id.
   */

  public Id getTarget() {
    return target;
  }

  public NodeHandle getPrevNode() {
    return prevNode;
  }

  public void setPrevNode(NodeHandle n) {
    prevNode = n;
  }

  public NodeHandle getNextHop() {
    return nextHop;
  }

  public void setNextHop(NodeHandle nh) {
//    new Exception(this+".setNextHop("+nh+"):"+nh.getLiveness()).printStackTrace();
    nextHop = nh;
  }

  /**
   * Get priority
   * 
   * @return the priority of this message.
   */

  public int getPriority() {
    if (internalMsg != null)
      return internalMsg.getPriority();
    return internalPriority;
  }

  /**
   * Get receiver address.
   * 
   * @return the address.
   */

//  public int getDestination() {
//    if (nextHop == null || auxAddress == 0)
//      return super.getDestination();
//
//    return auxAddress;
//  }

  /**
   * The wrapped message.
   * 
   * @return the wrapped message.
   * @deprecated use unwrap(MessageDeserializer)
   */
  public Message unwrap() {
    if (internalMsg != null) {
      return internalMsg;
    }
    try {      
      endpointDeserializer.setSubDeserializer(new JavaSerializedDeserializer(pn));
      return unwrap(endpointDeserializer);//pn.getEnvironment().getLogManager().getLogger(RouteMessage.class, null)));
    } catch (IOException ioe) {
      throw new RuntimeException(ioe); 
    }
  }

  /**
   * Get transmission options.
   * 
   * @return the options.
   */

  public SendOptions getOptions() {
    if (opts == null) {
      opts = new SendOptions();
    }
    return opts;
  }

  public String toString() {
    if (internalMsg == null) {
      return "R[serialized{"+auxAddress+","+internalType+"}]";
    }
    String str = "R[" + internalMsg + "]";

    return str;
  }

  // Common API Support

  public rice.p2p.commonapi.Id getDestinationId() {
    return getTarget();
  }

  public rice.p2p.commonapi.NodeHandle getNextHopHandle() {
    return nextHop;
  }

  /**
   * @deprecated use getMessage(MessageDeserializer)
   */
  @SuppressWarnings("deprecation")
  public rice.p2p.commonapi.Message getMessage() {
    return ((PastryEndpointMessage) unwrap()).getMessage();
  }

  public rice.p2p.commonapi.Message getMessage(MessageDeserializer md) throws IOException {
    endpointDeserializer.setSubDeserializer(md);
    return ((PastryEndpointMessage) unwrap(endpointDeserializer)).getMessage();
  }

  public void setDestinationId(rice.p2p.commonapi.Id id) {
    target = (Id) id;
  }

  public void setNextHopHandle(rice.p2p.commonapi.NodeHandle nextHop) {
    this.nextHop = (NodeHandle) nextHop;
  }

  public void setMessage(rice.p2p.commonapi.Message message) {
    ((PastryEndpointMessage) unwrap()).setMessage(message);
  }

  public void setMessage(RawMessage message) {
    ((PastryEndpointMessage) unwrap()).setMessage(message);
  }

  /**
   * version 1:
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            int auxAddress                                     +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + bool hasHndle +        // if it has a destinationHandle instead of an Id
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            Id    target                                       +
   *   +  (only existis if the hasHandle boolean is false              +
   *   +                                                               +
   *   +                                                               +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            NodeHandle destinationHandle                       + 
   *   +  (used if the RouteMessage is intended for a specific node)   +
   *   +       (only exists if the hasHandle boolean is true)          +
   *        ...                                                         
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            NodeHandle prev                                    + 
   *   +  (used to repair routing table during routing)                +
   *   +                                                               +
   *        ...                                                         
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            Internal Message                                   + 
   *   +  (see below)                                                  +
   *   +                                                               +
   *   
   * version 0:
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            int auxAddress                                     +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            Id    target                                       +
   *   +                                                               +
   *   +                                                               +
   *   +                                                               +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            NodeHandle prev                                    + 
   *   +  (used to repair routing table during routing)                +
   *   +                                                               +
   *        ...                                                         
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   +            Internal Message                                   + 
   *   +  (see below)                                                  +
   *   +                                                               +
   * @param buf
   * @return
   */
  public static RouteMessage build(InputBuffer buf, byte priority, PastryNode pn, NodeHandle prev, byte outputVersion) throws IOException {
  
    byte version = buf.readByte();
    switch(version) {
    case 0:
      {
        int auxAddress = buf.readInt();
        Id target = Id.build(buf);
//        NodeHandle prev = pn.readNodeHandle(buf);
        return new RouteMessage(target, auxAddress, prev, buf, priority, pn, null, outputVersion);
      }
    case 1:
      {
        int auxAddress = buf.readInt();
        NodeHandle destHandle = null;
        Id target = null;
        boolean hasDestHandle = buf.readBoolean();
        if (hasDestHandle) { // destHandle exists
          destHandle = pn.readNodeHandle(buf);
          target = (rice.pastry.Id)destHandle.getId();
        } else {
          target = Id.build(buf);
        }
//        NodeHandle prev = pn.readNodeHandle(buf);
//        System.out.println("RM.build() v:"+version+" aux:"+auxAddress+" t:"+target+" d:"+destHandle+" hDh:"+hasDestHandle); 
        return new RouteMessage(target, auxAddress, prev, buf, priority, pn, destHandle, outputVersion);
      }
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
  
  public RouteMessage(Id target, int auxAddress, NodeHandle prev, InputBuffer buf, byte priority, PastryNode pn, NodeHandle destinationHandle, byte serializeVersion) throws IOException {
    this(target, null, null, null, serializeVersion);
    hasSender = buf.readBoolean();
    internalPriority = priority; //buf.readByte();
    internalType = buf.readShort();
    prevNode = prev;
    serializedMsg = buf;
    this.destinationHandle = destinationHandle;
    this.pn = pn;
    this.auxAddress = auxAddress;
//    System.out.println("RouteMessage.build(): v:"+version+" addr:"+auxAddress+" dest:"+destinationHandle+" target:"+target+" prev:"+prevNode+" pri:"+internalPriority+" type:"+internalType+" hasSender:"+hasSender);

  }

  public void serialize(OutputBuffer buf) throws IOException {
//    System.out.println(this+".serialize()");
    buf.writeByte(version); // version (deserialized in build())
    buf.writeInt(auxAddress); // (deserialized in build())
    switch (version) {
    case 0:
      target.serialize(buf); // (deserialized in build())
      break;
    case 1:
      buf.writeBoolean(destinationHandle != null); // (deserialized in build())
      if (destinationHandle != null) {
        destinationHandle.serialize(buf); // (deserialized in build())
      } else {
        target.serialize(buf); // (deserialized in build())        
      }            
    } // switch
    
//    prevNode.serialize(buf); // (deserialized in build())
    if (serializedMsg != null) { // pri, sdr
//      System.out.println(this+".serialize() 1 "+serializedMsg.bytesRemaining());
      // fixed Fabio's bug from Nov 2006 (these were deserialized in the constructer above, but not added back into the internal stream.)
      buf.writeBoolean(hasSender);
//      buf.writeByte(internalPriority);      
      
      buf.writeShort(internalType);
      
      // optimize this, possibly by extending InternalBuffer interface to access the raw underlieing bytes
      byte[] raw = new byte[serializedMsg.bytesRemaining()]; 
      serializedMsg.read(raw);
      buf.write(raw,0,raw.length);
      serializedMsg = new SimpleInputBuffer(raw);
    } else {
//      System.out.println(this+".serialize() 2");
      if (rawInternalMsg == null) {
        rawInternalMsg = convert(internalMsg); 
      }
//    address was already peeled off as the auxAddress
//    different wire to deserialize the Address and eliminate unneeded junk
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    +bool hasSender +   Priority    +  Type (Application specifc)   + // zero is java serialization
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     
//    optional      
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    +            NodeHandle sender                                  + 
//    +                                                               +
//                      ...  flexable size  
//    +                                                               +
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      NodeHandle sender = rawInternalMsg.getSender();
      boolean hasSender = (sender != null);
      if (hasSender) {
        buf.writeBoolean(true);
      } else {
        buf.writeBoolean(false);
      }

      // range check priority
      int priority = rawInternalMsg.getPriority();
      if (priority > Byte.MAX_VALUE) throw new IllegalStateException("Priority must be in the range of "+Byte.MIN_VALUE+" to "+Byte.MAX_VALUE+".  Lower values are higher priority. Priority of "+rawInternalMsg+" was "+priority+".");
      if (priority < Byte.MIN_VALUE) throw new IllegalStateException("Priority must be in the range of "+Byte.MIN_VALUE+" to "+Byte.MAX_VALUE+".  Lower values are higher priority. Priority of "+rawInternalMsg+" was "+priority+".");
//      buf.writeByte((byte)priority);

      short type = rawInternalMsg.getType();
      buf.writeShort(type);
      
//      System.out.println("RouteMessage.serialize(): v:"+version+" addr:"+auxAddress+" dest:"+destinationHandle+" target:"+target+" prev:"+prevNode+" pri:"+priority+" type:"+type+" sender:"+sender);

      if (hasSender) {
        sender.serialize(buf);
      }

      rawInternalMsg.serialize(buf);       
    }    
  }

  public Message unwrap(MessageDeserializer md) throws IOException {
    if (internalMsg != null) {
      return internalMsg;
    }
//    
//      if (internalMsg.getType() == 0) {
//        PJavaSerializedMessage pjsm = (PJavaSerializedMessage)internalMsg;
//        return pjsm.getMessage();
//      }
//      return internalMsg;
//    }
    
    // deserialize using md
    
//  address was already peeled off as the auxAddress
//  different wire to deserialize the Address and eliminate unneeded junk
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  +bool hasSender +   Priority    +  Type (Application specifc)   + // zero is java serialization
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//   
//  optional      
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  +            NodeHandle sender                                  + 
//  +                                                               +
//                    ...  flexable size  
//  +                                                               +
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
    NodeHandle internalSender = null;
//    short internalType = serializedMsg.readShort();
    if (hasSender) {
      internalSender = pn.readNodeHandle(serializedMsg);
    }
    
    internalMsg = (Message)md.deserialize(serializedMsg, internalType, internalPriority, internalSender);
    
    // the serializedMsg is now dirty, because the unwrapper may change the internal message
    serializedMsg = null;
    pn = null;

    return internalMsg;
  }

  public short getType() {
    return TYPE;
  }

  class RMDeserializer extends PJavaSerializedDeserializer {
    MessageDeserializer sub;
    
    public RMDeserializer() {
      // the late binding of pn is pretty problematic, we'll set it right before we deserialize
      // the thing is, we usually won't even need it
      super(null);
    }

    public void setSubDeserializer(MessageDeserializer md) {
      sub = md;
    }

    public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
      // just in case we have to do java serialization
      pn = RouteMessage.this.pn;
//      switch(type) {
//        case PastryEndpointMessage.TYPE:
          return new PastryEndpointMessage(auxAddress, buf, sub, type, priority, sender);
//      }
//      return null;
    }
     
  }
 
  public int getAuxAddress() {
    return auxAddress; 
  }
  
  public short getInternalType() {
    if (rawInternalMsg != null) return rawInternalMsg.getType();
    if (internalMsg != null) {
      if (internalMsg instanceof RawMessage) {
        return ((RawMessage)internalMsg).getType();
      }        
      return 0;
    }
    // we don't yet know the internal type because we haven't deserialized it far enough yet
    return -1;
  }

  public void setDestinationHandle(NodeHandle handle) {
    destinationHandle = handle;    
  }

  public NodeHandle getDestinationHandle() {
    return destinationHandle;
  }

  private transient Map<String, Object> options;
  public Map<String, Object> getTLOptions() {
    return options;
  }
  
  public void setTLOptions(Map<String, Object> options) {
    this.options = options;
  }
  
  // synchronization problem...
  // This is so the RouteMessage can be cancelled
  Cancellable tlCancellable;
  public void setTLCancellable(Cancellable c) {
    tlCancellable = c;
  }
  
  public boolean cancel() {
    return tlCancellable.cancel();
  }

  RouteMessageNotification notifyMe;
  
  public void setRouteMessageNotification(RouteMessageNotification notification) {
    notifyMe = notification;
  }
  
  public void sendSuccess(NodeHandle nextHop) {
    if (notifyMe != null) notifyMe.sendSuccess(this, nextHop);
  }
  
  /**
   * Return true if it notified a higher layer.
   * 
   * @param e
   * @return
   */
  public boolean sendFailed(Exception e) {
    if (notifyMe != null) notifyMe.sendFailed(this, e);
    return notifyMe != null;
  }
}