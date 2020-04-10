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

package rice.pastry.commonapi;

import java.io.IOException;

import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.util.rawserialization.JavaSerializedMessage;
import rice.pastry.NodeHandle;
import rice.pastry.messaging.*;

/**
 * This class is an internal message to the commonapi gluecode.
 *
 * @version $Id: PastryEndpointMessage.java 4295 2008-07-18 15:38:04Z jeffh $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public class PastryEndpointMessage extends PRawMessage {

//  public static final short TYPE = 2;
  
  private static final long serialVersionUID = 4499456388556140871L;
  
  protected RawMessage message;
//  protected boolean isRaw = false;
  
  /**
    * Constructor.
   *
   * @param pn the pastry node that the application attaches to.
   */
  public PastryEndpointMessage(int address, Message message, NodeHandle sender) {
    this(address, message instanceof RawMessage ? (RawMessage) message : new JavaSerializedMessage(message), sender);
  }

  public static void checkRawType(RawMessage message) {
    if (message.getType() == 0) {
      if (!(message instanceof JavaSerializedMessage)) {
        throw new IllegalArgumentException("Message "+message+" is raw, but its type is 0, this is only allowed by Java Serialized Messages.");
      }
    }    
  }
  
  public PastryEndpointMessage(int address, RawMessage message, NodeHandle sender) {
    super(address);
    checkRawType(message);
    setSender(sender);
    this.message = message;
//    isRaw = true;
    setPriority(message.getPriority());
  }

  /**
   * Returns the internal message
   *
   * @return the credentials.
   */
  public Message getMessage() {
    if (message.getType() == 0) return ((JavaSerializedMessage)message).getMessage();
    return message;        
  }


  /**
   * Returns the internal message
  *
  * @return the credentials.
  */
 public void setMessage(Message message) {
   if (message instanceof RawMessage) {
     setMessage((RawMessage)message);
   } else {
     this.message = new JavaSerializedMessage(message);
//     isRaw = false;
   }
 }

 /**
  * Returns the internal message
   *
   * @return the credentials.
   */
  public void setMessage(RawMessage message) {
//    isRaw = true;
    this.message = message;
  }

  /**
   * Returns the String representation of this message
   *
   * @return The string
   */
  public String toString() {
//    return "[PEM " + getMessage() + "]";
    return getMessage().toString();
  }
  
  /***************** Raw Serialization ***************************************/  
  public short getType() {
    return message.getType();
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
//    buf.writeBoolean(isRaw); 
    
//    buf.writeByte((byte)0); // version
//    // range check priority
    int priority = message.getPriority();
    if (priority > Byte.MAX_VALUE) throw new IllegalStateException("Priority must be in the range of "+Byte.MIN_VALUE+" to "+Byte.MAX_VALUE+".  Lower values are higher priority. Priority of "+message+" was "+priority+".");
    if (priority < Byte.MIN_VALUE) throw new IllegalStateException("Priority must be in the range of "+Byte.MIN_VALUE+" to "+Byte.MAX_VALUE+".  Lower values are higher priority. Priority of "+message+" was "+priority+".");
//    buf.writeByte((byte)priority);
//
//    buf.writeShort(message.getType());    
    message.serialize(buf);
//    System.out.println("PEM.serialize() message:"+message+" type:"+message.getType());
  }
  
  public PastryEndpointMessage(int address, InputBuffer buf, MessageDeserializer md, short type, int priority, NodeHandle sender) throws IOException {
    super(address);

    byte version = 0;//buf.readByte();
    switch(version) {
      case 0:
        setSender(sender);
    //    isRaw = buf.readBoolean();
//        byte priority = buf.readByte();
//        short type = buf.readShort();
        if (type == 0) {
          message = new JavaSerializedMessage(md.deserialize(buf, type, priority, sender));
        } else {
          message = (RawMessage)md.deserialize(buf, type, priority, sender); 
        }
        if (getMessage() == null) throw new IOException("PEM.deserialize() message = null type:"+type+" md:"+md);
//    System.out.println("PEM.deserialize() message:"+message+" type:"+type+" md:"+md);
        break;
      default:
        throw new IOException("Unknown Version: "+version);
    }
    
  }

}




