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

package rice.p2p.multiring.messaging;

import java.io.IOException;
import java.util.Hashtable;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.multiring.*;
import rice.p2p.scribe.*;
import rice.p2p.scribe.rawserialization.RawScribeContent;
import rice.p2p.util.rawserialization.JavaSerializedMessage;

/**
 * @(#) RingMessage.java
 *
 * This class the abstraction of a message used internally by the multiring hierarchy.
 *
 * @version $Id: RingMessage.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
public class RingMessage implements RawScribeContent {
  
  // serialver for backward compatibility
  private static final long serialVersionUID = -7097995807488121199L;

  public static final short TYPE = 1;

  /** 
   * The target of this ring message
   */
  protected RingId id;
  
  /**
   * The internal message to be sent
   */
  protected RawMessage message;
  
  /**
   * The name of the application which sent this message
   */
  protected String application;

  /**
   * Constructor which takes a unique integer Id
   *
   * @param id The unique id
   * @param source The source address
   * @param dest The destination address
   */
//  public RingMessage(RingId id, Message message, String application) {
//    this(id, message instanceof RawMessage ? (RawMessage) message : new JavaSerializedMessage(message), application);
//  }
//  
  public RingMessage(RingId id, RawMessage message, String application) {
    this.id = id;
    this.message = message;
    this.application = application;
  }
  
  /**
   * Method which should return the priority level of this message.  The messages
   * can range in priority from 0 (highest priority) to Integer.MAX_VALUE (lowest) -
   * when sending messages across the wire, the queue is sorted by message priority.
   * If the queue reaches its limit, the lowest priority messages are discarded.  Thus,
   * applications which are very verbose should have LOW_PRIORITY or lower, and
   * applications which are somewhat quiet are allowed to have MEDIUM_PRIORITY or
   * possibly even HIGH_PRIORITY.
   *
   * @return This message's priority
   */
  public int getPriority() {
    return message.getPriority();
  }

  /**
   * Method which returns this messages'  id
   *
   * @return The id of this message
   */
  public RingId getId() {
    return id;
  }

  /**
   * Method which returns this messages' internal message
   *
   * @return The internal message of this message
   */
  public RawMessage getRawMessage() {
    return message;
  }
  
  public Message getMessage() {
    if (message.getType() == 0) {
      return ((JavaSerializedMessage)message).getMessage(); 
    }
    return message;
  }

  /**
   * Method which returns this messages' applicaiton name
   *
   * @return The application name of this message
   */
  public String getApplication() {
    return application;
  }

  public short getType() {
    return TYPE;
  }

  public void serialize(OutputBuffer buf) throws IOException {
    id.serialize(buf);
    buf.writeUTF(application);
    buf.writeShort(message.getType());
    
    // range check priority
    int priority = message.getPriority();
    if (priority > Byte.MAX_VALUE) throw new IllegalStateException("Priority must be in the range of "+Byte.MIN_VALUE+" to "+Byte.MAX_VALUE+".  Lower values are higher priority. Priority of "+message+" was "+priority+".");
    if (priority < Byte.MIN_VALUE) throw new IllegalStateException("Priority must be in the range of "+Byte.MIN_VALUE+" to "+Byte.MAX_VALUE+".  Lower values are higher priority. Priority of "+message+" was "+priority+".");
    buf.writeByte((byte)priority);

    message.serialize(buf);
  }
  
  /**
   * TODO: This can probably be done more efficiently, IE, deserialize the message on getMessage().  Can do that later.
   * @param buf
   * @param endpoint
   * @param md
   * @param sender
   * @param priority
   * @throws IOException
   */
  public RingMessage(InputBuffer buf, Endpoint ringEndpoint, Hashtable<String, Endpoint> endpoints) throws IOException {
    id = new RingId(buf, ringEndpoint);
    application = buf.readUTF();

    // this code finds the proper deserializer
    Endpoint endpoint = (Endpoint)endpoints.get(application);
    if (endpoint == null) {
      throw new IOException("Couldn't find application:"+application); 
    }
    MessageDeserializer md = endpoint.getDeserializer();
    
    short type = buf.readShort();
    byte priority = buf.readByte();
    // Jeff - 3/31/06 not sure if this is the correct decision, but I don't know how to get the sender
//    NodeHandle sender = endpoint.readNodeHandle(buf);
    Message m = md.deserialize(buf, type, priority, null);
    if (type == 0) {
      message = new JavaSerializedMessage(m);
    } else {
      message = (RawMessage)m; 
    }
  }
  
}

