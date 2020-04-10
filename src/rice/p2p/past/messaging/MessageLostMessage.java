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

package rice.p2p.past.messaging;

import java.io.IOException;

import rice.*;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.past.*;
import rice.p2p.util.rawserialization.JavaSerializedMessage;

/**
 * @(#) MessageLostMessage.java
 *
 * This class represents a reminder to Past that an outstanding message exists,
 * and that the waiting continuation should be informed if the message is lost.
 *
 * @version $Id: MessageLostMessage.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
public class MessageLostMessage extends PastMessage {
  public static final short TYPE = 7;

  private static final long serialVersionUID = -8664827144233122095L;

  // the id the message was sent to
  protected Id id;
  
  // the hint the message was sent to
  protected NodeHandle hint;
  
  // the message
  protected String messageString;

  /**
   * Constructor which takes a unique integer Id and the local id
   *
   * @param uid The unique id
   * @param local The local nodehandle
   */
  public MessageLostMessage(int uid, NodeHandle local, Id id, Message message, NodeHandle hint) {
    super(uid, local, local.getId());

    setResponse();
    this.hint = hint;
    if (message != null) {
      this.messageString = message.toString();
    } else {
      this.messageString = "";
    }
    this.id = id;
  }

  /**
   * Method by which this message is supposed to return it's response -
   * in this case, it lets the continuation know that a the message was
   * lost via the receiveException method.
   *
   * @param c The continuation to return the reponse to.
   */
  @SuppressWarnings("unchecked")
  public void returnResponse(Continuation c, Environment env, String instance) {
    Logger logger = env.getLogManager().getLogger(getClass(), instance);
    Exception e = new PastException("Outgoing message '" + messageString + "' to " + id + "/" + hint + " was lost - please try again.");
    if (logger.level <= Logger.WARNING) logger.logException("ERROR: Outgoing PAST message " + messageString + " with UID " + getUID() + " was lost", e);
    c.receiveException(e);
  }

  /**
  * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  public String toString() {
    return "[MessageLostMessage]";
  }

  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    throw new RuntimeException("serialize() not supported in MessageLostMessage"); 
    
//    super.serialize(buf); 
//    
//    if (hint == null) {
//      buf.writeBoolean(false); 
//    } else {
//      buf.writeBoolean(true); 
//      hint.serialize(buf);
//    }
//    
//    if (id == null) {
//      buf.writeBoolean(false); 
//    } else {
//      buf.writeBoolean(true); 
//      buf.writeShort(id.getType());
//      id.serialize(buf);
//    }
//    
//    buf.writeUTF(messageString);
  }

//  public MessageLostMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
//    super(buf, endpoint);
//
//    if (buf.readBoolean()) {
//      hint = endpoint.readNodeHandle(buf);
//    }
//    if (buf.readBoolean()) {
//      id = endpoint.readId(buf, buf.readShort());
//    }
//    
//    messageString = buf.readUTF();    
//  }  
}

