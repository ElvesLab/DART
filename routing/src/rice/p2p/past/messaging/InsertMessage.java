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
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.past.*;
import rice.p2p.past.rawserialization.*;

/**
 * @(#) InsertMessage.java
 *
 * This class represents a message which is an insert request in past.
 *
 * @version $Id: InsertMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
public class InsertMessage extends ContinuationMessage {
  public static final short TYPE = 4;

  // serailver for bward compatibility
  static final long serialVersionUID = -7027957470028259605L;
  
  // the data to insert
  protected RawPastContent content;
  
  /**
   * Constructor which takes a unique integer Id, as well as the
   * data to be stored
   *
   * @param uid The unique id
   * @param content The content to be inserted
   * @param source The source address
   * @param dest The destination address
   */
  public InsertMessage(int uid, PastContent content, NodeHandle source, Id dest) {
    this(uid, content instanceof RawPastContent ? (RawPastContent)content : new JavaSerializedPastContent(content), source, dest);
  }
  
  public InsertMessage(int uid, RawPastContent content, NodeHandle source, Id dest) {
    super(uid, source, dest);

    this.content = content;
  }

  /**
   * Method which returns the content
   *
   * @return The contained content
   */
  public PastContent getContent() {
//  if (content == null) 
    if (content.getType() == 0) return ((JavaSerializedPastContent)content).getContent();
    return content;
  }
  
  /**
   * Method which builds a response for this message, using the provided
   * object as a result.
   *
   * @param o The object argument
   */
  public void receiveResult(Object o) {
    super.receiveResult(o);
    content = null;
  }
  
  /**
   * Method which builds a response for this message, using the provided
   * exception, which was thrown
   *
   * @param e The exception argument
   */
  public void receiveException(Exception e) {
    super.receiveException(e);
    content = null;
  }

  /**
    * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  public String toString() {
    return "[InsertMessage for " + content + "]";
  }
  
  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version        
    serializeHelper(buf);
  }
  
  /**
   * So that it can be subclassed without serializing a version here
   * @param buf
   * @throws IOException
   */
  protected void serializeHelper(OutputBuffer buf) throws IOException {
    if (response != null && response instanceof Boolean) {
      super.serialize(buf, false); 
      buf.writeBoolean(((Boolean)response).booleanValue());
    } else {
      super.serialize(buf, true);       
    }
    
    buf.writeBoolean(content != null);
    if (content != null) {
      buf.writeShort(content.getType());
      content.serialize(buf);
    }
  }
  
  public static InsertMessage build(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new InsertMessage(buf, endpoint, pcd);
      default:
        throw new IOException("Unknown Version: "+version);        
    }
  }  
  
  protected InsertMessage(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
    super(buf, endpoint);
    
    if (serType == S_SUB) {
      response = new Boolean(buf.readBoolean());
    }
        
    // this can be done lazilly to be more efficient, must cache remaining bits, endpoint, cd, and implement own InputBuffer
    if (buf.readBoolean()) {
      short contentType = buf.readShort();
      if (contentType == 0) {
        content = new JavaSerializedPastContent(pcd.deserializePastContent(buf, endpoint, contentType));
      } else {
        content = (RawPastContent)pcd.deserializePastContent(buf, endpoint, contentType); 
      }
    }
  }
}

