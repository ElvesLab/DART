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
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.past.*;
import rice.p2p.past.rawserialization.*;

/**
 * @(#) CacheMessage.java
 *
 * This class represents message which pushes an object forward one hop in order
 * to be cached.
 *
 * @version $Id: CacheMessage.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
@SuppressWarnings("unchecked")
public class CacheMessage extends PastMessage {

  public static final short TYPE = 1;
  
  // the content to be cached
  protected RawPastContent content;
  
  /**
   * Constructor which takes a unique integer Id and the local id
   *
   * @param uid The unique id
   */
  public CacheMessage(int uid, PastContent content, NodeHandle source, Id dest) {
    this(uid, content instanceof RawPastContent ? (RawPastContent)content : new JavaSerializedPastContent(content), source, dest);    
  }
  
  public CacheMessage(int uid, RawPastContent content, NodeHandle source, Id dest) {
    super(uid, source, dest);

    this.content = content;
  }

  /**
   * Method which returns the content
   *
   * @return The content
   */
  public PastContent getContent() {
//  if (content == null) 
    if (content.getType() == 0) return ((JavaSerializedPastContent)content).getContent();
    return content;
  }

  /**
    * Method by which this message is supposed to return it's response.
   *
   * @param c The continuation to return the reponse to.
   */
  public void returnResponse(Continuation c, Environment env, String instance) {
    throw new RuntimeException("ERROR: returnResponse should not be called on cacheMessage!");
  }

  /**
   * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  public String toString() {
    return "[CacheMessage for " + content + "]";
  }
  
  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version
    super.serialize(buf);
    buf.writeShort(content.getType());
    content.serialize(buf);      
  }
  
  public static CacheMessage build(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new CacheMessage(buf, endpoint, pcd);
      default:
        throw new IOException("Unknown Version: "+version);        
    }
  }
  
  private CacheMessage(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
    super(buf, endpoint);
    // this can be done lazilly to be more efficient, must cache remaining bits, endpoint, cd, and implement own InputBuffer
    short contentType = buf.readShort();
    if (contentType == 0) {
      content = new JavaSerializedPastContent(pcd.deserializePastContent(buf, endpoint, contentType));
    } else {
      content = (RawPastContent)pcd.deserializePastContent(buf, endpoint, contentType); 
    }
  }   
  
}

