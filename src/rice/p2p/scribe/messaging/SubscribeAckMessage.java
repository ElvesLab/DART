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

package rice.p2p.scribe.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.scribe.*;
import rice.p2p.scribe.rawserialization.ScribeContentDeserializer;

/**
 * @(#) SubscribeAckMessage.java
 *
 * The ack for a subscribe message.
 * 
 * This message is sent on 2 occaisions.  
 * a) An ack to a subscribe.
 * b) When your pat to the root changes.  
 * 
 * TODO: Consider breaking up these functions.
 *
 * @version $Id: SubscribeAckMessage.java 3816 2007-08-01 17:54:53Z jeffh $
 *
 * @author Alan Mislove
 */
public class SubscribeAckMessage extends AbstractSubscribeMessage {

  public static final short TYPE = 3;
  
  /**
   * The contained path to the root
   */
  protected List<List<Id>> pathsToRoot;

  /**
   * Constructor which takes a unique integer Id
   *
   * @param id The unique id
   * @param source The source address
   * @param dest The destination address
   */
  public SubscribeAckMessage(NodeHandle source, List<Topic> topics, List<List<Id>> pathsToRoot, int id) {
    super(source, topics, id);

    if (topics.size() != pathsToRoot.size()) 
      throw new IllegalArgumentException(
          "Must be a path for each topic.  Topics: "+topics.size()+" Paths:"+pathsToRoot.size());    
    
    this.topics = topics;
    this.pathsToRoot = pathsToRoot;
  }

  /**
   * Returns the path to the root for the node receiving
   * this message
   *
   * @return The new path to the root for the node receiving this
   * message
   */
  public List<List<Id>> getPathsToRoot() {
    return pathsToRoot;
  }
  
  /**
   * Returns a String representation of this ack
   *
   * @return A String
   */
  public String toString() {
    if (topics.size() == 1) return "SubscribeAckMessage{"+topics.get(0)+" ID:"+id+"}";
    return "SubscribeAckMessage{"+topics.size()+" ID:"+id+"}"; 
  }

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)1); // version
    super.serialize(buf);

    int numTopics = topics.size(); 
    buf.writeInt(numTopics);

    
    Iterator<Topic> j = topics.iterator();
    Iterator<List<Id>> i = pathsToRoot.iterator();
      
    while (j.hasNext()) {
      j.next().serialize(buf); 
      List<Id> pathToRoot = i.next();
      buf.writeInt(pathToRoot.size());
      for (Id id : pathToRoot) {
        buf.writeShort(id.getType());
        id.serialize(buf);
      }        
    }
  }
  
  public static SubscribeAckMessage build(InputBuffer buf, Endpoint endpoint) throws IOException { 
    byte version = buf.readByte();
    switch(version) {
      case 0:
        // TODO: Handle this
      
        return null;        
      case 1:        
        return new SubscribeAckMessage(buf, endpoint);
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
  
  /**
   * Private because it should only be called from build(), if you need to extend this,
   * make sure to build a serializeHelper() like in AnycastMessage/SubscribeMessage, and properly handle the 
   * version number.
   */
  private SubscribeAckMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
    super(buf, endpoint);
    
    int numTopics = buf.readInt();
    topics = new ArrayList<Topic>(numTopics);
    pathsToRoot = new ArrayList<List<Id>>(numTopics);
    
    // note: make sure to start at 1 so we don't overflow
    for (int i = 0; i < numTopics; i++) {
      topics.add(new Topic(buf, endpoint));
      
      int length = buf.readInt();
      List<Id> pathToRoot = new ArrayList<Id>(length);
      for (int j = 0; j < length; j++) {
        pathToRoot.add(endpoint.readId(buf, buf.readShort()));
      }      
      pathsToRoot.add(pathToRoot);
    }
  }

}

