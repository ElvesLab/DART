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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.scribe.*;
import rice.p2p.scribe.rawserialization.*;

/**
 * @(#) SubscribeMessage.java The subscribe message.
 *
 * @version $Id: SubscribeMessage.java 3801 2007-07-18 15:04:40Z jeffh $
 * @author Alan Mislove
 */
public class SubscribeMessage extends AnycastMessage {
  public static final short TYPE = 2;

  /**
   * The original subscriber
   */
  protected NodeHandle subscriber;

  /**
   * The id of this message
   */
  protected int id;

  /**
   * You can now subscribe to a bunch of Topics at the same time.
   * 
   * This list must be sorted.
   */
  List<Topic> topics;
  
  /**
   * Constructor which takes a unique integer Id
   *
   * @param source The source address
   * @param topics The topics must be sorted, this is not verified.  It will be routed to the first
   * topic in the list.
   * @param id The UID for this message
   * @param content The content
   */
  public SubscribeMessage(NodeHandle source, List<Topic> topics, int id, RawScribeContent content) {
    super(source, topics.iterator().next(), content);
    this.id = id;
    this.subscriber = source;
    this.topics = new ArrayList<Topic>(topics);
  }

  /**
   * 
   * @param source
   * @param topic to subscribe to only 1 topic
   * @param id
   * @param content
   */
  public SubscribeMessage(NodeHandle source, Topic topic, int id, RawScribeContent content) {
    this(source, Collections.singletonList(topic), id, content);
  }
  
  /**
   * Returns the node who is trying to subscribe
   *
   * @return The node who is attempting to subscribe
   */
  public NodeHandle getSubscriber() {
    return subscriber;
  }

  /**
   * Returns this subscribe lost message's id
   *
   * @return The id of this subscribe message
   */
  public int getId() {
    return id;
  }

  /**
   * Returns a String represneting this message
   *
   * @return A String of this message
   */
  public String toString() {
    return "[SubscribeMessage{"+System.identityHashCode(this)+"} " + topic + " subscriber " + subscriber + " ID " + id + "]";
  }

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
//    System.out.println("SubscribeMessage.serialize()");
    buf.writeByte((byte)1); // version
    super.serializeHelper(buf);
    
    buf.writeInt(id);
    subscriber.serialize(buf);
    
    int numTopics = topics.size(); 
    buf.writeInt(numTopics);
    if (numTopics > 1) {
      Iterator<Topic> i = topics.iterator();
      
      // for efficiency, don't serialize the first one
      // we can get it from the super class
      i.next();
      
      while (i.hasNext()) {
        i.next().serialize(buf); 
      }
    }
  }
  
  public static SubscribeMessage buildSM(InputBuffer buf, Endpoint endpoint, ScribeContentDeserializer scd) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        // TODO: Handle this
        
        return null;
      case 1:
        return new SubscribeMessage(buf, endpoint, scd);
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
  
  /**
   * Private because it should only be called from build(), if you need to extend this,
   * make sure to build a serializeHelper() like in AnycastMessage/SubscribeMessage, and properly handle the 
   * version number.
   */
  private SubscribeMessage(InputBuffer buf, Endpoint endpoint, ScribeContentDeserializer contentDeserializer) throws IOException {
    super(buf, endpoint, contentDeserializer);
    id = buf.readInt();
    subscriber = endpoint.readNodeHandle(buf);
    
    
    int numTopics = buf.readInt();
    topics = new ArrayList<Topic>(numTopics);
    
    // the first topic is the super's topic
    topics.add(getTopic());
    
    // note: make sure to start at 1 so we don't overflow
    for (int i = 1; i < numTopics; i++) {
      topics.add(new Topic(buf, endpoint));
    }
    
  }

  /**
   * Call this when you accept topics in the list.
   * 
   * @param topic
   * @return the remaining topics
   */
  public void removeTopics(Collection<Topic> accepted) {
//    ArrayList<Topic> ret = new ArrayList<Topic>();
    topics.removeAll(accepted);
       
    if (topics.isEmpty()) {
      topic = null;
    } else {
      topic = topics.get(0);
    }
//    System.out.println(this+".removeTopics("+accepted+")");
  }

  
  
  public List<Topic> getTopics() {
    return topics;
  }

  public boolean isEmpty() {
    return topics.isEmpty();
  } 
  
  /**
   * Copies everything except changes the topics to the new list
   * 
   * @param newTopics
   * @return
   */
  public SubscribeMessage copy(List<Topic> newTopics, RawScribeContent content) {
    SubscribeMessage ret = new SubscribeMessage(getSource(), newTopics, getId(), content);
    ret.visited = visited;
    ret.toVisit = toVisit;
    ret.subscriber = subscriber;
    return ret;
  }
}

