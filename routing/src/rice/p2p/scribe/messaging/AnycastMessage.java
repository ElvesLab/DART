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
import java.util.*;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.scribe.*;
import rice.p2p.scribe.rawserialization.*;

/**
 * @(#) AnycastMessage.java The anycast message.
 *
 * @version $Id: AnycastMessage.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 */
public class AnycastMessage extends ScribeMessage {

  public static final short TYPE = 1;

  /**
   * the content of this message
   */
  protected RawScribeContent content;

  /**
   * the list of nodes which we have visited
   */
  protected ArrayList<NodeHandle> visited;

  /**
   * the list of nodes which we are going to visit
   */
  protected LinkedList<NodeHandle> toVisit;
  
  protected NodeHandle initialRequestor;
  
  /**
   * Constructor which takes a unique integer Id
   *
   * @param source The source address
   * @param topic The topic
   * @param content The content
   */
  public AnycastMessage(NodeHandle source, Topic topic, ScribeContent content) {
    this(source, topic, content instanceof RawScribeContent ? (RawScribeContent)content : new JavaSerializedScribeContent(content));
  }
  
  public AnycastMessage(NodeHandle source, Topic topic, RawScribeContent content) {
    super(source, topic);
    this.initialRequestor = source;
//    if (content == null) throw new IllegalArgumentException
    this.content = content;
    this.visited = new ArrayList<NodeHandle>();
    this.toVisit = new LinkedList<NodeHandle>();

    addVisited(source);
  }

  /**
   * Returns the content
   *
   * @return The content
   */
  public ScribeContent getContent() {
    if (content == null) return null; 
    if (content.getType() == 0) return ((JavaSerializedScribeContent)content).getContent();
    return content;
  }
  
  public RawScribeContent getRawContent() {
    return content; 
  }

  /**
   * Sets the content
   *
   * @param content The content
   */
  public void setContent(RawScribeContent content) {
    this.content = content;
  }

  public void setContent(ScribeContent content) {
    if (content instanceof RawScribeContent) {
      setContent(content); 
    } else {
      setContent(new JavaSerializedScribeContent(content));
    }
  }

  /**
   * Returns the next handle to visit
   *
   * @return The next handle to visit
   */
  public NodeHandle peekNext() {
    if (toVisit.size() == 0) {
      return null;
    }

    return (NodeHandle) toVisit.getFirst();
  }

  /**
   * Returns the next handle to visit and removes the
   * node from the list.
   *
   * @return The next handle to visit
   */
  public NodeHandle getNext() {
    if (toVisit.size() == 0) {
      return null;
    }
    
    return (NodeHandle) toVisit.removeFirst();
  }
  
  public NodeHandle peekLastToVisit() {
    return toVisit.getLast();
  }

  /**
   * Adds a node to the visited list
   *
   * @param handle The node to add
   */
  public void addVisited(NodeHandle handle) {
//    if (!(this instanceof SubscribeMessage)) System.out.println(this+".addVisited("+handle+")");
    if (handle == null) {
      return;
    }

    if (!visited.contains(handle)) {
      visited.add(handle);
    }

    while (toVisit.remove(handle)) {
      toVisit.remove(handle);
    }
  }

  /**
   * Adds a node the the front of the to-visit list
   *
   * @param handle The handle to add
   */
  public void addFirst(NodeHandle handle) {
//    if (!(this instanceof SubscribeMessage)) System.out.println(this+".addFirst("+handle+")");

    if (handle == null) {
      return;
    }

    if ((!toVisit.contains(handle)) && (!visited.contains(handle))) {
      toVisit.addFirst(handle);
    }
  }

  /**
   * Adds a node the the end of the to-visit list
   *
   * @param handle The handle to add
   */
  public void addLast(NodeHandle handle) {
//    if (!(this instanceof SubscribeMessage)) System.out.println(this+".addLast("+handle+")");
    
    if (handle == null) {
      return;
    }

    if ((!toVisit.contains(handle)) && (!visited.contains(handle))) {
      toVisit.addLast(handle);
    }
  }

  public String toString() {
    return "Anycast["+getTopic()+" "+content+"]";
  }
  
  public NodeHandle getInitialRequestor() {
    return initialRequestor;
  }

  public NodeHandle getLastVisited() {
    if (visited.size() == 0) {
      return null;
    } else {
      return visited.get(visited.size()-1);
    }
  }
  
  /**
   * Removes the node handle from the to visit and visited lists
   *
   * @param handle The handle to remove
   */
  public void remove(NodeHandle handle) {
    if (handle == null) {
      return;
    }

    toVisit.remove(handle);
    visited.remove(handle);
  }
  
  public int getVisitedSize() {
    return visited.size();
  }

  // This method will be called on the intermediate hops to check if the list is
  // already big, in which case it will not append to this list
  public int getToVisitSize() {
    return toVisit.size();
  }
  
  public boolean hasVisited(NodeHandle handle) {
    return visited.contains(handle);
  }
  
  /** *************** Raw Serialization ************************************** */
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
//  super.serialize(buf); // note, can't make this a hierarchy, because of the super() must be first line rule in java
    buf.writeByte((byte)1); // version
    serializeHelper(buf);
    
  }
  
  /**
   * Use this to allow SubscribeMessage to extend this, but not have the version number
   * nor the content.
   */
  protected void serializeHelper(OutputBuffer buf) throws IOException {    
    super.serialize(buf);
    
    initialRequestor.serialize(buf);
    
    buf.writeInt(toVisit.size());
    Iterator<NodeHandle> i = toVisit.iterator();
    while(i.hasNext()) {
      ((NodeHandle)i.next()).serialize(buf); 
    }
    
    buf.writeInt(visited.size());
    i = visited.iterator();
    while(i.hasNext()) {
      ((NodeHandle)i.next()).serialize(buf); 
    }
    
    if (content == null) {
      buf.writeBoolean(false);
    } else {
      buf.writeBoolean(true);
      buf.writeShort(content.getType());
      content.serialize(buf);
    }    
  }
  
  public static AnycastMessage build(InputBuffer buf, Endpoint endpoint, ScribeContentDeserializer scd) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 1:
        return new AnycastMessage(buf, endpoint, scd);
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
  
  /**
   * Protected because it should only be called from an extending class, to get version
   * numbers correct.
   * 
   */
  protected AnycastMessage(InputBuffer buf, Endpoint endpoint, ScribeContentDeserializer cd) throws IOException {
    super(buf, endpoint);

    initialRequestor = endpoint.readNodeHandle(buf);
    
    toVisit = new LinkedList<NodeHandle>();
    int toVisitLength = buf.readInt();
    for (int i = 0; i < toVisitLength; i++) {
      toVisit.addLast(endpoint.readNodeHandle(buf)); 
    }
    
    int visitedLength = buf.readInt();
    visited = new ArrayList<NodeHandle>(visitedLength);
    for (int i = 0; i < visitedLength; i++) {
      visited.add(endpoint.readNodeHandle(buf)); 
    }
    
    // this can be done lazilly to be more efficient, must cache remaining bits, endpoint, cd, and implement own InputBuffer
    if (buf.readBoolean()) {
      short contentType = buf.readShort();
      if (contentType == 0) {
        content = new JavaSerializedScribeContent(cd.deserializeScribeContent(buf, endpoint, contentType));
      } else {
        content = (RawScribeContent)cd.deserializeScribeContent(buf, endpoint, contentType); 
      }
    }  
  }

}

