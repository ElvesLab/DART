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

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.scribe.*;

/**
 * @(#) ScribeMessage.java
 *
 * This class the abstraction of a message used internally by Scribe.
 *
 * @version $Id: ScribeMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public abstract class ScribeMessage implements RawMessage {
  
  // serialver for backward compatibility
  private static final long serialVersionUID = 4593674882226544604L;

  // the source of this message
  protected NodeHandle source;

  // the topic of this message
  protected Topic topic;

  /**
   * Constructor which takes a unique integer Id
   *
   * @param source The source address
   * @param topic The topic
   */
  protected ScribeMessage(NodeHandle source, Topic topic) {
    this.source = source;
    this.topic = topic;
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
    return MEDIUM_HIGH_PRIORITY;
  }

  /**
   * Method which returns this messages' source address
   *
   * @return The source of this message
   */
  public NodeHandle getSource() {
    return source;
  }
  
  /**
   * Method which set this messages' source address
   *
   * @param source The source of this message
   */
  public void setSource(NodeHandle source) {
    this.source = source;
  }

  /**
   * Method which returns this messages' topic
   *
   * @return The topic of this message
   */
  public Topic getTopic() {
    return topic;
  }
  
  /**
   * Protected because it should only be called from an extending class, to get version
   * numbers correct.
   */
  protected ScribeMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
    if (buf.readBoolean())
      source = endpoint.readNodeHandle(buf);
    topic = new Topic(buf, endpoint);
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    boolean hasSource = (source != null);
    buf.writeBoolean(hasSource);
    if (hasSource)
      source.serialize(buf);
    topic.serialize(buf);
  }
  
}

