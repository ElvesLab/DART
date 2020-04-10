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
import java.util.List;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.scribe.*;
import rice.p2p.scribe.rawserialization.ScribeContentDeserializer;

/**
 * @(#) AbstractSubscribeMessage.java
 *
 * The ack for a subscribe message.
 *
 * @version $Id: AbstractSubscribeMessage.java 3672 2007-04-12 10:46:09Z jeffh $
 *
 * @author Alan Mislove
 */
public abstract class AbstractSubscribeMessage implements RawMessage /*extends ScribeMessage*/ {

  /**
  * The id of this subscribe message
   */
  protected int id;
  
  // the source of this message
  protected NodeHandle source;
  
  /**
   * You can now subscribe to a bunch of Topics at the same time.
   * 
   * This list must be sorted.
   * 
   * note: NOT AUTOMATICALLY SERIALIZED!!!
   */
  protected List<Topic> topics;
  

  /**
  * Constructor which takes a unique integer Id
   *
   * @param id The unique id
   * @param source The source address
   * @param dest The destination address
   */
  public AbstractSubscribeMessage(NodeHandle source, List<Topic> topics, int id) {
    this.source = source;
    this.id = id;
    this.topics = topics;
  }

  /**
    * Returns this subscribe lost message's id
   *
   * @return The id of this subscribe lost message
   */
  public int getId() {
    return id;
  }
  
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
  

  public List<Topic> getTopics() {
    return topics;
  }
  
  public String toString() {
    String s = source + ","+id;
    if (topics.size() <= 3) {
      for (Topic topic : topics) {
        s+=" "+topic; 
      }
    } else {
      s+=" numTopics:"+topics.size(); 
    }
    return s;
  }

  /**
   * Protected because it should only be called from an extending class, to get version
   * numbers correct.
   */
  protected AbstractSubscribeMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
    source = endpoint.readNodeHandle(buf);
    id = buf.readInt();    
  }

  public void serialize(OutputBuffer buf) throws IOException {
    source.serialize(buf);
    buf.writeInt(id);
  }  
}

