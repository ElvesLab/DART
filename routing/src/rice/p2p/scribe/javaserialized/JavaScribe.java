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

package rice.p2p.scribe.javaserialized;

import java.util.Collection;

import rice.p2p.commonapi.NodeHandle;
import rice.p2p.scribe.BaseScribe;
import rice.p2p.scribe.ScribeClient;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.ScribeMultiClient;
import rice.p2p.scribe.Topic;

/**
 * Scribe that uses JavaSerialization for the ScribeContent
 * 
 * @author Jeff Hoye
 *
 */
public interface JavaScribe extends BaseScribe {
  // ***************** Membership functions messages **************
  /**
   * Subscribes the given client to the provided topic.  Any message published
   * to the topic will be delivered to the Client via the deliver() method.
   *
   * @param topic The topic to subscribe to
   * @param client The client to give messages to
   * @param content The content to include in the subscribe
   * @deprecated use subscribe(Topic, ScribeMultiClient, ScribeContent, NodeHandle)
   */
  public void subscribe(Topic topic, ScribeClient client, ScribeContent content);

  /**
   * Subscribes the given client to the provided topic.  Any message published
   * to the topic will be delivered to the Client via the deliver() method.
   *
   * @param topic The topic to subscribe to
   * @param client The client to give messages to
   * @param content The content to include in the subscribe
   * @param hint The first hop of the message ( Helpful to implement a centralized solution)
   * @deprecated use the version with the MultiClient
   */
  public void subscribe(Topic topic, ScribeClient client, ScribeContent content, NodeHandle hint);
  public void subscribe(Topic topic, ScribeMultiClient client, ScribeContent content, NodeHandle hint);

  /**
   * Subscribe to multiple topics.
   * 
   * @param topics
   * @param client
   * @param content
   * @param hint
   * @deprecated use the version with the MultiClient
   */
  public void subscribe(Collection<Topic> topics, ScribeClient client, ScribeContent content, NodeHandle hint);
  public void subscribe(Collection<Topic> topics, ScribeMultiClient client, ScribeContent content, NodeHandle hint);

  // ***************** Messaging functions ****************
  /**
   * Publishes the given message to the topic.
   *
   * @param topic The topic to publish to
   * @param content The content to publish
   */
  public void publish(Topic topic, ScribeContent content);

  /**
   * Anycasts the given content to a member of the given topic
   *
   * @param topic The topic to anycast to
   * @param content The content to anycast
   */
  public void anycast(Topic topic, ScribeContent content);

  /**
   * Anycasts the given content to a member of the given topic
   * 
   * The hint helps us to implement centralized algorithms where the hint is the 
   * cachedRoot for the topic. Additionally it enables us to do more fancy 
   * anycasts that explore more portions of the Scribe tree
   *
   * @param topic The topic to anycast to
   * @param content The content to anycast
   * @param hint the first hop of the Anycast
   */
  public void anycast(Topic topic, ScribeContent content, NodeHandle hint);

}
