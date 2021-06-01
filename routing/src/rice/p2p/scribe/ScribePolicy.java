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

package rice.p2p.scribe;

import java.util.*;

import rice.environment.Environment;
import rice.p2p.commonapi.*;
import rice.p2p.scribe.messaging.*;
import rice.p2p.util.TimerWeakHashMap;

/**
 * @(#) ScribePolicy.java This interface represents a policy for Scribe, which is asked whenever a
 * child is about to be added or removed, or when the the local node is about to be implicitly
 * subscribed to a new topic.
 *
 * @version $Id: ScribePolicy.java 4098 2008-02-13 09:36:33Z jeffh $
 * @author Alan Mislove
 */
public interface ScribePolicy {

  /**
   * This method is called when the newChild is about to become our child, and the policy should
   * return whether or not the child should be allowed to become our child. If the length of
   * children and clients is both 0, allowing the child to join will have the effect of implicitly
   * subscribing this node the the given topic.
   *
   * For each Topic that you are willing to accept, call message.accept(Topic);
   * 
   * Here is some example code:
   * 
   * <pre>
   * Iterator<Topic> i = topics.iterator();
   * while (i.hasNext()) {
   *   Topic topic = i.next();
   *   if (!accept(topic)) { // your decision on what to do for the topic
   *     i.remove();
   *   }
   * }
   * return topics;
   * </pre>
   * 
   * Or, to accept all:
   * <pre>
   * return topics;
   * </pre>
   *
   * Some calls that are likely useful are: 
   * <ul> 
   *   <li>scribe.getChildren(topic)</li>
   *   <li>scribe.getClients(topic)</li>
   * </ul>
   *
   * If only some of the topics are accepted, the content may need to be modified.  
   * This method <i>is</i> allowed to modify the content.
   *
   * @param scribe the Scribe that is making the request
   * @param source the subscriber
   * @param topics the topics that are requested
   * @param content the content that came with the message
   * @param return the list that is accepted
   */
  public List<Topic> allowSubscribe(Scribe scribe, NodeHandle source, List<Topic> topics, ScribeContent content);

  /**
   * This method is called when an anycast is received which is not satisfied at the local node.
   * This method should add both the parent and child nodes to the anycast's to-search list, but
   * this method allows different policies concerning the order of the adding as well as selectively
   * adding nodes.
   *
   * @param message The anycast message in question
   * @param parent Our current parent for this message's topic
   * @param children Our current children for this message's topic
   */
  public void directAnycast(AnycastMessage message, NodeHandle parent, Collection<NodeHandle> children);
  
  /**
   * Informs this policy that a child was added to a topic - the topic is free to ignore this
   * upcall if it doesn't care.
   *
   * @param topic The topic to unsubscribe from
   * @param child The child that was added
   */
  public void childAdded(Topic topic, NodeHandle child);
  
  /**
   * Informs this policy that a child was removed from a topic - the topic is free to ignore this
   * upcall if it doesn't care.
   *
   * @param topic The topic to unsubscribe from
   * @param child The child that was removed
   */
  public void childRemoved(Topic topic, NodeHandle child);
  
  /**
   * This notifies us when we receive a failure for a anycast
   */
  public void recvAnycastFail(Topic topic, NodeHandle failedAtNode, ScribeContent content);

  /**
   * This is invoked whenever this message arrives on any overlay node, this gives the ScribeClient's power
   * to tap into some datastructures they might wanna edit
   */
  public void intermediateNode(ScribeMessage message);

  /**
   * This method is called when the ScribeImpl splits a SubscribeMessage into multiple parts.  
   * 
   * If you modify the content, you must make a copy, as the same content will be passed in for the other
   * divisions of the SubscribeMessage.  ScribeContent's are not naturally copyable, so Scribe cannot 
   * make a copy apriori.
   * 
   * @param theTopics topics going to a particular location
   * @param content the content that may need to be divided
   * @return the content if not changed, a new ScribeContent if changed
   */
  public ScribeContent divideContent(List<Topic> theTopics, ScribeContent content);

  /**
   * The default policy for Scribe, which always allows new children to join and adds children in
   * the order in which they are provided, implicitly providing a depth-first search.
   *
   * @version $Id: ScribePolicy.java 4098 2008-02-13 09:36:33Z jeffh $
   * @author amislove
   */
  public static class DefaultScribePolicy implements ScribePolicy {
    protected Environment environment;
    public DefaultScribePolicy(Environment env) {
      environment = env;
    }
    /**
     * If you don't override the deprecated allowSubscribe(), This method always return true;
     *
     * @param message The subscribe message in question
     * @param children The list of children who are currently subscribed
     * @param clients The list of clients are are currently subscribed
     * @return the topics to accept
     */
    public List<Topic> allowSubscribe(Scribe scribe, NodeHandle source, List<Topic> topics, ScribeContent content) {
      Iterator<Topic> i = topics.iterator();
      while(i.hasNext()) {
        Topic topic = i.next();        
        if (!allowSubscribe(
            new BogusSubscribeMessage(source, topic, 0, content),
            scribe.getClients(topic).toArray(new ScribeClient[0]), 
            scribe.getChildrenOfTopic(topic).toArray(new NodeHandle[0]))) {
          i.remove();
        }
      }
      return topics;
    }

    class BogusSubscribeMessage extends SubscribeMessage {
      ScribeContent theContent;
      
      public BogusSubscribeMessage(NodeHandle source, Topic topic, int i, ScribeContent content) {
        super(source, topic, i, null);
        theContent = content;
      }
      
      
      public ScribeContent getContent() {
        return theContent;
      }
    }
    
    /**
     * This method should be deprecated, but is here for reverse compatibility.  We changed the 
     * allowSubscribe() method, but classes that extend DefaultScribePolicy object could make a mistake
     * and not override the new one.
     * 
     * @param message
     * @param clients
     * @param children
     * @return
     */
    public boolean allowSubscribe(SubscribeMessage message, ScribeClient[] clients, NodeHandle[] children) {
      return true;
    }
    
    /**
     * Simply adds the parent and children in order, which implements a depth-first-search.
     *
     * randomly picks a child
     *
     * @param message The anycast message in question
     * @param parent Our current parent for this message's topic
     * @param children Our current children for this message's topic
     */
    public void directAnycast(AnycastMessage message, NodeHandle parent, Collection<NodeHandle> theChildren) {
      if (parent != null) {
        message.addLast(parent);
      }
      
      ArrayList<NodeHandle> children = new ArrayList<NodeHandle>(theChildren);
      
      // now randomize the children list
      while (!children.isEmpty()) {
        message.addFirst(
            children.remove(
                environment.getRandomSource().nextInt(
                    children.size())));
      }
    }
    
    
    /**
     * Informs this policy that a child was added to a topic - the topic is free to ignore this
     * upcall if it doesn't care.
     *
     * @param topic The topic to unsubscribe from
     * @param child The child that was added
     */
    public void childAdded(Topic topic, NodeHandle child) {
    }
    
    /**
     * Informs this policy that a child was removed from a topic - the topic is free to ignore this
     * upcall if it doesn't care.
     *
     * @param topic The topic to unsubscribe from
     * @param child The child that was removed
     */
    public void childRemoved(Topic topic, NodeHandle child) {
    }
    public void intermediateNode(ScribeMessage message) {
    }
    public void recvAnycastFail(Topic topic, NodeHandle failedAtNode, ScribeContent content) {
    }
    public ScribeContent divideContent(List<Topic> theTopics, ScribeContent content) {
      return content;
    }
  }

  /**
   * An optional policy for Scribe, which allows up to a specified number of children per topic.
   *
   * @version $Id: ScribePolicy.java 4098 2008-02-13 09:36:33Z jeffh $
   * @author amislove
   */
  public static class LimitedScribePolicy extends DefaultScribePolicy {

    /**
     * The number of children to allow per topic
     */
    protected int maxChildren;

    /**
     * Construtor which takes a maximum number
     *
     * @param max The maximum number of children
     */
    public LimitedScribePolicy(int max, Environment env) {
      super(env);
      this.maxChildren = max;
    }

    /**
     * This method returns (children.length < maxChildren-1);
     *
     * @param message The subscribe message in question
     * @param children The list of children who are currently subscribed
     * @param clients The list of clients are are currently subscribed
     * @return True.
     */
    public boolean allowSubscribe(SubscribeMessage message, ScribeClient[] clients, NodeHandle[] children) {
      return (children.length < (maxChildren - 1));
    }
  }
  
  /**
   * Nicely departs.
   * 
   * You must notify this policy when you depart, or when a node nicely departs.
   * 
   * @author Jeff Hoye
   *
   */
//  public class NiceDeparturePolicy extends DefaultScribePolicy {
//    /**
//     * These nodes have been marked dead by the application.  Avoid them in Anycasts.
//     */
//    TimerWeakHashMap<NodeHandle, Object> markedDead;
//    
//    /**
//     * Dummy to hold in markedDead.
//     */
//    public static final Object OBJECT = new Object();
//
//    public NiceDeparturePolicy(Environment env) {
//      super(env);
//      markedDead = new TimerWeakHashMap<NodeHandle, Object>(env.getSelectorManager().getTimer(),300000);
//    }
//
//    /**
//     * Call this when you know a neighbor has nicely left, so we won't 
//     * route messags to it any more.
//     * @param neighbor
//     */
//    public void markDead(NodeHandle neighbor) {
//      markedDead.put(neighbor, OBJECT);
//    }
//  public static final int REPLICAS_TO_NOTIFY = 24;
//  /**
//   * endpoint is supposed to fail if we ask for too many replicas... so just try to get as many as possible
//   * @return
//   */
//  protected Iterable<NodeHandle> getReplicaSet() {
//    for (int numReplicas = REPLICAS_TO_NOTIFY; numReplicas > 0; numReplicas--) {
//      try {
//        return endpoint.replicaSet(endpoint.getId(), numReplicas);         
//      } catch (Exception e) {
//        // try again
//      }
//    }
//    return Collections.emptyList();
//  }
//
//  }
}

