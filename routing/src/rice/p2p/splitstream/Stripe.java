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

package rice.p2p.splitstream;

import java.io.*;
import java.util.*;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.*;
import rice.p2p.scribe.*;
import rice.selector.TimerTask;

/**
 * This class encapsulates all data about an individual stripe. It is the basic unit in the system.
 * It is rooted at the stripeId for the stripe. It is through the stripe that data is sent. It can
 * be subscribed to in which case data is recieved or it can be unsubscribed from. A stripe can have
 * some number of children and controlling this is the way that bandwidth is controlled. If a stripe
 * is dropped then it searches for a new parent. 
 *
 * @version $Id: Stripe.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Ansley Post
 * @author Alan Mislove
 * @author Atul Singh
 */
public class Stripe implements ScribeClient {

  /**
   * The maximum number of failed subscriptions
   */
  public final int MAX_FAILED_SUBSCRIPTION;// = 5;

  /**
   * The stripeId for this stripe
   */
  protected StripeId stripeId;

  /**
   * The topic corresponding to this stripeId
   */
  protected Topic topic;

  /**
   * The scribe object
   */
  protected Scribe scribe;

  /**
   * A flag whether or not this stripe is the primary stripe for this node
   */
  protected boolean isPrimary;

  /**
   * The list of SplitStreamClients interested in data from this client
   */
  protected Vector<SplitStreamClient> clients;

  /**
   * This stripe's channel
   */
  protected Channel channel;

  /**
   * The count of failed subscribe messages
   */
  protected Hashtable<Topic, Integer> failed;

  protected String instance;
  
  Logger logger;
  
  /**
   * The constructor used when creating a stripe from scratch.
   *
   * @param stripeId the stripeId that this stripe will be rooted at
   * @param scribe the scribe the stripe is running on top of
   */
  public Stripe(StripeId stripeId, Scribe scribe, String instance, Channel channel, int maxFailedSubscriptions) {
    this.instance = instance;
    this.MAX_FAILED_SUBSCRIPTION = maxFailedSubscriptions;
    this.stripeId = stripeId;
    this.scribe = scribe;
    logger = scribe.getEnvironment().getLogManager().getLogger(Stripe.class, instance);    
    this.channel = channel;
    this.isPrimary = false;
    this.failed = new Hashtable<Topic, Integer>();
    if(SplitStreamScribePolicy.getPrefixMatch(this.channel.getLocalId(), stripeId.getId(), channel.getStripeBase()) > 0)
      this.isPrimary = true;
    
    this.clients = new Vector();
    this.topic = new Topic(stripeId.getId());
  }

  /**
   * gets the StripeID for this stripe
   *
   * @return theStripeID
   */
  public StripeId getStripeId() {
    return stripeId;
  }

  /**
   * Returns whether or not this stripe is the primary stripe for the local node
   *
   * @return Whether or not this stripe is primary
   */
  public boolean isPrimary() {
    return isPrimary;
  }

  /**
   * get the state of the Stripe
   *
   * @return the State the stripe is in
   */
  public boolean isSubscribed() {
    return (clients.size() != 0);
  }

  /**
   * Adds a client to this stripe - the client will be informed whenever data arrives for this stripe
   *
   * @param client The client to add
   */
  public void subscribe(SplitStreamClient client) {
    if (! clients.contains(client)) {
      if (clients.size() == 0) {
        scribe.subscribe(topic, this);
      }

      clients.add(client);
    }
  }

  /**
   * Removes a client from this stripe - the client will no longer be informed whenever data arrives for this stripe
   *
   * @param client The client to remove
   */
  public void unsubscribe(SplitStreamClient client) {
    if (clients.contains(client)) {
      clients.remove(client);

      if (clients.size() == 0) {
        scribe.unsubscribe(topic, this);
      }
    }
  }

  /**
   * Publishes the given data to this stripe
   *
   * @param data The data to publish
   */
  public void publish(byte[] data) {
    scribe.publish(topic, new SplitStreamContent(data)); 
  }

  /**
   * This method is invoked when an anycast is received for a topic which this client is interested
   * in. The client should return whether or not the anycast should continue.
   *
   * @param topic The topic the message was anycasted to
   * @param content The content which was anycasted
   * @return Whether or not the anycast should continue
   */
  public boolean anycast(Topic topic, ScribeContent content) {
      return false;
  }

  /**
   * This method is invoked when a message is delivered for a topic this client is interested in.
   *
   * @param topic The topic the message was published to
   * @param content The content which was published
   */
  public void deliver(Topic topic, ScribeContent content) {
    if (this.topic.equals(topic)) {
      if (content instanceof SplitStreamContent) {
        byte[] data = ((SplitStreamContent) content).getData();

        SplitStreamClient[] clients = (SplitStreamClient[]) this.clients.toArray(new SplitStreamClient[0]);

        for (int i=0; i<clients.length; i++) {
          clients[i].deliver(this, data);
        }
      } else {
        if (logger.level <= Logger.WARNING) logger.log("Received unexpected content " + content);
      }
    } else {
      if (logger.level <= Logger.WARNING) logger.log("Received update for unexcpected topic " + topic + " content " + content);
    }
  }

  /**
   * Informs this client that a child was added to a topic in which it was interested in.
   *
   * @param topic The topic to unsubscribe from
   * @param child The child that was added
   */
  public void childAdded(Topic topic, NodeHandle child) {
    if (logger.level <= Logger.FINE) logger.log("childAdded("+topic+","+child+")");
  }

  /**
   * Informs this client that a child was removed from a topic in which it was interested in.
   *
   * @param topic The topic to unsubscribe from
   * @param child The child that was removed
   */
  public void childRemoved(Topic topic, NodeHandle child) {
    if (logger.level <= Logger.FINE) logger.log("childRemoved("+topic+","+child+")");
  }


  /**
   * Informs this client that a subscription failed
   *
   * @param topic The topic that failed
   */
  public void subscribeFailed(final Topic topic) {
    Integer count = (Integer) failed.get(topic);

    if (count == null) {
      count = new Integer(0);
    }

    if (count.intValue() < MAX_FAILED_SUBSCRIPTION) {
      count = new Integer(count.intValue() + 1);

      if (logger.level <= Logger.WARNING) logger.log( 
          "DEBUG :: Subscription failed at " + channel.getLocalId() + " for topic " + topic + " - retrying.");
      scribe.subscribe(topic, this);

      failed.put(topic, count);
    } else {
      // TODO: Make this random exponential backoff!
      // set random exponential backoff timer
      // if there is a parent then cancel self, else 
      
      // TODO: Need to reset count, (and in the future, the exponential backoff count)
      TimerTask resubscribeTask = new TimerTask() {
        public void run() {
          if (getParent() == null) {
            scribe.subscribe(topic, Stripe.this);         
          }
        }
      };
      scribe.getEnvironment().getSelectorManager().getTimer().schedule(resubscribeTask, 
          scribe.getEnvironment().getParameters().getInt("p2p_splitStream_stripe_max_failed_subscription_retry_delay"));
    }
  }

  /**
    * Returns a String representation of this Stripe
   *
   * @return A String representing this stripe
   */
  public String toString() {
    return "Stripe " + stripeId;
  }

  /**
   * Utility method. Returns the list of children for this stripe.
   * @return A array of children.
   */
  public NodeHandle[] getChildren(){
    return this.scribe.getChildren(new Topic(this.getStripeId().getId()));
  }

  /**
   * Utility method. Returns the parent for this topic in the scribe tree.
   * @return Parent for this topic.
   */
  public NodeHandle getParent(){
    return ((ScribeImpl) this.scribe).getParent(new Topic(this.getStripeId().getId()));
  }

  /**
   * Utility method. Checks if local node is root for this topic.
   * @return True/False depending on if local node is root for this topic
   */
  public boolean isRoot(){
    return ((ScribeImpl) this.scribe).isRoot(topic);
  }
}

