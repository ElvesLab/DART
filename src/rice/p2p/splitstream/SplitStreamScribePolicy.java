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

import java.util.*;

import rice.p2p.commonapi.*;
import rice.p2p.scribe.*;
import rice.p2p.scribe.ScribePolicy.DefaultScribePolicy;
import rice.p2p.scribe.messaging.*;

import rice.pastry.routing.RoutingTable;

/**
 * This class represents SplitStream's policy for Scribe, which only allows children
 * according to the bandwidth manager and makes anycasts first traverse all nodes
 * who have the stripe in question as their primary stripe, and then the nodes
 * who do not.
 *
 * @version $Id: SplitStreamScribePolicy.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Atul Singh
 */
public class SplitStreamScribePolicy extends DefaultScribePolicy /*implements ScribePolicy*/ {

  /**
   * The default maximum number of children per channel
   */
  public final int DEFAULT_MAXIMUM_CHILDREN;// = 24;

  /**
   * A reference to this policy's splitstream object
   */
  protected SplitStream splitStream;
  
  /**
   * A reference to this policy's scribe object
   */
  protected Scribe scribe;

  /**
   * A mapping from channelId -> maximum children
   */
  protected Hashtable<ChannelId, Integer> policy;

  /**
   * Constructor which takes a splitStream object
   *
   * @param splitStream The local splitstream
   */
  public SplitStreamScribePolicy(Scribe scribe, SplitStream splitStream) {
    super(splitStream.getEnvironment());
    DEFAULT_MAXIMUM_CHILDREN = scribe.getEnvironment().getParameters().getInt("p2p_splitStream_policy_default_maximum_children");
    this.scribe = scribe;
    this.splitStream = splitStream;
    this.policy = new Hashtable<ChannelId, Integer>();
  }

  /**
   * Gets the max bandwidth for a channel.
   *
   * @param id The id to get the max bandwidth of
   * @return The amount of bandwidth used
   */
  public int getMaxChildren(ChannelId id) {
    Integer max = (Integer) policy.get(id);

    if (max == null) {
      return DEFAULT_MAXIMUM_CHILDREN;
    } else {
      return max.intValue();
    }
  }

  /**
   * Adjust the max bandwidth for this channel.
   *
   * @param id The id to get the max bandwidth of
   * @param children The new maximum bandwidth for this channel
   */
  public void setMaxChildren(ChannelId id, int children) {
    policy.put(id, new Integer(children));
  }

  /**
   * This method implements the "locating parent" algorithm of SplitStream. Whenever a node receives 
   * subscription request, it executes following algorithm :
   * 
   * 1) Checks if it has available capacity, 
   *    if yes, take it
   *    if no, go to step 2
   * 
   * 2) If subscription is for primary stripe,
   *    if no, dont take it, return false
   *    if yes, need to drop someone
   *           first check if our children list for this topic is non-zero
   *             if yes, find a child which shares less prefix match than the new subscriber
   *                     if found one, send it a drop message and accept newe subscriber, return true
   *                     if no, dont accept the new subscriber, return false
   *              if no, look for children in other stripes which we can drop
   *                  we select a stripe such that it is not
   *                  a) primary stripe
   *                  b) we are not the root for the stripe (may happen in very small networks)
   *                  c) we have non-zero children for this stripe
   *                  if found such a stripe, we drop a random child from that stripe and accept the new 
   *                     subscriber, return true.
   *
   * 3) Checks if one of our child for a stripe sent us a request message for dropping it and taking
   *    the new subscriber (STAGE_3), then we drop the child and return true to accept the subscriber.    
   *
   *
   * @param message The subscribe message in question
   * @param children The list of children who are currently subscribed to this topic
   * @param clients The list of clients are are currently subscribed to this topic
   * @return Whether or not this child should be allowed add.
   */
//  public boolean allowSubscribe(Scribe scribe, NodeHandle subscriber, Collection<ScribeClient> clients, Collection<NodeHandle> children) {
  public boolean allowSubscribe(SubscribeMessage message, ScribeClient[] clients, NodeHandle[] children) {

    Channel channel = getChannel(message.getTopic());
    NodeHandle newChild = (NodeHandle)message.getSubscriber();

    if (channel == null) {
      // If the channel is null, return false *unless you are root*, in which case you 
      // return true.  This is a weird case.  We have to accept here if we are the root, even 
      // if we are not attached to the channel (it's a bootstrapping issue)
      return scribe.isRoot(message.getTopic());
    }
    
    /* do not accept self - wierd case, should not happen */
    if(message.getSubscriber().getId().equals(channel.getLocalId()))
      return false;

    /* first see if we are in the 3rd stage of algorithm for locating parent. */
    ScribeContent content = message.getContent();

    /* this occurs if we are in the third stage of locating a parent */
    if (content != null && (content instanceof SplitStreamSubscribeContent)) {
      int stage = ((SplitStreamSubscribeContent) content).getStage();
      if (stage == SplitStreamSubscribeContent.STAGE_FINAL) {
        List list = Arrays.asList(children);
        
        if (!list.contains(message.getSource())) {
          return false;
        } else {
          this.scribe.removeChild(message.getTopic(), message.getSource());
          return true;
        }
      }
    }

    /* see if we can accept */
    if (getTotalChildren(channel) < getMaxChildren(channel.getChannelId())) {
      return true;
    } else {
      /* check if non-primary stripe */
      if ((! message.getTopic().getId().equals(channel.getPrimaryStripe().getStripeId().getId())) &&
          (! scribe.isRoot(message.getTopic()))) {
        return false;
      } else {
        /* find a victim child */
        if (children.length > 0) {
          NodeHandle victimChild = freeBandwidth(channel, newChild, message.getTopic().getId());

          /* make sure victim is not subscriber */
          if (victimChild.getId().equals(newChild.getId())) {
            return false;
          } else {
            scribe.removeChild(new Topic(message.getTopic().getId()), victimChild);
            return true;
          }
        } else {
          /* we must accept, because this is primary stripe */
          Vector res = freeBandwidthUltimate(message.getTopic().getId());
          if (res != null) {
            scribe.removeChild(new Topic((Id)res.elementAt(1)), (NodeHandle)res.elementAt(0));
            return true;
          } else {
            return false;
          }
        }
      }
    }
  }

  /**
   * This method adds the parent and child in such a way that the nodes who have this stripe as
   * their primary strpe are examined first.
   *
   * @param message The anycast message in question
   * @param parent Our current parent for this message's topic
   * @param children Our current children for this message's topic
   */
  public void directAnycast(AnycastMessage message, NodeHandle parent, Collection<NodeHandle> children) {
    /* we add parent first if it shares prefix match */
    if (parent != null) {
      if (SplitStreamScribePolicy.getPrefixMatch(message.getTopic().getId(), parent.getId(), splitStream.getStripeBaseBitLength()) > 0)
        message.addFirst(parent);
      else
        message.addLast(parent);
    }

    /* if it's a subscribe */
    if (message instanceof SubscribeMessage) {

      /* First add children which match prefix with the stripe, then those which dont.
         Introduce some randomness so that load is balanced among children. */
      Vector good = new Vector();
      Vector bad = new Vector();

      for (NodeHandle child : children) {
        if (SplitStreamScribePolicy.getPrefixMatch(message.getTopic().getId(), child.getId(), splitStream.getStripeBaseBitLength()) > 0)
          good.add(child);
        else
          bad.add(child);
      }

      int index;

      /* introduce randomness to child order */
      while (good.size() > 0) {
        index = scribe.getEnvironment().getRandomSource().nextInt(good.size());
        message.addFirst((NodeHandle)(good.elementAt(index)));
        good.remove((NodeHandle)(good.elementAt(index)));
      }

      while (bad.size() > 0) {
        index = scribe.getEnvironment().getRandomSource().nextInt(bad.size());
        message.addLast((NodeHandle)(bad.elementAt(index)));
        bad.remove((NodeHandle)(bad.elementAt(index)));
      }

      NodeHandle nextHop = message.getNext();

      /* make sure that the next node is alive */
      while ((nextHop != null) && (!nextHop.isAlive())) {
        nextHop = message.getNext();
      }

      if (nextHop == null) {
        /* if nexthop is null, then we are in 3rd stage of algorithm for locating parent.
           two cases, either
           a. local node is a leaf
              send message to our parent for dropping us and taking new subscriber
           b. local node is root for non-prefix match topic,
              drop a child from non-primary, non-root stripe and accept the new subscriber */
        if (this.scribe.isRoot(message.getTopic())) {
          Vector res = freeBandwidthUltimate(message.getTopic().getId());

          if (res != null) {
            scribe.removeChild(new Topic((Id)res.elementAt(1)), (NodeHandle)res.elementAt(0));
            scribe.addChild(message.getTopic(),((SubscribeMessage)message).getSubscriber());
            return;
          }
        } else {
          SplitStreamSubscribeContent ssc = new SplitStreamSubscribeContent(SplitStreamSubscribeContent.STAGE_FINAL);
          message.remove(parent);
          message.addFirst(parent);
          message.setContent(ssc);
        }
      } else {
        message.addFirst(nextHop);
      }
    }
  }
    
  /**
   * Returns the Channel which contains the stripe cooresponding to the
   * provided topic.
   *
   * @param topic The topic in question
   * @return The channel which contains a cooresponding stripe
   */
  private Channel getChannel(Topic topic) {
    Channel[] channels = splitStream.getChannels();

    for (int i=0; i<channels.length; i++) {
      Channel channel = channels[i];
      Stripe[] stripes = channel.getStripes();

      for (int j=0; j<stripes.length; j++) {
        Stripe stripe = stripes[j];

        if (stripe.getStripeId().getId().equals(topic.getId())) {
          return channel;
        }
      }
    }

    return null;
  }

  /**
   * Returns the total number of children for the given channel
   *
   * @param channel The channel to get the children for
   * @return The total number of children for that channel
   */
  public int getTotalChildren(Channel channel) {
    int total = 0;
    Stripe[] stripes = channel.getStripes();

    for (int j=0; j<stripes.length; j++) {
      total += scribe.getChildren(new Topic(stripes[j].getStripeId().getId())).length;
    }

    return total;
  }

  /**
    * This method makes an attempt to free up bandwidth
   * from non-primary, non-root stripes (for which local node is not root).
   *
   * @return A vector containing the child to be dropped and
   *         the corresponding stripeId
   */
  public Vector freeBandwidthUltimate(Id stripeId){
    Channel channel = getChannel(new Topic(stripeId));
    Stripe[] stripes = channel.getStripes();

    // find those stripes which are
    // a) non-primary
    // b) i am not root for them
    // c) have at least one child
    Vector candidateStripes = new Vector();
    Id victimStripeId = null;
    Topic tp;

    /* find all candidate stripes */
    for(int i = 0; i < stripes.length; i++){
      tp = new Topic(stripes[i].getStripeId().getId());
      if (!channel.getPrimaryStripe().getStripeId().getId().equals(stripes[i].getStripeId().getId()) &&
          !this.scribe.isRoot(tp) &&
          (scribe.getChildren(tp).length > 0)) {
        candidateStripes.add(stripes[i].getStripeId().getId());
      }
    }

    /* if there are no candidates, find somewhere where i am the root*/
    if(candidateStripes.size() == 0) {
      for(int i = 0; i < stripes.length; i++){
        tp = new Topic(stripes[i].getStripeId().getId());
        if ((! channel.getPrimaryStripe().getStripeId().getId().equals(stripes[i].getStripeId().getId())) &&
            (scribe.getChildren(tp).length > 0) &&
            (! stripes[i].getStripeId().getId().equals(stripeId))) {
          candidateStripes.add(stripes[i].getStripeId().getId());
        }
      }
    }

    /* hopefully, there is a candidate stripe */
    if(candidateStripes.size() > 0){
      victimStripeId = (Id)candidateStripes.elementAt(scribe.getEnvironment().getRandomSource().nextInt(candidateStripes.size()));

      NodeHandle[] children;
      children = this.scribe.getChildren(new Topic(victimStripeId));

      NodeHandle child = children[scribe.getEnvironment().getRandomSource().nextInt(children.length)];
      Vector result = new Vector();

      result.addElement(child);
      result.addElement(victimStripeId);

      return result;
    }

    return null;
  }
  
  /**
   * This method attempts to free bandwidth from our primary stripe.
   * It selects a child whose prefix match with the stripe is minimum, and drops it.
   * If multiple such child exist and newChild has same prefix match as them, then
   * new child is not taken, otherwise a random selection is made.
   * Otherwise, new child is taken and victim child is dropped.
   *
   * @return The victim child to drop.
   */
  public NodeHandle freeBandwidth(Channel channel, NodeHandle newChild, Id stripeId) {
    Stripe primaryStripe = channel.getPrimaryStripe();
    Id localId = channel.getLocalId();

    /* We have to drop one of child of the primary stripe */
    NodeHandle[] children = scribe.getChildren(new Topic(primaryStripe.getStripeId().getId()));

    /* Now, select that child which doesnt share least prefix with local node */
    int minPrefixMatch = getPrefixMatch(stripeId, newChild.getId(), channel.getStripeBase());

    /* find all potential victims */
    Vector victims = new Vector();
    for(int j = 0; j < children.length; j++){
      NodeHandle c = (NodeHandle)children[j];
      int match = getPrefixMatch(stripeId, c.getId(), channel.getStripeBase());
      if(match < minPrefixMatch){
        victims.addElement(c);
      }
    }

    /* check if new child is our best victim */
    if(victims.size() == 0)
      return newChild;
    else
      return (NodeHandle) victims.elementAt(scribe.getEnvironment().getRandomSource().nextInt(victims.size()));
  }

  /**
   * Helper method for finding prefix match between two Ids.
   *
   * @return The number of most significant digits that match.
   */
  public static int getPrefixMatch(Id target, Id sample, int digitLength){
//    int digitLength = RoutingTable.baseBitLength();
    int numDigits = rice.pastry.Id.IdBitLength / digitLength - 1;

    return (numDigits - ((rice.pastry.Id)target).indexOfMSDD((rice.pastry.Id)sample, digitLength));
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
}





