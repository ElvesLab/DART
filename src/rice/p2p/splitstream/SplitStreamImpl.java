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

import rice.environment.Environment;
import rice.environment.params.Parameters;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.scribe.*;
import rice.p2p.scribe.rawserialization.ScribeContentDeserializer;

/**
 * This is the implementing class of the ISplitStream interface. It provides the functionality of
 * creating and attaching to channels. This class is responsible for handling all channels that a 
 * local node is part of.
 * @(#) SplitStreamImpl.java
 *
 * @version $Id: SplitStreamImpl.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Ansley Post
 * @author Alan Mislove
 */
public class SplitStreamImpl implements SplitStream {

  /**
   * The scribe instance for this SplitStream Object
   */
  protected Scribe scribe;

  /**
   * The node that this application is running on
   */
  protected Node node;

  /**
   * Hashtable of all the channels currently created on this node implicitly or explicitly.
   */
  protected Hashtable<ChannelId, Channel> channels;

  protected final int stripeBaseBitLength;  
  protected final int maxFailedSubscriptions;
  protected final int defaultMaxChildren;
  
  protected String instance;

  /**
   * Convenience constructor which uses the default SplitStreamScribePolicy.
   * @param node
   * @param instance
   */
  public SplitStreamImpl(Node node, String instance) {
    this(node, instance, new SplitStreamScribePolicyFactory() {
      public ScribePolicy getSplitStreamScribePolicy(Scribe scribe,
          SplitStream splitstream) {
        return new SplitStreamScribePolicy(scribe, splitstream);
      }
    });
  }
  
  /**
   * The constructor for building the splitStream object which internally
   * creates it's own Scribe.
   *
   * @param node the pastry node that we will use
   * @param instance The instance name for this splitstream
   */
  public SplitStreamImpl(Node node, String instance, SplitStreamScribePolicyFactory factory) {
    this.instance = instance;
    Environment environment = node.getEnvironment();
    Parameters p = environment.getParameters();
    defaultMaxChildren = p.getInt("p2p_splitStream_policy_default_maximum_children");
    maxFailedSubscriptions = p.getInt("p2p_splitStream_stripe_max_failed_subscription");
    stripeBaseBitLength = p.getInt("p2p_splitStream_stripeBaseBitLength");
    this.scribe = new ScribeImpl(node, instance);
    scribe.setContentDeserializer(new ScribeContentDeserializer() {    
      public ScribeContent deserializeScribeContent(InputBuffer buf,
          Endpoint endpoint, short contentType) throws IOException {
        switch(contentType) {
          case SplitStreamContent.TYPE:
            return new SplitStreamContent(buf);
          case SplitStreamSubscribeContent.TYPE:
            return new SplitStreamSubscribeContent(buf);
        }
        throw new IllegalArgumentException("Invalid type:"+contentType);
      }    
    });
    
    this.node = node;
    this.channels = new Hashtable<ChannelId, Channel>();
    scribe.setPolicy(factory.getSplitStreamScribePolicy(scribe, this));
  }

  /**
   * This method is used by a peer who wishes to distribute the content using SplitStream. It
   * creates a Channel Object consisting of numStripes number of Stripes, one for each stripe's
   * content. A Channel object is responsible for implementing SplitStream functionality, like
   * maintaining multiple multicast trees, bandwidth management and discovering parents having spare
   * capacity. One Channel object should be created for each content distribution which wishes to
   * use SplitStream.
   *
   * @param id The id of the channel to create
   * @return an instance of a Channel class.
   */
  public Channel createChannel(ChannelId id) {
    return attachChannel(id);
  }

  /**
   * This method is used by peers who wish to listen to content distributed by some other peer using
   * SplitStream. It attaches the local node to the Channel which is being used by the source peer
   * to distribute the content. Essentially, this method finds out the different parameters of
   * Channel object which is created by the source, (the peer distributing the content) , and then
   * creates a local Channel object with these parameters and returns it. This is a non-blocking
   * call so the returned Channel object may not be initialized with all the parameters, so
   * applications should wait for channelIsReady() notification made by channels when they are
   * ready.
   *
   * @param id The id of the channel to create
   * @return An instance of Channel object.
   */
  public Channel attachChannel(ChannelId id) {
    Channel channel = (Channel) channels.get(id);

    if (channel == null) {
      channel = new Channel(id, scribe, instance, node.getIdFactory(),this.node.getId(), 
          stripeBaseBitLength, maxFailedSubscriptions);
      channels.put(id, channel);
    }
    
    ((SplitStreamScribePolicy)scribe.getPolicy()).setMaxChildren(id, 
        defaultMaxChildren);
    return channel;
  }


  /**
   * Returns all of the channels on this local splitstream
   *
   * @return All of the channels currently being received by this splitstream
   */
  public Channel[] getChannels() {
    return (Channel[]) channels.values().toArray(new Channel[0]);
  }

  /**
   * Returns the policy used to control Scribe
   *
   * @return The Scribe policy
   */
  public SplitStreamScribePolicy getPolicy(){
    return (SplitStreamScribePolicy)(scribe.getPolicy());
  }
  
  public int getStripeBaseBitLength() {
    return stripeBaseBitLength;
  }
  
  public Environment getEnvironment() {
    return scribe.getEnvironment();
  }

  public void destroy() {
    scribe.destroy();
  }
}

