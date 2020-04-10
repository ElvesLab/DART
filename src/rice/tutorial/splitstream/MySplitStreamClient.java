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
/*
 * Created on May 4, 2005
 */
package rice.tutorial.splitstream;

import rice.environment.random.RandomSource;
import rice.p2p.commonapi.*;
import rice.p2p.scribe.Scribe;
import rice.p2p.scribe.ScribeClient;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.ScribeImpl;
import rice.p2p.scribe.Topic;
import rice.p2p.splitstream.*;
import rice.pastry.commonapi.PastryIdFactory;

/**
 * We implement the Application interface to receive regular timed messages (see lesson5).
 * We implement the ScribeClient interface to receive scribe messages (called ScribeContent).
 * 
 * @author Jeff Hoye
 */
public class MySplitStreamClient implements SplitStreamClient, Application {

  /**
   * The lenght of a message in bytes.
   */
  public static final int DATA_LENGTH = 10;
  /**
   * The number of messages to publish.
   */
  public static final int NUM_PUBLISHES = 10;
  
  /**
   * The message sequence number.  Will be incremented after each send.
   * 
   * Out of laziness we are encoding this as a byte in the stream, so the range is limited
   */
  byte seqNum = 0;
  
  /** 
   * My handle to a SplitStream impl.
   */
  SplitStream mySplitStream;
  
  /**
   * The Id of the only Channel we are subscribing to.
   */
  ChannelId myChannelId;

  /**
   * The channel.
   */
  Channel myChannel;
  
  /**
   * The stripes... acquired from myChannel.
   */
  Stripe[] myStripes;
  
  /**
   * Data source...
   */
  protected RandomSource random;
  
  /**
   * This task kicks off publishing and anycasting.
   * We hold it around in case we ever want to cancel the publishTask.
   */
  CancellableTask publishTask;
  
  /**
   * The Endpoint represents the underlieing node.  By making calls on the 
   * Endpoint, it assures that the message will be delivered to a MyApp on whichever
   * node the message is intended for.
   */
  protected Endpoint endpoint;

  /**
   * The constructor for this scribe client.  It will construct the ScribeApplication.
   * 
   * @param node the PastryNode
   */
  public MySplitStreamClient(Node node) {
    // you should recognize this from lesson 3
    this.endpoint = node.buildEndpoint(this, "myinstance");

    // use this to generate data
    this.random = endpoint.getEnvironment().getRandomSource();
    
    // construct Scribe
    mySplitStream = new SplitStreamImpl(node,"splitStreamTutorial");

    // The ChannelId is built from a normal PastryId
    Id temp = new PastryIdFactory(node.getEnvironment()).buildId("my channel");

    // construct the ChannelId
    myChannelId = new ChannelId(temp);
    
    // now we can receive messages
    endpoint.register();
  }
  
  /**
   * Subscribes to all stripes in myChannelId.
   */
  public void subscribe() {
    // attaching makes you part of the Channel, and volunteers to be an internal node of one of the trees
    myChannel = mySplitStream.attachChannel(myChannelId);
    
    // subscribing notifies your application when data comes through the tree
    myStripes = myChannel.getStripes();
    for (int curStripe = 0; curStripe < myStripes.length; curStripe++) {
      myStripes[curStripe].subscribe(this); 
    }
  }
  
  /**
   * Starts the publish task.
   */
  public void startPublishTask() {
    publishTask = endpoint.scheduleMessage(new PublishContent(), 5000, 5000);    
  }
  
  
  /**
   * Part of the Application interface.  Will receive PublishContent every so often.
   */
  public void deliver(Id id, Message message) {
    if (message instanceof PublishContent) {
      publish();
    }
  }
  
  /**
   * Multicasts data.
   */
  public void publish() {

    for (byte curStripe = 0; curStripe < myStripes.length; curStripe++) {
      // format of the data:
      // first byte: seqNum
      // second byte: stripe
      // rest: random
      byte[] data = new byte[DATA_LENGTH];
      
      // yes, we waste some random bytes here
      random.nextBytes(data);
      data[0] = seqNum;
      data[1] = curStripe;
      
      // print what we are sending
      System.out.println("Node "+endpoint.getLocalNodeHandle()+" publishing "+seqNum+" "+printData(data));

      // publish the data
      myStripes[curStripe].publish(data); 
    }
    
    // increment the sequence number
    seqNum++;
    
    // cancel after sending all the messages
    if (seqNum >= NUM_PUBLISHES) publishTask.cancel();
  }

  /**
   * Converts the data into a string.
   * 
   * @param data
   * @return
   */
  private String printData(byte[] data) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < data.length-1; i++) {
      sb.append((int)data[i]);
      sb.append(',');
    }
    sb.append((int)data[data.length-1]);
    return sb.toString();
  }
  
  /**
   * Called whenever we receive a published message.
   */
  public void deliver(Stripe s, byte[] data) {
    System.out.println(endpoint.getId()+" deliver("+s+"):seq:"+data[0]+" stripe:"+data[1]+" "+printData(data)+")");
  }

  class PublishContent implements Message {
    public int getPriority() {
      return Message.MEDIUM_PRIORITY;
    }
  }
  
  // error handling
  public void joinFailed(Stripe s) {
    System.out.println("joinFailed("+s+")");    
  }

  // rest of the Application Interface
  public boolean forward(RouteMessage message) {
    throw new RuntimeException("Cant happen.");
  }

  public void update(NodeHandle handle, boolean joined) {
  }

}
