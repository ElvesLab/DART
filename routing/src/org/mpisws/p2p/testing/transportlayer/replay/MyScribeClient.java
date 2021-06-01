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
package org.mpisws.p2p.testing.transportlayer.replay;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.*;
import rice.p2p.scribe.Scribe;
import rice.p2p.scribe.ScribeClient;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.scribe.ScribeImpl;
import rice.p2p.scribe.Topic;
import rice.pastry.commonapi.PastryIdFactory;

/**
 * We implement the Application interface to receive regular timed messages (see lesson5).
 * We implement the ScribeClient interface to receive scribe messages (called ScribeContent).
 * 
 * @author Jeff Hoye
 */
public class MyScribeClient implements ScribeClient, Application {

  /**
   * The message sequence number.  Will be incremented after each send.
   */
  int seqNum = 0;
  
  /**
   * This task kicks off publishing and anycasting.
   * We hold it around in case we ever want to cancel the publishTask.
   */
  CancellableTask publishTask;
  
  /** 
   * My handle to a scribe impl.
   */
  Scribe myScribe;
  
  /**
   * The only topic this appl is subscribing to.
   */
  Topic myTopic;

  Node node;
  
  /**
   * The Endpoint represents the underlieing node.  By making calls on the 
   * Endpoint, it assures that the message will be delivered to a MyApp on whichever
   * node the message is intended for.
   */
  protected Endpoint endpoint;

  protected Logger logger;
  
  /**
   * The constructor for this scribe client.  It will construct the ScribeApplication.
   * 
   * @param node the PastryNode
   */
  public MyScribeClient(Node node) {
    this.node = node;
    logger = node.getEnvironment().getLogManager().getLogger(MyScribeClient.class, null);
    
    // you should recognize this from lesson 3
    this.endpoint = node.buildEndpoint(this, "myinstance");

    // construct Scribe
    myScribe = new ScribeImpl(node,"myScribeInstance");

    // construct the topic
    myTopic = new Topic(new PastryIdFactory(node.getEnvironment()), "example topic");
//    System.out.println("myTopic = "+myTopic);
    
    // now we can receive messages
    endpoint.register();
  }
  
  /**
   * Subscribes to myTopic.
   */
  public void subscribe() {
    myScribe.subscribe(myTopic, this); 
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
      sendMulticast();
      sendAnycast();
    }
  }
  
  /**
   * Sends the multicast message.
   */
  public void sendMulticast() {
    System.out.println("Node "+endpoint.getLocalNodeHandle()+" broadcasting "+seqNum);
    MyScribeContent myMessage = new MyScribeContent(endpoint.getLocalNodeHandle(), seqNum);
    myScribe.publish(myTopic, myMessage); 
    seqNum++;
  }

  /**
   * Called whenever we receive a published message.
   */
  public void deliver(Topic topic, ScribeContent content) {
    logger.log("MyScribeClient.deliver("+topic+","+content+")");
    if (((MyScribeContent)content).from == null) {
      new Exception("Stack Trace").printStackTrace();
    }
  }

  /**
   * Sends an anycast message.
   */
  public void sendAnycast() {
    System.out.println("Node "+endpoint.getLocalNodeHandle()+" anycasting "+seqNum);
    MyScribeContent myMessage = new MyScribeContent(endpoint.getLocalNodeHandle(), seqNum);
    myScribe.anycast(myTopic, myMessage); 
    seqNum++;
  }
  
  /**
   * Called when we receive an anycast.  If we return
   * false, it will be delivered elsewhere.  Returning true
   * stops the message here.
   */
  public boolean anycast(Topic topic, ScribeContent content) {
    boolean returnValue = myScribe.getEnvironment().getRandomSource().nextInt(3) == 0;
    System.out.println("MyScribeClient.anycast("+topic+","+content+"):"+returnValue);
    return returnValue;
  }

  public void childAdded(Topic topic, NodeHandle child) {
//    System.out.println("MyScribeClient.childAdded("+topic+","+child+")");
  }

  public void childRemoved(Topic topic, NodeHandle child) {
//    System.out.println("MyScribeClient.childRemoved("+topic+","+child+")");
  }

  public void subscribeFailed(Topic topic) {
//    System.out.println("MyScribeClient.childFailed("+topic+")");
  }

  public boolean forward(RouteMessage message) {
    return true;
  }


  public void update(NodeHandle handle, boolean joined) {
    
  }

  class PublishContent implements Message {
    public int getPriority() {
      return MAX_PRIORITY;
    }
  }

  
  /************ Some passthrough accessors for the myScribe *************/
  public boolean isRoot() {
    return myScribe.isRoot(myTopic);
  }
  
  public NodeHandle getParent() {
    // NOTE: Was just added to the Scribe interface.  May need to cast myScribe to a
    // ScribeImpl if using 1.4.1_01 or older.
    return ((ScribeImpl)myScribe).getParent(myTopic); 
    //return myScribe.getParent(myTopic); 
  }
  
  public NodeHandle[] getChildren() {
    return myScribe.getChildren(myTopic); 
  }
  
}
