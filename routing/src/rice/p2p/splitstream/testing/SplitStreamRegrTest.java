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

package rice.p2p.splitstream.testing;

import java.io.IOException;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.environment.params.simple.SimpleParameters;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.testing.CommonAPITest;
import rice.p2p.splitstream.*;

/**
 * @(#) SplitStreamRegrTest.java Provides regression testing for the Scribe
 *      service using distributed nodes.
 * 
 * @version $Id: SplitStreamRegrTest.java 3992 2007-12-02 20:09:18Z jeffh $
 * @author Ansley Post
 */

public class SplitStreamRegrTest extends CommonAPITest {

  // the instance name to use
  /**
   * DESCRIBE THE FIELD
   */
  public static String INSTANCE = "SplitStreamRegrTest";

  // the scribe impls in the ring
  /**
   * DESCRIBE THE FIELD
   */
  protected SplitStreamImpl splitstreams[];

  protected SplitStreamTestClient ssclients[];

  /**
   * Constructor which sets up all local variables
   */
  public SplitStreamRegrTest(Environment env) throws IOException {
    super(env);
    splitstreams = new SplitStreamImpl[NUM_NODES];
    ssclients = new SplitStreamTestClient[NUM_NODES];
  }

  /**
   * Usage: DistScribeRegrTest [-port p] [-bootstrap host[:port]] [-nodes n]
   * [-protocol (rmi|wire)] [-help]
   * 
   * @param args DESCRIBE THE PARAMETER
   */
  public static void main(String args[]) throws IOException {

    // by properly setting the params first, the enviornment will use
    // the specified seed when creating a default RandomSource
    Environment env = parseArgs(args);
    
    SplitStreamRegrTest splitstreamTest = new SplitStreamRegrTest(env);
    splitstreamTest.start();
    env.destroy();
  }

  /**
   * Method which should process the given newly-created node
   * 
   * @param node The newly created node
   * @param num The number of this node
   */
  protected void processNode(int num, Node node) {
    splitstreams[num] = new SplitStreamImpl(node, INSTANCE);
    ssclients[num] = new SplitStreamTestClient(node, splitstreams[num]);
  }

  /**
   * Method which should run the test - this is called once all of the nodes
   * have been created and are ready.
   */
  protected void runTest() {
    if (NUM_NODES < 2) {
      System.out
          .println("The DistScribeRegrTest must be run with at least 2 nodes for proper testing.  Use the '-nodes n' to specify the number of nodes.");
      return;
    }

    // Run each test
    testBasic();
    testBandwidthUsage();
    testIndependence();
    testMaintenance(NUM_NODES / 10);
  }

  protected void testBandwidthUsage() {
    int DEFAULT_MAX_CHILDREN = environment.getParameters().getInt("p2p_splitStream_policy_default_maximum_children");
    
    boolean result = true;
    int count = 0;
    int total = 0;
    Channel channel;
    sectionStart("BandwidthUsage Test");
    stepStart("Usage");
    simulate();
    for (int i = 0; i < NUM_NODES; i++) {
      channel = ssclients[i].getChannel();
      count = ((SplitStreamScribePolicy) splitstreams[i].getPolicy())
          .getTotalChildren(channel);
      if (count > DEFAULT_MAX_CHILDREN)
        result = false;
      //System.out.println("count "+count);
      total += count;
    }
    //System.out.println("Total outgoing capacity Used "+total);

    if (result
        && (total <= (NUM_NODES - 1)
            * DEFAULT_MAX_CHILDREN)) {
      stepDone(SUCCESS);
    } else {
      stepDone(FAILURE);
    }
    sectionDone();
  }

  protected void testIndependence() {
    boolean result = true;
    int count = 0;
    int num = 0;
    int[] array = new int[20];
    Channel channel;
    Stripe[] stripes;
    sectionStart("Path Independence Test");
    stepStart("Usage");
    simulate();
    for (int i = 0; i < NUM_NODES; i++) {
      channel = ssclients[i].getChannel();
      stripes = channel.getStripes();
      num = 0;
      for (int j = 0; j < stripes.length; j++) {
        count = stripes[j].getChildren().length;
        if (count > 0)
          num++;
      }
      array[num]++;
    }
    for (int i = 0; i < 20; i++)
      System.out.println(i + "\t" + array[i]);
    sectionDone();
  }

  protected void testMaintenance(int num) {
    sectionStart("Maintenance of multicast trees");
    stepStart("Killing Nodes");
    for (int i = 0; i < num; i++) {
      System.out.println("Killing " + ssclients[i].getId());
      ssclients[i].destroy();
      kill(i);
      simulate();
    }
    
    // wait for LEASE+TimeToFindFaulty+SubscribeRetry
    // wait for notification of failure to propegate
    waitToRecoverFromKilling(params.getInt("p2p_scribe_message_timeout")*10);
    
    if (checkTree(num, NUM_NODES))
      stepDone(SUCCESS);
    else {
      stepDone(FAILURE, "not all have parent");

    }
    stepStart("Tree Recovery");

    byte[] data = { 0, 1, 0, 1, 1 };
    boolean pass = true;
    for (int i = 0; i < 10; i++) {
      ssclients[environment.getRandomSource().nextInt(NUM_NODES - num) + num]
          .publishAll(data);
      simulate();

      int totalmsgs = 0;
      for (int j = 0; j < NUM_NODES - num; j++) {
        totalmsgs = totalmsgs + ssclients[j + num].getNumMesgs();
        if (ssclients[j + num].getNumMesgs() != 16)
          System.out.println(ssclients[j + num].getId() + " recived "
              + ssclients[j + num].getNumMesgs());
        ssclients[j + num].reset();
      }
      if (totalmsgs != ((NUM_NODES - num) * 16)) {
        System.out.println("Expected " + ((NUM_NODES - num) * 16) + " messages, got " + totalmsgs);
        pass = false;
        stepDone(FAILURE);
      }
    }

    if (pass) {
      stepDone(SUCCESS);
    } else {
      stepDone(FAILURE);
    }

    sectionDone();
  }

  /*
   * ---------- Test methods and classes ----------
   */
  /**
   * Tests routing a Past request to a particular node.
   */
  protected void testBasic() {
    sectionStart("Basic Test");
    stepStart("Creating Channel");
    int creator = environment.getRandomSource().nextInt(NUM_NODES);
    ChannelId id = new ChannelId(generateId());
    ssclients[creator].createChannel(id);
    simulate();
    stepDone(SUCCESS);
    stepStart("Attaching and Joining Stripes");
    for (int i = 0; i < NUM_NODES; i++) {
      ssclients[i].attachChannel(id);
      simulate();
    }
    for (int i = 0; i < NUM_NODES; i++) {
      ssclients[i].getStripes();
      simulate();
    }
    for (int i = 0; i < NUM_NODES; i++) {
      ssclients[i].subscribeStripes();
      simulate();
    }
    if (checkTree(0, NUM_NODES))
      stepDone(SUCCESS);
    else
      stepDone(FAILURE, "not all stripes have a parent");
    stepStart("Sending Data");
    byte[] data = { 0, 1, 0, 1, 1 };
    ssclients[creator].publishAll(data);
    simulate();

    ssclients[creator].publishAll(new byte[0]);
    simulate();
    int totalmsgs = 0;
    for (int i = 0; i < NUM_NODES; i++) {
      totalmsgs = totalmsgs + ssclients[i].getNumMesgs();
      ssclients[i].reset();
    }

    if (totalmsgs == (NUM_NODES * 16 * 2)) {
      stepDone(SUCCESS);
    } else {
      stepDone(FAILURE, "Expected " + (NUM_NODES * 16 * 2) + " messages, got "
          + totalmsgs);
    }
    sectionDone();
    testFailure(1);
  }

  protected boolean checkTree(int startindex, int num) {
    Stripe[] stripes;
    boolean result = true;
    for (int i = startindex; i < num; i++) {
      stripes = ssclients[i].getStripes();
      for (int j = 0; j < stripes.length; j++) {
        if (stripes[j].getParent() == null && !stripes[j].isRoot()) {
          result = false;
          System.out
              .println("Node " + ssclients[i].getId()
                  + " is parentless for topic "
                  + stripes[j].getStripeId().getId());
        }
        //if(stripes[j].getParent() == null && stripes[j].isRoot())
        //System.out.println("Node "+ssclients[i].getId()+" is parent less, but
        // is the root for topic "+stripes[j].getStripeId().getId());
      }
    }
    return result;
  }

  protected void testFailure(int numnodes) {
    sectionStart("Failure Test");
    sectionDone();
  }

  /**
   * Private method which generates a random Id
   * 
   * @return A new random Id
   */
  private Id generateId() {
    byte[] data = new byte[20];
    environment.getRandomSource().nextBytes(data);
    return FACTORY.buildId(data);
  }

  private class SplitStreamTestClient implements SplitStreamClient {

    /**
     * The underlying common api node
     *  
     */
    private Node n = null;

    /**
     * The stripes for a channel
     *  
     */
    private Stripe[] stripes;

    /**
     * The channel to be used for this test
     *  
     */
    private Channel channel;

    /**
     * The SplitStream service for this node
     *  
     */
    private SplitStream ss;

    private int numMesgsReceived = 0;

    private SplitStreamScribePolicy policy = null;

    public SplitStreamTestClient(Node n, SplitStream ss) {
      this.n = n;
      this.ss = ss;
      log("Client Created " + n);
    }

    public void destroy() {
      ss.destroy();
    }

    public Channel getChannel() {
      return this.channel;
    }

    public void joinFailed(Stripe s) {
      log("Join Failed on " + s);
    }

    public void deliver(Stripe s, byte[] data) {
      log("Data recieved on " + s);
      numMesgsReceived++;
    }

    public void createChannel(ChannelId cid) {
      log("Channel " + cid + " created.");
      channel = ss.createChannel(cid);
    }

    public void attachChannel(ChannelId cid) {
      log("Attaching to Channel " + cid + ".");
      if (channel == null)
        channel = ss.attachChannel(cid);
    }

    public Stripe[] getStripes() {
      log("Retrieving Stripes.");
      stripes = channel.getStripes();
      return stripes;
    }

    public void subscribeStripes() {
      log("Subscribing to all Stripes.");
      for (int i = 0; i < stripes.length; i++) {
        stripes[i].subscribe(this);
      }
    }

    public void publishAll(byte[] b) {
      log("Publishing to all Stripes.");
      for (int i = 0; i < stripes.length; i++) {
        publish(b, stripes[i]);
      }
    }

    public void publish(byte[] b, Stripe s) {
      log("Publishing to " + s);
      s.publish(b);
    }

    public int getNumMesgs() {
      return numMesgsReceived;
    }

    public void reset() {
      numMesgsReceived = 0;
    }

    public Id getId() {
      return channel.getLocalId();
    }

    private void log(String s) {
      if (logger.level <= Logger.FINE) logger.log("" + n + " " + s);
      //System.out.println("" + n + " " + s);
    }

  }
}