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
package rice.p2p.scribe.testing;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.commonapi.testing.CommonAPITest;
import rice.p2p.scribe.*;
import rice.p2p.scribe.messaging.SubscribeMessage;
import rice.p2p.util.tuples.Tuple;
import rice.pastry.PastryNode;

/**
 * @(#) DistScribeRegrTest.java Provides regression testing for the Scribe service using distributed
 * nodes.
 *
 * @version $Id: ScribeRegrTest.java 4221 2008-05-19 16:41:19Z jeffh $
 * @author Alan Mislove
 */

public class ScribeRegrTest extends CommonAPITest {

  // the instance name to use
  /**
   * DESCRIBE THE FIELD
   */
  public static String INSTANCE = "ScribeRegrTest";

  // the scribe impls in the ring
  /**
   * DESCRIBE THE FIELD
   */
  protected ScribeImpl[] scribes;

  /**
   * The scribe policies
   */
  protected TestScribePolicy policies[];

  /**
   * Constructor which sets up all local variables
   */
  public ScribeRegrTest(Environment env) throws IOException {
    super(env);
    scribes = new ScribeImpl[NUM_NODES+1];
    policies = new TestScribePolicy[NUM_NODES+1];
  }

  public TestScribeContent buildTestScribeContent(Topic topic, int numMessages) {
    return new TestScribeContent(topic, numMessages);
  }

  public void setupParams(Environment env) {
    super.setupParams(env);
    // we want to see if messages are dropped because not ready
//    if (!env.getParameters().contains("rice.p2p.scribe.ScribeImpl@ScribeRegrTest_loglevel"))
//      env.getParameters().setInt("rice.p2p.scribe.ScribeImpl@ScribeRegrTest_loglevel",Logger.INFO);
    
    // want to retry fast because of problems with isReady()
    env.getParameters().setInt("p2p_scribe_message_timeout",3000); 
  }
  
  /**
   * Usage: DistScribeRegrTest [-port p] [-bootstrap host[:port]] [-nodes n] [-protocol (rmi|wire)]
   * [-help]
   *
   * @param args DESCRIBE THE PARAMETER
   */
  public static void main(String args[]) throws IOException {
//    System.setOut(new PrintStream("srt.log"));
//    System.setErr(System.out);
    
    Environment env = parseArgs(args);
    
    ScribeRegrTest scribeTest = new ScribeRegrTest(env);
    
    
    scribeTest.start();
    env.destroy();
  }

  /**
   * Method which should process the given newly-created node
   *
   * @param node The newly created node
   * @param num The number of this node
   */
  protected void processNode(int num, Node node) {
    scribes[num] = new ScribeImpl(node, INSTANCE);
    policies[num] = new TestScribePolicy(scribes[num]);
    scribes[num].setPolicy(policies[num]);
  }

  /**
   * Method which should run the test - this is called once all of the nodes have been created and
   * are ready.
   */
  protected void runTest() {
    if (NUM_NODES < 2) {
      System.out.println("The DistScribeRegrTest must be run with at least 2 nodes for proper testing.  Use the '-nodes n' to specify the number of nodes.");
      return;
    }

    // Run each test
    testBasic(1, "Basic");
    testBasic(2, "Partial (1)");
    testBasic(4, "Partial (2)");
    testMultiSubscribe(1, "Basic");
    testSingleRoot("Single rooted Trees");
    testAPI();
    testFailureNotification();
    testAddNode();    
    testMaintenance();

  }

  /*
   *  ---------- Test methods and classes ----------
   */
  
  protected void testAddNode() {
    String name = "TestAddNode";
    Topic topic = new Topic(generateId());
    Id id = topic.getId();
    TestScribeClient[] clients = new TestScribeClient[NUM_NODES];
    
    sectionStart("Test Add Node");
    stepStart("Tree Construction id "+id);
    for (int i = 0; i < NUM_NODES; i++) {
      clients[i] = new TestScribeClient(scribes[i], topic, i);
      scribes[i].subscribe(topic, clients[i]);
      simulate();
    }
    stepDone(SUCCESS);
    
    stepStart("Add New Root");
    PastryNode pn = factory.newNode(getBootstrap(),(rice.pastry.Id)id);
    nodes[NUM_NODES] = pn;
    processNode(NUM_NODES, pn);    
    if (logger.level <= Logger.INFO) logger.log("Adding new root: "+ pn);
    synchronized(nodes[NUM_NODES]) {
      while(!pn.isReady()) {
        try { pn.wait(1000); } catch (InterruptedException ie) {return;}
      }
    }
    Collection<NodeHandle> children = scribes[NUM_NODES].getChildrenOfTopic(topic);
    if (children.size() >= 1) {
      stepDone(SUCCESS);
    } else {
      stepDone(FAILURE, "Node should be the root, has "+children.size()+" children for topic "+topic+"."); 
    }
    sectionDone();
  }
  
  /**
    * Tests basic functionality
   */
  protected void testBasic(int skip, String name) {
    sectionStart(name + " Scribe Networks");
    int NUM_MESSAGES = 5;
    int SKIP = skip;
    Topic topic = new Topic(generateId());
    TestScribeClient[] clients = new TestScribeClient[NUM_NODES/SKIP];

    stepStart(name + " Tree Construction");
    for (int i = 0; i < NUM_NODES/SKIP; i++) {
      clients[i] = new TestScribeClient(scribes[i], topic, i);
      scribes[i].subscribe(topic, clients[i]);
      simulate();
    }
    simulate(NUM_NODES/SKIP);

    int numWithParent = 0;
    for (int i=0; i < NUM_NODES/SKIP; i++) {
      if (scribes[i].getParent(topic) != null)
        numWithParent++;
    }

    if (numWithParent < (NUM_NODES/SKIP) - 1)
      stepDone(FAILURE, "Expected at least " + (NUM_NODES/SKIP - 1) + " nodes with parents, found " + numWithParent);
    else
      stepDone(SUCCESS);

    stepStart(name + " Publish");
    ScribeImpl local = scribes[environment.getRandomSource().nextInt(NUM_NODES/SKIP)];

    for (int i = 0; i < NUM_MESSAGES; i++) {
      local.publish(topic, buildTestScribeContent(topic, i));
      simulate();
    }

    boolean failed = false;
    for (int i=0; i < NUM_NODES/SKIP; i++) {
      if (clients[i].getPublishMessages().size() != NUM_MESSAGES) {
        stepDone(FAILURE, "Expected client " + clients[i] + " to receive all "+NUM_MESSAGES+" messages, received " + clients[i].getPublishMessages().size());
        failed = true;
      }
    }

    if (! failed)
      stepDone(SUCCESS);

    stepStart(name + " Anycast - No Accept");
    local = scribes[environment.getRandomSource().nextInt(NUM_NODES)];

    local.anycast(topic, buildTestScribeContent(topic, 59));    
    simulate(NUM_NODES);

    failed = false;
    for (int i=0; i < NUM_NODES/SKIP; i++) {
      if (clients[i].getAnycastMessages().size() != 0) {
        stepDone(FAILURE, "Expected no accepters for anycast, found one at " + scribes[i]);
        failed = true;
      }
    }

    if (! failed)
      stepDone(SUCCESS);

    /**
     * This test sends an anycast ... (write more)
     */
    stepStart(name + " Anycast - 1 Accept");
    TestScribeClient client = clients[environment.getRandomSource().nextInt(NUM_NODES/SKIP)];
    client.acceptAnycast(true);
    local = scribes[environment.getRandomSource().nextInt(NUM_NODES)];

    local.anycast(topic, buildTestScribeContent(topic, 60));
    simulate(NUM_NODES);

    failed = false;
    for (int i=0; i < NUM_NODES/SKIP; i++) {
      if (clients[i].equals(client)) {
        if (clients[i].getAnycastMessages().size() != 1) {
          String s = "";
          for (ScribeContent item : clients[i].getAnycastMessages()) {
            s+=" "+item;
          }

          stepDone(FAILURE, "Expected node "+client.scribe+" to accept anycast at " + client+" accepted "+clients[i].getAnycastMessages().size()+" local "+local+" "+s);
          failed = true;
        }
      } else {
        if (clients[i].getAnycastMessages().size() != 0) {
          stepDone(FAILURE, "Expected no accepters for anycast, found one at " + scribes[i]);
          failed = true;
        }
      }
    }

    if (! failed)
      stepDone(SUCCESS);

    stepStart(name + " Anycast - All Accept");
    for (int i=0; i < NUM_NODES/SKIP; i++) {
      clients[i].acceptAnycast(true);
    }

    local = scribes[environment.getRandomSource().nextInt(NUM_NODES/SKIP)];

    local.anycast(topic, buildTestScribeContent(topic, 61));
    simulate(NUM_NODES);

    int total = 0;
    for (int i=0; i < NUM_NODES/SKIP; i++) {
      total += clients[i].getAnycastMessages().size();
    }

    if (total != 2) {
      String str = "";
      for (int i=0; i < NUM_NODES/SKIP; i++) {
        if (clients[i].getAnycastMessages().size() != 0) {
          for (ScribeContent sc : clients[i].getAnycastMessages()) {
            str+="\n"+clients[i]+" "+sc;
          }
        }
      }
    
      stepDone(FAILURE, "Expected 2 anycast messages to be found, found "+total+" l:"+str);
    } else {
      stepDone(SUCCESS);
    }
    
    stepStart(name + " Unsubscribe");
    for (int i=0; i < NUM_NODES/SKIP; i++) {
      scribes[i].unsubscribe(topic, clients[i]);
      simulate();
    }

    local = scribes[environment.getRandomSource().nextInt(NUM_NODES)];
    local.publish(topic, buildTestScribeContent(topic, 100));
    simulate(NUM_NODES);

    failed = false;
    for (int i=0; i < NUM_NODES/SKIP; i++) {
      if (clients[i].getPublishMessages().size() != NUM_MESSAGES) {
        stepDone(FAILURE, "Expected client " + clients[i] + " to receive no additional messages, received " + clients[i].getPublishMessages().size());
        failed = true;
      }
    }

    if (! failed)
      stepDone(SUCCESS);

    stepStart(name + " Tree Completely Demolished");
    failed = false;
    for (int i=0; i < NUM_NODES; i++) {
      if (scribes[i].getClients(topic).size() > 0) {
        stepDone(FAILURE, "Expected scribe " + scribes[i] + " to have no clients, had " + scribes[i].getClients(topic).size());
        failed = true;
      }

      if (scribes[i].getChildren(topic).length > 0) {
        stepDone(FAILURE, "Expected scribe " + scribes[i] + " to have no children, had " + scribes[i].getChildren(topic).length);
        failed = true;
      }

      if (scribes[i].getParent(topic) != null) {
        stepDone(FAILURE, "Expected scribe " + scribes[i] + " to have no parent, had " + scribes[i].getParent(topic));
        failed = true;
      }
    }

    if (! failed)
      stepDone(SUCCESS);    

    sectionDone();
  }

  /**
   * Makes sure that if you subscribe to multiple topics at once that they end up on different nodes.
   *
   */
  protected void testMultiSubscribe(int skip, String name) {
      sectionStart(name + " MultiSubscription Scribe Network");
      int NUM_MESSAGES = 5;
      int SKIP = skip;
      int NUM_TOPICS = 2;
      
      // build antipodal topics so they are guaranteed to have different parents   
      // TODO: Randomize this
      TestScribeClient[] clients = new TestScribeClient[NUM_NODES];
      List<Topic> topics = new ArrayList<Topic>(NUM_TOPICS);
      int[] id1 = {0, 0, 0, 0, 0x80000000};
      int[] id2 = {0, 0, 0, 0, 0};
      topics.add(new Topic(FACTORY.buildId(id1)));
      topics.add(new Topic(FACTORY.buildId(id2)));
//      System.out.println(topics.get(0).toString()+" "+topics.get(1).toString());
      
      stepStart(name + " Tree Construction (Parent Independence)");
      for (int i = 0; i < NUM_NODES; i++) {
        clients[i] = new TestScribeClient(scribes[i], topics, i);
        scribes[i].subscribe(topics, clients[i],null, null);
        simulate();
      }

      int numWithParent = 0;
      for (int i=0; i < NUM_NODES; i++) {
        if (scribes[i].getParent(topics.get(0)) == scribes[i].getParent(topics.get(1))) {
          stepDone(FAILURE, "Topics "+topics.get(0)+" and "+topics.get(1)+" had same parent "+scribes[i].getParent(topics.get(0))+" on node "+scribes[i].getEndpoint().getLocalNodeHandle());         
        }
      }
      stepDone(SUCCESS);
      
      stepStart(name + " Publish");
      ScribeImpl local = scribes[environment.getRandomSource().nextInt(NUM_NODES/SKIP)];

      for (int i = 0; i < NUM_MESSAGES; i++) {
        for (Topic topic : topics) {
          local.publish(topic, buildTestScribeContent(topic, i));
        }
        simulate();
      }

      boolean failed = false;
      for (int i=0; i < NUM_NODES/SKIP; i++) {
        if (clients[i].getPublishMessages().size() != NUM_MESSAGES*NUM_TOPICS) {
          stepDone(FAILURE, "Expected client " + clients[i] + " to receive all "+NUM_MESSAGES*NUM_TOPICS+" messages, received " + clients[i].getPublishMessages().size());
          failed = true;
        }
      }

      if (! failed)
        stepDone(SUCCESS);

      stepStart(name + " Anycast - No Accept");
      local = scribes[environment.getRandomSource().nextInt(NUM_NODES)];

      for (Topic topic : topics) 
        local.anycast(topic, buildTestScribeContent(topic, 62));
      simulate(NUM_NODES*NUM_TOPICS);

      failed = false;
      for (int i=0; i < NUM_NODES/SKIP; i++) {
        if (clients[i].getAnycastMessages().size() != 0) {
          stepDone(FAILURE, "Expected no accepters for anycast, found one at " + scribes[i]);
          failed = true;
        }
      }

      if (! failed)
        stepDone(SUCCESS);

      /**
       * This test sends an anycast on each topic, but only 1 client is allowed to accept the message
       */
      stepStart(name + " Anycast - 1 Accept");
      TestScribeClient client = clients[environment.getRandomSource().nextInt(NUM_NODES/SKIP)];
      client.acceptAnycast(true);
      local = scribes[environment.getRandomSource().nextInt(NUM_NODES)];

      for (Topic topic : topics) 
        local.anycast(topic, buildTestScribeContent(topic, 63));
      simulate(NUM_NODES*NUM_TOPICS);

      failed = false;
      for (int i=0; i < NUM_NODES/SKIP; i++) {
        if (clients[i].equals(client)) {
          if (clients[i].getAnycastMessages().size() != NUM_TOPICS) {
            String s = "";
            for (ScribeContent item : clients[i].getAnycastMessages()) {
              s+=" "+item;
            }
            stepDone(FAILURE, "Expected node to accept "+NUM_TOPICS+" anycasts at " + client+" accepted "+clients[i].getAnycastMessages().size()+" local "+local+" "+s);
            failed = true;
          }
        } else {
          if (clients[i].getAnycastMessages().size() != 0) {
            stepDone(FAILURE, "Expected no accepters for anycast, found one at " + scribes[i]);
            failed = true;
          }
        }
      }

      if (! failed)
        stepDone(SUCCESS);

      stepStart(name + " Anycast - All Accept");
      for (int i=0; i < NUM_NODES/SKIP; i++) {
        clients[i].acceptAnycast(true);
      }

      local = scribes[environment.getRandomSource().nextInt(NUM_NODES/SKIP)];

      for (Topic topic : topics) 
        local.anycast(topic, buildTestScribeContent(topic, 64));
      simulate(NUM_TOPICS);

      int total = 0;
      for (int i=0; i < NUM_NODES/SKIP; i++) {
        total += clients[i].getAnycastMessages().size();
      }

      if (total != NUM_TOPICS*2) {
        String str = "";
        for (int i=0; i < NUM_NODES/SKIP; i++) {
          if (clients[i].getAnycastMessages().size() != 0) {
            for (ScribeContent sc : clients[i].getAnycastMessages()) {
              str+="\n"+clients[i]+" "+sc;
            }
          }
        }
      
        stepDone(FAILURE, "Expected "+(NUM_TOPICS*2)+" anycast messages to be found, found "+total+" l:"+str);
      } else {
        stepDone(SUCCESS);
      }
      
      stepStart(name + " Unsubscribe");
      for (int i=0; i < NUM_NODES/SKIP; i++) {
        for (Topic topic : topics) {
          scribes[i].unsubscribe(topic, clients[i]);
        }
        simulate();
      }

      local = scribes[environment.getRandomSource().nextInt(NUM_NODES)];
      for (Topic topic : topics) 
        local.publish(topic, buildTestScribeContent(topic, 100));
      simulate();

      failed = false;
      for (int i=0; i < NUM_NODES/SKIP; i++) {
        if (clients[i].getPublishMessages().size() != NUM_MESSAGES*NUM_TOPICS) {
          stepDone(FAILURE, "Expected client " + clients[i] + " to receive no additional messages, received " + clients[i].getPublishMessages().size());
          failed = true;
        }
      }

      if (! failed)
        stepDone(SUCCESS);

      stepStart(name + " Tree Completely Demolished");
      failed = false;
      for (int i=0; i < NUM_NODES; i++) {
        for (Topic topic : topics) {
          if (scribes[i].getClients(topic).size() > 0) {
            stepDone(FAILURE, "Expected scribe " + scribes[i] + " to have no clients, had " + scribes[i].getClients(topic).size());
            failed = true;
          }
  
          if (scribes[i].getChildren(topic).length > 0) {
            stepDone(FAILURE, "Expected scribe " + scribes[i] + " to have no children, had " + scribes[i].getChildren(topic).length);
            failed = true;
          }
  
          if (scribes[i].getParent(topic) != null) {
            stepDone(FAILURE, "Expected scribe " + scribes[i] + " to have no parent, had " + scribes[i].getParent(topic));
            failed = true;
          }
        }
      }

      if (! failed)
        stepDone(SUCCESS);    

      sectionDone();
    }

  
  /**
    * Tests basic publish functionality
   */
    protected void testAPI() {
  sectionStart("Scribe API Functionality");
    int NUM_MESSAGES = 5;
    Topic topic = new Topic(generateId());
    TestScribeClient[] clients = new TestScribeClient[NUM_NODES];

    stepStart("Tree Construction");
    for(int i = 0; i < NUM_NODES; i++)
  policies[i].allowSubscribe(false);

    for (int i = 0; i < NUM_NODES/2; i++) {
      clients[i] = new TestScribeClient(scribes[i], topic, i);
      scribes[i].subscribe(topic, clients[i]);
      simulate();
    }
    simulate(NUM_NODES);

    int numWithParent = 0;
    for (int i=0; i < NUM_NODES; i++) {
      if (scribes[i].getParent(topic) != null)
        numWithParent++;
    }

    if (numWithParent < (NUM_NODES/2) - 1) {
      String s = "";
      for (int i=0; i < NUM_NODES; i++) {
        if (scribes[i].getParent(topic) == null) {
          s+=" "+scribes[i];
        }
      }
      
      stepDone(FAILURE, "Expected at least " + (NUM_NODES/2 - 1) + " nodes with parents, found " + numWithParent+" "+s);
    } else
      if (numWithParent > (NUM_NODES/2))
        stepDone(FAILURE, "Expected no more than " + (NUM_NODES/2) + " nodes with parents, due to policy, found " + numWithParent);
      else 
        stepDone(SUCCESS);

    stepStart("Drop Child");
    // now, find a scribe with a child
    ScribeImpl scribe = null;
    TestScribeClient client = null;
    TestScribePolicy policy = null;

    for (int i=0; (i<NUM_NODES) && (scribe == null); i++) {
      if (scribes[i].getChildren(topic).length > 0) {
        scribe = scribes[i];
        client = clients[i];
        policy = policies[i];
      }
    }

    if (scribe == null) {
      stepDone(FAILURE, "Could not find any scribes with children");
    } else {
      NodeHandle child = scribe.getChildren(topic)[0];

      // set this client to never allow subscribes
      policy.neverAllowSubscribe(true);
      
      // drop the handle now
      scribe.removeChild(topic, child);
      simulate();

      ScribeImpl local = scribes[environment.getRandomSource().nextInt(NUM_NODES)];

      for (int i = 0; i < NUM_MESSAGES; i++) {
        local.publish(topic, buildTestScribeContent(topic, i));
        simulate();
      }

      boolean failed = false;
      for (int i=0; i < NUM_NODES/2; i++) {
        if (clients[i].getPublishMessages().size() != NUM_MESSAGES) {
          stepDone(FAILURE, "Expected client " + clients[i] + " to receive all "+NUM_MESSAGES+" messages, received " + clients[i].getPublishMessages().size());
          failed = true;
        }
      }

      NodeHandle[] children = scribe.getChildren(topic);

      if (Arrays.asList(children).contains(child)) {
        stepDone(FAILURE, "Child resubscribed to previous node, policy should prevent this.");
        failed = true;
      }

      if (! failed)
        stepDone(SUCCESS);
    }

    stepStart("Reset Policies");
    for (int i=0; i<NUM_NODES; i++) {
      policies[i].allowSubscribe(true);
      policies[i].neverAllowSubscribe(false);
    }
    stepDone(SUCCESS);
      
    sectionDone();
  }

  /**
    * Tests failure notification
   */
  protected void testFailureNotification() {
    sectionStart("Subscribe Failure Notification");
    Topic topic = new Topic(generateId());
    TestScribeClient client;

    stepStart("Policy Change");
    for (int i=0; i < NUM_NODES; i++) {
      policies[i].neverAllowSubscribe(true);
    }

    stepDone(SUCCESS);
    
//    stepStart("Force Root To Accept");
//    client = new TestScribeClient(scribes[NUM_NODES-1], topic, NUM_NODES-1);
//    // join 1 node first to force the Topic to be created
//    scribes[NUM_NODES-1].subscribe(topic, client);
//    simulate();
//    if (client.getSubscribeFailed())
//      stepDone(FAILURE, "Expected subscribe to succeed because we force the root to accept 1 child.");
//    else
//      stepDone(SUCCESS);
    
    stepStart("Subscribe Attempt");
    int i = environment.getRandomSource().nextInt(NUM_NODES-1);

    while (scribes[i].isRoot(topic))
      i = environment.getRandomSource().nextInt(NUM_NODES-1);
    
    client = new TestScribeClient(scribes[i], topic, i);
    scribes[i].subscribe(topic, client);
    simulate();

    stepDone(SUCCESS);

    stepStart("Failure Notification Delivered");
    if (! client.getSubscribeFailed())
      stepDone(FAILURE, "Expected subscribe to fail, but did not.");
    else
      stepDone(SUCCESS);

    stepStart("Policy Reset");
    for (int j=0; j < NUM_NODES; j++) {
      policies[j].neverAllowSubscribe(false);
    }

    stepDone(SUCCESS);

    sectionDone();
  }    


    protected void testSingleRoot(String name) {
      sectionStart(name + "");
      int numTrees = 10;
      boolean failed = false;

  for(int num=0; num<numTrees; num ++) {
      Topic topic = new Topic(generateId());
      TestScribeClient[] clients = new TestScribeClient[NUM_NODES];
      
      stepStart(name + " TopicId=" + topic.getId());
      for (int i = 0; i < NUM_NODES; i++) {
        clients[i] = new TestScribeClient(scribes[i], topic, i);
        scribes[i].subscribe(topic, clients[i]);
        simulate();
      }
      
      int numRoot = 0;
      for (int i=0; i < NUM_NODES; i++) {
    if (scribes[i].isRoot(topic)) {
        numRoot++;
        //System.out.println("myId= " + scribes[i].getId());
    }
      }
      
      if (numRoot != 1) {
    stepDone(FAILURE, "Number of roots= " + numRoot);
    failed = true;
      }
      else
    stepDone(SUCCESS);
      
  }
  sectionDone();
  
    }
    


  /**
   * Tests basic publish functionality
   */
    protected void testMaintenance() {
  sectionStart("Tree Maintenance Under Node Death");
  int NUM_MESSAGES = 5;
  Topic topic = new Topic(generateId());
  TestScribeClient[] clients = new TestScribeClient[NUM_NODES];

    stepStart("Tree Construction");
    for (int i = 0; i < NUM_NODES; i++) {
      clients[i] = new TestScribeClient(scribes[i], topic, i);
      scribes[i].subscribe(topic, clients[i]);
      simulate();
    }
    simulate(NUM_NODES);

    int numWithParent = 0;
    for (int i=0; i < NUM_NODES; i++) {
      if (scribes[i].getParent(topic) != null)
        numWithParent++;
    }

    if (numWithParent < NUM_NODES-1)
      stepDone(FAILURE, "Expected at least " + (NUM_NODES - 1) + " nodes with parents, found " + numWithParent);
    else
      stepDone(SUCCESS);
    
//    environment.getParameters().setInt("org.mpisws.p2p.transport.wire.UDPLayer_loglevel", Logger.ALL);
    
    stepStart("Killing Nodes");
    for (int i=0; i<NUM_NODES/2; i++) {
//      System.out.println("Killing " + scribes[i].getId());
//      logger.log("Killing " + nodes[i]);
      if (logger.level <= Logger.INFO) logger.log("Killing " + nodes[i]);
      scribes[i].destroy();
      kill(i);
      simulate(5);
    }

    waitToRecoverFromKilling(scribes[0].MESSAGE_TIMEOUT);    
    stepDone(SUCCESS);

    stepStart("Tree Recovery");
    
    int localIndex = environment.getRandomSource().nextInt(NUM_NODES/2) + NUM_NODES/2;
    ScribeImpl local = scribes[localIndex];

//    System.out.println("Local:"+nodes[localIndex]);
    
    for (int i = 0; i < NUM_MESSAGES; i++) {
      local.publish(topic, buildTestScribeContent(topic, i));
      simulate(5);
    }

    boolean failed = false;
    for (int i=NUM_NODES/2; i < NUM_NODES; i++) {
      if (clients[i].getPublishMessages().size() != NUM_MESSAGES) {
        stepDone(FAILURE, "Expected client " + nodes[i] +":"+clients[i]+ " to receive all "+NUM_MESSAGES+" messages, received " + clients[i].getPublishMessages().size());
        failed = true;
      }
    }

    if (! failed)
      stepDone(SUCCESS);

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

  public static List<Topic> buildListOf1(Topic topic) {
    List<Topic> ret = new ArrayList<Topic>(1);
    ret.add(topic);
    return ret;
  }    

  /**
   * Utility class for past content objects
   *
   * @version $Id: ScribeRegrTest.java 4221 2008-05-19 16:41:19Z jeffh $
   * @author amislove
   */
  protected static class TestScribeContent implements ScribeContent {

    /**
     * DESCRIBE THE FIELD
     */
    protected Topic topic;

    /**
     * DESCRIBE THE FIELD
     */
    protected int num;

    /**
     * Constructor for TestScribeContent.
     *
     * @param topic DESCRIBE THE PARAMETER
     * @param num DESCRIBE THE PARAMETER
     */
    public TestScribeContent(Topic topic, int num) {
      this.topic = topic;
      this.num = num;
    }

    /**
     * DESCRIBE THE METHOD
     *
     * @param o DESCRIBE THE PARAMETER
     * @return DESCRIBE THE RETURN VALUE
     */
    public boolean equals(Object o) {
      if (!(o instanceof TestScribeContent)) {
        return false;
      }

      return (((TestScribeContent) o).topic.equals(topic) &&
        ((TestScribeContent) o).num == num);
    }

    /**
     * DESCRIBE THE METHOD
     *
     * @return DESCRIBE THE RETURN VALUE
     */
    public String toString() {
      return "TestScribeContent(" + topic + ", " + num + ")";
    }
  }

  /**
   * Utility class which simulates a route message
   *
   * @version $Id: ScribeRegrTest.java 4221 2008-05-19 16:41:19Z jeffh $
   * @author amislove
   */
  protected static class TestRouteMessage implements RouteMessage {

    private Id id;

    private NodeHandle nextHop;

    private Message message;

    /**
     * Constructor for TestRouteMessage.
     *
     * @param id DESCRIBE THE PARAMETER
     * @param nextHop DESCRIBE THE PARAMETER
     * @param message DESCRIBE THE PARAMETER
     */
    public TestRouteMessage(Id id, NodeHandle nextHop, Message message) {
      this.id = id;
      this.nextHop = nextHop;
      this.message = message;
    }

    /**
     * Gets the DestinationId attribute of the TestRouteMessage object
     *
     * @return The DestinationId value
     */
    public Id getDestinationId() {
      return id;
    }

    /**
     * Gets the NextHopHandle attribute of the TestRouteMessage object
     *
     * @return The NextHopHandle value
     */
    public NodeHandle getNextHopHandle() {
      return nextHop;
    }

    /**
     * Gets the Message attribute of the TestRouteMessage object
     * 
     * @deprecated
     * @return The Message value
     */
    public Message getMessage() {
      return message;
    }

    public Message getMessage(MessageDeserializer md) {
      return message;
    }

    /**
     * Sets the DestinationId attribute of the TestRouteMessage object
     *
     * @param id The new DestinationId value
     */
    public void setDestinationId(Id id) {
      this.id = id;
    }

    /**
     * Sets the NextHopHandle attribute of the TestRouteMessage object
     *
     * @param nextHop The new NextHopHandle value
     */
    public void setNextHopHandle(NodeHandle nextHop) {
      this.nextHop = nextHop;
    }

    /**
     * Sets the Message attribute of the TestRouteMessage object
     *
     * @param message The new Message value
     */
    public void setMessage(Message message) {
      this.message = message;
    }
    
    public void setMessage(RawMessage message) {
      this.message = message;
    }
    
  }

  /**
   * DESCRIBE THE CLASS
   *
   * @version $Id: ScribeRegrTest.java 4221 2008-05-19 16:41:19Z jeffh $
   * @author amislove
   */
  protected class TestScribeClient implements ScribeClient {

    /**
     * DESCRIBE THE FIELD
     */
    protected Scribe scribe;

    /**
     * DESCRIBE THE FIELD
     */
    protected int i;

    /**
     * The publish messages received so far
     */
    protected Vector publishMessages;

    /**
      * The publish messages received so far
     */
    protected Vector anycastMessages;

    /**
     * The topic this client is listening for
     */
    protected List<Topic> topics;

    /**
     * Whether or not this client should accept anycasts
     */
    protected boolean acceptAnycast;

    /**
     * Whether this client has had a subscribe fail
     */
    protected boolean subscribeFailed;

    /**
     * Constructor for TestScribeClient.
     *
     * @param scribe DESCRIBE THE PARAMETER
     * @param i DESCRIBE THE PARAMETER
     */
    public TestScribeClient(Scribe scribe, Topic topic, int i) {
      this(scribe, buildListOf1(topic), i);   
    }
    
    public TestScribeClient(Scribe scribe, List<Topic> topics, int i) {
      this.scribe = scribe;
      this.i = i;
      this.topics = topics;
      this.publishMessages = new Vector();
      this.anycastMessages = new Vector();
      this.acceptAnycast = false;
      this.subscribeFailed = false;
    }

    public List<ScribeContent> getPublishMessages() {
      return publishMessages;
    }

    public List<ScribeContent> getAnycastMessages() {
      return anycastMessages;
    }

    public void acceptAnycast(boolean value) {
      this.acceptAnycast = value;
    }

    /**
     * DESCRIBE THE METHOD
     *
     * @param topic DESCRIBE THE PARAMETER
     * @param content DESCRIBE THE PARAMETER
     * @return DESCRIBE THE RETURN VALUE
     */
    public boolean anycast(Topic topic, ScribeContent content) {
//      System.out.println(scribe+" "+this+".anycast():"+acceptAnycast);
      if (acceptAnycast)
        anycastMessages.add(content);

      return acceptAnycast;
    }

    /**
     * DESCRIBE THE METHOD
     *
     * @param topic DESCRIBE THE PARAMETER
     * @param content DESCRIBE THE PARAMETER
     */
    public void deliver(Topic topic, ScribeContent content) {
//      scribe.getEnvironment().getLogManager().getLogger(TestScribeClient.class,null).log(this+"deliver("+topic+","+content+")");
      publishMessages.add(content);
    }

    /**
     * DESCRIBE THE METHOD
     *
     * @param topic DESCRIBE THE PARAMETER
     * @param child DESCRIBE THE PARAMETER
     */
    public void childAdded(Topic topic, NodeHandle child) {
     // System.out.println("CHILD ADDED AT " + scribe.getId());
    }

    /**
     * DESCRIBE THE METHOD
     *
     * @param topic DESCRIBE THE PARAMETER
     * @param child DESCRIBE THE PARAMETER
     */
    public void childRemoved(Topic topic, NodeHandle child) {
     // System.out.println("CHILD REMOVED AT " + scribe.getId());
    }

    public void subscribeFailed(Topic topic) {
      subscribeFailed = true;
      scribe.subscribe(topic, this);
    }

    public boolean getSubscribeFailed() {
      return subscribeFailed;
    }
    
    public String toString() {      
      if (topics.size() == 1) {
        return topics.get(0).toString(); 
      }
      String s = "";
      for (Topic topic : topics) {
        s+=topics.toString()+" "; 
      }
      return s+scribe;
    }
  }

  public class TestScribePolicy extends ScribePolicy.DefaultScribePolicy {

    protected Scribe scribe;

    protected boolean allowSubscribe;

    protected boolean neverAllowSubscribe;
    
    public TestScribePolicy(Scribe scribe) {
      super(scribe.getEnvironment());
      this.scribe = scribe;
      allowSubscribe = true;
      neverAllowSubscribe = false;
    }

    public void allowSubscribe(boolean allowSubscribe) {
      this.allowSubscribe = allowSubscribe;
    }

    public void neverAllowSubscribe(boolean neverAllowSubscribe) {
      this.neverAllowSubscribe = neverAllowSubscribe;
    }

    public boolean allowSubscribe(SubscribeMessage message, ScribeClient[] clients, NodeHandle[] children) {
  //System.out.println("Allow subscribe , client.size "+clients.length+", children "+children.length+" for subscriber "+message.getSubscriber());
      return (! neverAllowSubscribe) && (allowSubscribe || (clients.length > 0) || this.scribe.isRoot(message.getTopic()));
    }
  }
}














