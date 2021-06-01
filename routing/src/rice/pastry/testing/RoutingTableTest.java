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
package rice.pastry.testing;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

import rice.Destructable;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.random.RandomSource;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.scribe.*;
import rice.pastry.*;
import rice.pastry.NodeHandle;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.direct.*;
import rice.pastry.leafset.LeafSet;
import rice.pastry.routing.*;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.selector.SelectorManager;
import rice.selector.TimerTask;
import rice.tutorial.direct.MyMsg;

/**
 * @author Jeff Hoye
 */
@SuppressWarnings("unchecked")
public class RoutingTableTest {
  boolean printLiveness = false;
  boolean printLeafSets = false;
  
  // this will keep track of our nodes
  Vector<PastryNode> nodes = new Vector<PastryNode>();
  
  HashMap apps = new HashMap();
  
  int timeToFindFaulty = 30000; // millis
  
  final Environment env;
  int numNodes;
  int meanSessionTime;
  boolean useScribe;
  int msgSendRate;
  int rtMaintTime;
  int tryNum;
  
  PastryNodeFactory factory;
  
  IdFactory idFactory;
  NodeIdFactory nidFactory;
  Topic topic;
  
  int reportRate = 60*1000; // 1 minute
//  int reportRate = 60*1000*10; // 10 minutes
//  int reportRate = 60*1000*60; // 1 hour
  
  int testTime = 60*1000*60*10; // 10 hours
//  int testTime = 60*1000*60; // 1 hour

  
//  public static boolean useMaintenance = false;
//  public static boolean useMessaging = false;
//  public static boolean useScribe = false;
//  
//  public static int rtMaintInterval = 15*60; // seconds
//  public static int msgSendRate = 10000; // millis per node
  
  public static final boolean logHeavy = false;
  
//  public static int T_total = 0;
//  public static int T_ctr = 0;
//  public static int T_ave = 0;
  
  /**
   * This constructor launches numNodes PastryNodes.  They will bootstrap 
   * to an existing ring if one exists at the specified location, otherwise
   * it will start a new ring.
   * 
   * @param bindport the local port to bind to 
   * @param bootaddress the IP:port of the node to boot from
   * @param numNodes the number of nodes to create in this JVM
   * @param env the environment for these nodes
   */
  public RoutingTableTest(int numNodes, int meanSessionTime, boolean useScribe, int msgSendRate, int rtMaintTime, int tryNum, final Environment env) throws Exception {
    System.out.println("numNodes:"+numNodes+" meanSessionTime:"+meanSessionTime+" scribe:"+useScribe+" msgSendRate:"+msgSendRate+" rtMaint:"+rtMaintTime+" try:"+tryNum);
    this.env = env;
    this.numNodes = numNodes;
    this.meanSessionTime = meanSessionTime;
    this.useScribe = useScribe;
    this.msgSendRate = msgSendRate;
    this.rtMaintTime = rtMaintTime;
    this.tryNum = tryNum;
    
    idFactory = new PastryIdFactory(env);
    topic = new Topic(idFactory.buildId("test"));
    
    
    // Generate the NodeIds Randomly
    nidFactory = new RandomNodeIdFactory(env);
    
    env.getParameters().setInt("pastry_direct_gtitm_max_overlay_size",numNodes);
//    if (numNodes > 2000) {
    env.getParameters().setString("pastry_direct_gtitm_matrix_file","sample_10k");
//    env.getParameters().setString("pastry_direct_gtitm_matrix_file","sample_2k");
//    }  
    // construct the PastryNodeFactory, this is how we use rice.pastry.direct, with a Euclidean Network
//    factory = new DirectPastryNodeFactory(nidFactory, new GenericNetwork(env), env);
    factory = new DirectPastryNodeFactory(nidFactory, new EuclideanNetwork(env), env);
    
    // loop to construct the nodes/apps
    createNodes();    
  }

  public void startLoggerTask() {
    env.getSelectorManager().getTimer().schedule(new TimerTask() {
      int ctr = 0;
      public void run() {
        testRoutingTables(ctr++);
      }    
    },reportRate,reportRate);
    
    env.getSelectorManager().getTimer().schedule(new TimerTask() {
      public void run() {
        env.destroy();
      }    
    },testTime);  
  }
  
  class CreatorTimerTask extends TimerTask {
    public CreatorTimerTask() {
    }
    
    int ctr = 0;
    public void run() {
      try {
        createNode();
      } catch (Exception ie) {
        ie.printStackTrace(); 
      }
      synchronized(this) {
        ctr++;
        
        if (ctr %100 == 0) {
          System.out.println("Created "+ctr+" nodes."); 
        }
        
        if (ctr >= numNodes) {
          startLoggerTask();          
          cancel();
//          env.getSelectorManager().getTimer().schedule(new TimerTask() {          
//            public void run() {
//              killNodes(numToKill);
//              if (logHeavy)
//                env.getParameters().setInt("rice.pastry.routing.RoutingTable_loglevel", Logger.FINE);
//              
//              env.getSelectorManager().getTimer().schedule(new TimerTask() {
//                public void run() {
//                  testRoutingTables();
//                }    
//              },1000*60,1000*60);
//            }          
//          },100000);
//          notifyAll(); 
        }
      }
    }    
  }
  
  public void createNodes() throws InterruptedException {    
    CreatorTimerTask ctt = new CreatorTimerTask();    
    env.getSelectorManager().getTimer().schedule(ctt,1000,1000); 
//    synchronized(ctt) {
//      while(ctt.ctr < numNodes) {
//        ctt.wait(); 
//      }
//    }
  }
  
  public void sendSomeMessages() {        
    // for each app
    Iterator appIterator = apps.values().iterator();
    while(appIterator.hasNext()) {
      MyApp app = (MyApp)appIterator.next();
      
      // pick a key at random
      Id randId = nidFactory.generateNodeId();
      
      // send to that key
      app.routeMyMsg(randId);
    }
  }
  
  public void sendSomeScribeMessages() {        
    // for each app
    Iterator appIterator = apps.values().iterator();
    while(appIterator.hasNext()) {
      Scribe app = (Scribe)appIterator.next();
      app.publish(topic, new TestScribeContent(topic, 0));
    }
  }
  
  /**
   * Utility class for past content objects
   *
   * @version $Id: ScribeRegrTest.java 3274 2006-05-15 16:17:47Z jeffh $
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


  public PastryNode createNode() throws InterruptedException, IOException {
    NodeHandle bootHandle = null;
    if (nodes.size() > 0) {
      PastryNode bootNode = null;
      while(bootNode == null || !bootNode.isReady()) {
        bootNode = (PastryNode)nodes.get(env.getRandomSource().nextInt(nodes.size())); 
        bootHandle = bootNode.getLocalHandle();
      }
    }
    // construct a node, passing the null boothandle on the first loop will cause the node to start its own ring
    final PastryNode node = factory.newNode();

    node.addObserver(new Observer() {
    
      public void update(Observable o, Object arg) {
        System.out.println("Observing "+o+" "+arg);
      }    
    });
        
    // this will add "magic" to the node such that if it is destroyed, then it will automatically create its replacement
    node.addDestructable(new Destructable() {          
      public void destroy() {
//        System.out.println("Destructable called.");
        nodes.remove(node);
        apps.remove(node);
        try {
          createNode(); // create a new node every time we
                                // destroy one
        } catch (Exception ie) {
          ie.printStackTrace();
        }              
      }          
    });

    node.boot(bootHandle);
    
    if (printLiveness) 
      System.out.println("Creating "+node);
    
    // the node may require sending several messages to fully boot into the ring
//    synchronized(node) {
//      while(!node.isReady()) {
//        // delay so we don't busy-wait
//        node.wait();
////        env.getTimeSource().sleep(100);
//      }
//    }    
    synchronized(node) {
      if (node.isReady()) {
        finishNode(node);
      } else {
//        System.out.println("Adding observer to "+node);
      node.addObserver(new Observer() {

      public void update(Observable o, Object arg) {
//        System.out.println("observer.update("+arg+")");
        if (arg instanceof Boolean) {
          if (!((Boolean) arg).booleanValue()) return;
          
          node.deleteObserver(this);
          finishNode(node);
        } else if (arg instanceof JoinFailedException) {
//          System.out.println("Got JoinFailedException:"+arg);
          node.destroy(); 
        }
      }// update

    });//addObserver
    //    System.out.println("Adding "+node);
    //      nodes.add(node);
      }//if
    }//synchronized
    return node;
  }
  
  public void finishNode(final PastryNode node) {
    nodes.add(node);

    if ((meanSessionTime > 0)) {
      env.getSelectorManager().getTimer().schedule(new TimerTask() {
        {
          node.addDestructable(new Destructable() {          
            public void destroy() {
              cancel();
            }          
          });
        }
        
        @Override
        public void run() {
          if (env.getRandomSource().nextInt(meanSessionTime * 2) == 0) {
            if (printLiveness)
              System.out.println("Destroying " + node);
            // if (!nodes.remove(node)) {
            // String s = "Couldn't remove "+node+" from ";
            // Iterator i = nodes.iterator();
            // while(i.hasNext()) {
            // s+="\t"+i.next()+"\n";
            // }
            //              
            // System.out.print(s);
            // }
            cancel();
            node.destroy();
          }
        }
      }, 60 * 1000, 60 * 1000);
    }
    if (msgSendRate > 0) {
      if (useScribe) {
        Scribe app = new ScribeImpl(node,"test");
        ScribeClient client = new TestScribeClient(app, topic);
        app.subscribe(topic, client);
        apps.put(node,app);
      } else {
        // construct a new MyApp
        MyApp app = new MyApp(node);    
        apps.put(node,app);
      }    
      
      env.getSelectorManager().getTimer().schedule(new TimerTask() {      
        @Override
        public void run() {
          if (useScribe) {
            sendSomeScribeMessages();
          } else {
            sendSomeMessages();
          }
        }      
      }, msgSendRate);
    }
    
    if (rtMaintTime > 0)
      node.scheduleMsg(new InitiateRouteSetMaintenance(),rtMaintTime*1000,rtMaintTime*1000);
        
    if (printLiveness)
      System.out.println("Finished creating new node("+nodes.size()+") "+node+" at "+env.getTimeSource().currentTimeMillis());
 
  }
  
  private void testLeafSets() {
//    if (!logHeavy) return;
    ArrayList<PastryNode> nds = new ArrayList<PastryNode>(nodes);
    Collections.sort(nds,new Comparator<PastryNode>() {
    
      public int compare(PastryNode one, PastryNode two) {
        PastryNode n1 = (PastryNode)one;
        PastryNode n2 = (PastryNode)two;
        return n1.getId().compareTo(n2.getId());
      }
    
    });
    
    Iterator i = nds.iterator();
    while(i.hasNext()) {
      PastryNode n = (PastryNode)i.next(); 
      System.out.println(n.isReady()+" "+n.getLeafSet());
    }
  }
  
  private void testRoutingTables(int round) {    
    if (printLeafSets)
      testLeafSets();
    double streatch;
    int holes;
    if (numNodes <= 1000) {
      streatch = testRoutingTables1();
      holes = testRoutingTables2();
    } else {
      streatch = testRoutingTables1a();
      holes = testRoutingTables2a();      
    }
    System.out.println(round+","+streatch+","+holes+" numNodes:"+nodes.size());
    if (round%50 == 0) {
      PastryNode pn = nodes.get(nodes.size()/2);
      System.out.println(pn.getRoutingTable().printSelf());
      System.out.println(pn.getLeafSet());
      byte[] randomMaterial = new byte[20];
      pn.getEnvironment().getRandomSource().nextBytes(randomMaterial);
      rice.pastry.Id key = rice.pastry.Id.build(randomMaterial);
      System.out.println("Key "+key.toStringBare());
      Iterator<NodeHandle> i = pn.getRouter().getBestRoutingCandidates(key);
      while(i.hasNext()) {
        System.out.println(i.next());
      }
    }
  }
  
  /**
   * 
   * @return delay stretch
   */
  private double testRoutingTables1() {
    
    // for each node
    Iterator nodeIterator = nodes.iterator();
    int ctr = 0;
    double acc = 0;
    
    while(nodeIterator.hasNext()) {
      PastryNode node = (PastryNode)nodeIterator.next();
      RoutingTable rt = node.getRoutingTable();
      Iterator i2 = nodes.iterator();
      while(i2.hasNext()) {
        PastryNode that = (PastryNode)i2.next();
        if ((that != node) && that.isReady() && node.isReady()) {
          NodeHandle thatHandle = that.getLocalHandle();        
          int latency = calcLatency(node,thatHandle);
          int proximity = node.proximity(thatHandle);
          if (proximity == 0) {
            throw new RuntimeException("proximity zero:"+node+".proximity("+thatHandle+")"); 
          }
          if (latency < proximity) { // due to rounding error
            latency = proximity;            
//            calcLatency(node, thatHandle); 
          }          
          double streatch = (1.0*latency)/(1.0*proximity);
//          if (streatch > 3.0) System.out.println("streatch: "+streatch);
          acc+=streatch;
          ctr++;
        }
      }
    }    
//    System.out.println("Time "+env.getTimeSource().currentTimeMillis()+" = "+(acc/ctr));
    return acc/ctr;
  }

  private double testRoutingTables1a() {
    
    // for each node
    int ctr = 0;
    double acc = 0;
    RandomSource rand = env.getRandomSource();
    for (int i = 0; i < 1000000; i++) {
      PastryNode node = (PastryNode)nodes.get(rand.nextInt(nodes.size()));
      RoutingTable rt = node.getRoutingTable();
      PastryNode that = (PastryNode)nodes.get(rand.nextInt(nodes.size()));
      if ((that != node) && that.isReady() && node.isReady()) {
        NodeHandle thatHandle = that.getLocalHandle();        
        int latency = calcLatency(node,thatHandle);
        int proximity = node.proximity(thatHandle);
        if (proximity == 0) {
          throw new RuntimeException("proximity zero:"+node+".proximity("+thatHandle+")"); 
        }
        if (latency < proximity) { // due to rounding error
          latency = proximity;            
//            calcLatency(node, thatHandle); 
        }          
        double streatch = (1.0*latency)/(1.0*proximity);
//          if (streatch > 3.0) System.out.println("streatch: "+streatch);
        acc+=streatch;
        ctr++;
      }
    }
//    System.out.println("Time "+env.getTimeSource().currentTimeMillis()+" = "+(acc/ctr));
    return acc/ctr;
  }

  private int testRoutingTables2() {    
    // for each node
    Iterator nodeIterator = nodes.iterator();
    int curNodeIndex = 0;
    int ctr = 0;
    int[] ctrs = new int[5];
    while(nodeIterator.hasNext()) {      
      PastryNode node = (PastryNode)nodeIterator.next();
      if (!node.isReady()) continue;
      PastryNode temp = DirectPastryNode.setCurrentNode(node);
      RoutingTable rt = node.getRoutingTable();
      Iterator i2 = nodes.iterator();
      while(i2.hasNext()) {
        PastryNode that = (PastryNode)i2.next();
        if (!that.isReady()) continue;
        NodeHandle thatHandle = that.getLocalHandle();
        int response = rt.test(thatHandle);
        if (response > 1) {
          ctrs[response]++;
          ctr++;
          if (logHeavy)
            System.out.println(response+": ("+curNodeIndex+")"+node+" could have held "+thatHandle);    
        }
      }
      DirectPastryNode.setCurrentNode(temp);
      curNodeIndex++;
    }    
//    System.out.println("Time "+env.getTimeSource().currentTimeMillis()+" = "+ctr+"   ENTRY_WAS_DEAD:"+ctrs[2]+" AVAILABLE_SPACE:"+ctrs[3]+" NO_ENTRIES:"+ctrs[4]);
    return ctr;
  }

  private int testRoutingTables2a() {    
    // for each node
    Iterator nodeIterator = nodes.iterator();
    int curNodeIndex = 0;
    int ctr = 0;
    int[] ctrs = new int[5];
    RandomSource rand = env.getRandomSource();
    for (int i = 0; i < 1000000; i++) {
      PastryNode node = (PastryNode)nodes.get(rand.nextInt(nodes.size()));
      if (!node.isReady()) continue;
      PastryNode temp = DirectPastryNode.setCurrentNode(node);
      RoutingTable rt = node.getRoutingTable();
      Iterator i2 = nodes.iterator();
      PastryNode that = (PastryNode)nodes.get(rand.nextInt(nodes.size()));
      
        if (!that.isReady()) continue;
        NodeHandle thatHandle = that.getLocalHandle();
        int response = rt.test(thatHandle);
        if (response > 1) {
          ctrs[response]++;
          ctr++;
          if (logHeavy)
            System.out.println(response+": ("+curNodeIndex+")"+node+" could have held "+thatHandle);    
        }
        
      DirectPastryNode.setCurrentNode(temp);
      curNodeIndex++;
    }    
//    System.out.println("Time "+env.getTimeSource().currentTimeMillis()+" = "+ctr+"   ENTRY_WAS_DEAD:"+ctrs[2]+" AVAILABLE_SPACE:"+ctrs[3]+" NO_ENTRIES:"+ctrs[4]);
    return ctr;
  }

  // recursively calculate the latency
  private int calcLatency(PastryNode node, NodeHandle thatHandle) {
      RoutingTable rt = node.getRoutingTable();
      LeafSet ls = node.getLeafSet();
      thePenalty = 0;
      NodeHandle next = getNextHop(rt, ls, thatHandle, node);
      int penalty = thePenalty;
//      if (penalty > 0) System.out.println("penalty "+thePenalty);
      if (next == thatHandle) return node.proximity(thatHandle);  // base case
      DirectNodeHandle dnh = (DirectNodeHandle)next;    
      PastryNode nextNode = dnh.getRemote();
      return penalty+node.proximity(next)+calcLatency(nextNode, thatHandle); // recursive case
  }
  
  int thePenalty = 0;  // the penalty for trying non-alive nodes
  private NodeHandle getNextHop(RoutingTable rt, LeafSet ls, NodeHandle thatHandle, PastryNode localNode) {
    rice.pastry.Id target = (rice.pastry.Id)thatHandle.getId();

    int cwSize = ls.cwSize();
    int ccwSize = ls.ccwSize();

    int lsPos = ls.mostSimilar(target);

    if (lsPos == 0) // message is for the local node so deliver it
      throw new RuntimeException("can't happen: probably a partition");

    else if ((lsPos > 0 && (lsPos < cwSize || !ls.get(lsPos).getNodeId()
        .clockwise(target)))
        || (lsPos < 0 && (-lsPos < ccwSize || ls.get(lsPos).getNodeId()
            .clockwise(target)))) {

    // the target is within range of the leafset, deliver it directly    
      NodeHandle handle = ls.get(lsPos);

      if (handle.isAlive() == false) {
        // node is dead - get rid of it and try again
        thePenalty += timeToFindFaulty; // rtt*2
        LeafSet ls2 = ls.copy();
        ls2.remove(handle);
        return getNextHop(rt, ls2, thatHandle, localNode);
      } else {
        return handle;
      }
    } else {
      // use the routing table
      RouteSet rs = rt.getBestEntry(target);
      NodeHandle handle = null;

      // apply penalty if node was not alive
      NodeHandle notAlive = null;
      if (rs != null
          && ((notAlive = rs.closestNode(10)) != null)) {
        if ((notAlive != null) && !notAlive.isAlive()) thePenalty+=localNode.proximity(notAlive)*4;
      }
      
      if (rs == null
          || ((handle = rs.closestNode(NodeHandle.LIVENESS_ALIVE)) == null)) {

        // penalize for choosing dead route
        NodeHandle notAlive2 = null;
        notAlive2 = rt.bestAlternateRoute(10,
            target);
        if (notAlive2 == notAlive) {
          // don't doublePenalize 
        } else {
          if ((notAlive2 != null) && !notAlive2.isAlive()) thePenalty+=localNode.proximity(notAlive2)*4;
        }
        
        // no live routing table entry matching the next digit
        // get best alternate RT entry
        handle = rt.bestAlternateRoute(NodeHandle.LIVENESS_ALIVE,
            target);

        if (handle == null) {
          // no alternate in RT, take leaf set extent
          handle = ls.get(lsPos);

          if (handle.isAlive() == false) {
            thePenalty += timeToFindFaulty;
            LeafSet ls2 = ls.copy();
            ls2.remove(handle);
            return getNextHop(rt, ls2, thatHandle, localNode);
          }
        } else {
          Id.Distance altDist = handle.getNodeId().distance(target);
          Id.Distance lsDist = ls.get(lsPos).getNodeId().distance(
              target);

          if (lsDist.compareTo(altDist) < 0) {
            // closest leaf set member is closer
            handle = ls.get(lsPos);

            if (handle.isAlive() == false) {
              thePenalty += timeToFindFaulty;
              LeafSet ls2 = ls.copy();
              ls2.remove(handle);
              return getNextHop(rt, ls2, thatHandle, localNode);
            }
          }
        }
      } //else {
        // we found an appropriate RT entry, check for RT holes at previous node
//      checkForRouteTableHole(msg, handle);
//      }

      return handle;    
    }
  }
  
  public class MyApp implements Application {
    /**
     * The Endpoint represents the underlieing node.  By making calls on the 
     * Endpoint, it assures that the message will be delivered to a MyApp on whichever
     * node the message is intended for.
     */
    protected Endpoint endpoint;
    
    /**
     * The node we were constructed on.
     */
    protected Node node;

    public MyApp(Node node) {
      // We are only going to use one instance of this application on each PastryNode
      this.endpoint = node.buildEndpoint(this, "myinstance");
      
      this.node = node;
          
      // now we can receive messages
      this.endpoint.register();
    }

    /**
     * Getter for the node.
     */
    public Node getNode() {
      return node;
    }
    
    /**
     * Called to route a message to the id
     */
    public void routeMyMsg(Id id) {
      if (logHeavy)
        System.out.println(this+" sending to "+id);    
      Message msg = new MyMsg(endpoint.getId(), id);
      endpoint.route(id, msg, null);
    }
    
    /**
     * Called to directly send a message to the nh
     */
    public void routeMyMsgDirect(NodeHandle nh) {
      if (logHeavy)
        System.out.println(this+" sending direct to "+nh);    
      Message msg = new MyMsg(endpoint.getId(), nh.getId());
      endpoint.route(null, msg, nh);
    }
      
    /**
     * Called when we receive a message.
     */
    public void deliver(Id id, Message message) {
      if (logHeavy)
        System.out.println(this+" received "+message);
    }

    /**
     * Called when you hear about a new neighbor.
     * Don't worry about this method for now.
     */
    public void update(rice.p2p.commonapi.NodeHandle handle, boolean joined) {
    }
    
    /**
     * Called a message travels along your path.
     * Don't worry about this method for now.
     */
    @SuppressWarnings("deprecation")
    public boolean forward(RouteMessage message) {
      if (logHeavy)
        System.out.println(this+"forwarding "+message.getMessage());
      return true;
    }
    
    public String toString() {
      return "MyApp "+endpoint.getId();
    }
  }

  
  class TestScribeClient implements ScribeClient {

    /**
     * DESCRIBE THE FIELD
     */
    protected Scribe scribe;

    /**
     * The topic this client is listening for
     */
    protected Topic topic;

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
    public TestScribeClient(Scribe scribe, Topic topic) {
      this.scribe = scribe;
      this.topic = topic;
      this.acceptAnycast = false;
      this.subscribeFailed = false;
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
      return acceptAnycast;
    }

    /**
     * DESCRIBE THE METHOD
     *
     * @param topic DESCRIBE THE PARAMETER
     * @param content DESCRIBE THE PARAMETER
     */
    public void deliver(Topic topic, ScribeContent content) {

    }

    /**
     * DESCRIBE THE METHOD
     *
     * @param topic DESCRIBE THE PARAMETER
     * @param child DESCRIBE THE PARAMETER
     */
    public void childAdded(Topic topic, rice.p2p.commonapi.NodeHandle child) {
     // System.out.println("CHILD ADDED AT " + scribe.getId());
    }

    /**
     * DESCRIBE THE METHOD
     *
     * @param topic DESCRIBE THE PARAMETER
     * @param child DESCRIBE THE PARAMETER
     */
    public void childRemoved(Topic topic, rice.p2p.commonapi.NodeHandle child) {
     // System.out.println("CHILD REMOVED AT " + scribe.getId());
    }

    public void subscribeFailed(Topic topic) {
      subscribeFailed = true;
    }

    public boolean getSubscribeFailed() {
      return subscribeFailed;
    }
  }
  
  /**
   * Usage: 
   */
  public static void main(String[] args) throws Exception {
//    System.setOut(new PrintStream("out.txt"));
//    System.setErr(System.out);
    
  
    int numNodes = 100;
    int meanSessionTime = 0;
    int useScribeIndex = 1;
    int rtMaintIndex = 0;
    int msgSendRateIndex = 0;
    int numNodesIndex = -1;
    
    if (args.length > 0) {
      numNodes = Integer.parseInt(args[0]);  
    }
    
    if (args.length > 1) {
      meanSessionTime = Integer.parseInt(args[1]);  
    }
    
    int[] rtMaintVals = {0,60,15,1};
    int[] msgSendVals = {0,10000,1000,100};
    int[] numNodesVals = {100,200,500,1000,2000,5000,10000};
//    for (numNodesIndex = 0; numNodesIndex < numNodesVals.length; numNodesIndex++) 
//      for (numNodes = 100; numNodes <= 10000; numNodes*=10) 
//        for (meanSessionTime = 1; meanSessionTime < 10001; meanSessionTime*=10)          
        for (useScribeIndex = 0; useScribeIndex < 2; useScribeIndex++) 
        for (msgSendRateIndex = ((useScribeIndex == 0) ? 0 : 1); msgSendRateIndex < 4; msgSendRateIndex++)
        for (rtMaintIndex = 0; rtMaintIndex < 4; rtMaintIndex++)
          for (int tries = 0; tries < 10; tries++) {
            if (numNodesIndex >= 0) numNodes = numNodesVals[numNodesIndex];
            
            boolean useScribe = true;
            if (useScribeIndex == 0) useScribe = false;
            
            final Object lock = new Object();
            // Loads pastry settings, and sets up the Environment for simulation
            Environment env = Environment.directEnvironment();
            env.addDestructable(new Destructable() {            
              public void destroy() {
                synchronized(lock) {
                  lock.notify();
                }
              }            
            });
            
            if (logHeavy) {
              env.getParameters().setInt("rice.pastry.standard.ConsistentJoinProtocol_loglevel",Logger.FINE); 
              env.getParameters().setInt("rice.pastry.standard.StandardRouteSetProtocol_loglevel",405); 
              env.getParameters().setInt("rice.pastry.standard.StandardRouter_loglevel", Logger.FINE); 
            }

            // launch our node!
//            public RoutingTableTest(int numNodes, int meanSessionTime, int msgSendRate, int rtMaintTime, final Environment env) throws Exception {
            RoutingTableTest dt = new RoutingTableTest(numNodes, meanSessionTime, useScribe, msgSendVals[msgSendRateIndex], rtMaintVals[rtMaintIndex], tries, env);
            synchronized(lock) {
              lock.wait(); // will be notified when the environment is destroyed
            }
          } // tries
  }
}
