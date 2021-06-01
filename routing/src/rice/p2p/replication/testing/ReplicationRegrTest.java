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
package rice.p2p.replication.testing;

import java.io.IOException;
import java.util.*;

import rice.Continuation.ListenerContinuation;
import rice.environment.Environment;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.testing.CommonAPITest;
import rice.p2p.replication.*;
import rice.persistence.MemoryStorage;

/**
 * @(#) ReplicationRegrTest.java Provides regression testing for the replication service using distributed
 * nodes.
 *
 * @version $Id: ReplicationRegrTest.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public class ReplicationRegrTest extends CommonAPITest {

  /**
   * The replication factor to use
   */
  public static final int REPLICATION_FACTOR = 3;
  
  /**
   * the instance name to use
   */
  public static String INSTANCE = "ReplicationRegrTest";

  /**
   * the replication impls in the ring
   */
  protected ReplicationImpl[] replications;

  /**
   * The clients
   */
  protected TestReplicationClient[] clients;

  /**
   * Constructor which sets up all local variables
   */
  public ReplicationRegrTest(Environment env) throws IOException {
    super(env);
    replications = new ReplicationImpl[NUM_NODES];
    clients = new TestReplicationClient[NUM_NODES];
  }


  /**
   * Usage: ReplicationRegrTest [-port p] [-bootstrap host[:port]] [-nodes n] [-protocol (rmi|wire)]
   * [-help]
   *
   * @param args DESCRIBE THE PARAMETER
   */
  public static void main(String args[]) throws IOException {
    Environment env = parseArgs(args);
    ReplicationRegrTest test = new ReplicationRegrTest(env);
    test.start();
    env.destroy();

  }

  /**
   * Method which should process the given newly-created node
   *
   * @param node The newly created node
   * @param num The number of this node
   */
  protected void processNode(int num, Node node) {
    clients[num] = new TestReplicationClient(node);
    replications[num] = new ReplicationImpl(node, clients[num], REPLICATION_FACTOR, INSTANCE);
  }

  /**
   * Method which should run the test - this is called once all of the nodes have been created and
   * are ready.
   */
  protected void runTest() {
    testBasic();
    testMaintenance();
  }

  /*
   *  ---------- Test methods and classes ----------
   */
  
  /**
    * Tests basic functionality
   */
  public void testBasic() {
    int num = environment.getRandomSource().nextInt(NUM_NODES);
    Id id = nodes[num].getId();
    
    IdRange all = FACTORY.buildIdRange(FACTORY.buildId(new byte[20]), FACTORY.buildId(new byte[20]));
    
    sectionStart("Testing Basic Functionality");
    
    stepStart("Inserting Object");
    
    clients[num].insert(id);
    
    stepDone(SUCCESS);
    
    stepStart("Initiating Maintenance");
    
    runMaintenance();
    simulate();
    
    int count = 0;
    
    for (int i=0; i<NUM_NODES; i++)  {
      if (clients[i].scan(all).isMemberId(id)) 
        count++;
    }
  
    assertTrue("Correct number of replicas should be " + (REPLICATION_FACTOR + 1) + " was " + count, 
               count == REPLICATION_FACTOR + 1);
    
    stepDone(SUCCESS);
    
    sectionDone();
  }
  
  /**
    * Tests maintenance functionality
   */
  public void testMaintenance() {
    int num = environment.getRandomSource().nextInt(NUM_NODES);
    Id id = nodes[num].getId();
    
    IdRange all = FACTORY.buildIdRange(FACTORY.buildId(new byte[20]), FACTORY.buildId(new byte[20]));
    
    sectionStart("Testing Basic Functionality");
    
    stepStart("Inserting Object");
    
    clients[num].insert(id);
    
    stepDone(SUCCESS);
    
    stepStart("Initiating Maintenance");
    
    runMaintenance();
    simulate();
    
    int count = 0;
    
    for (int i=0; i<NUM_NODES; i++)  {
      if (clients[i].scan(all).isMemberId(id)) 
        count++;
    }
    
    assertTrue("Correct number of replicas should be " + (REPLICATION_FACTOR + 1) + " was " + count, 
               count == REPLICATION_FACTOR + 1);
    
    stepDone(SUCCESS);
    
    stepStart("Killing Primary Replica");
    
    kill(num);
    
    // wait for notification of failure to propegate
    // No routing involved... but we need to wait long enough for the node
    // to be found faulty.
    waitToRecoverFromKilling(5000);  
    
    stepDone(SUCCESS);
    
    stepStart("Initiating Maintenance");
    
    runMaintenance();
    simulate();
    
    count = 0;
    
    for (int i=0; i<NUM_NODES; i++)  {
      if (clients[i].scan(all).isMemberId(id)) 
        count++;
    }
    
    assertTrue("Correct number of replicas should be " + (REPLICATION_FACTOR + 2) + " was " + count, 
               count == REPLICATION_FACTOR + 2);
    
    stepDone(SUCCESS);

    sectionDone();
  }
  
  public void runMaintenance() {
    for (int i=0; i<NUM_NODES; i++) {
      replications[i].replicate();
    }
    
    simulate();
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

  /**
   * DESCRIBE THE CLASS
   *
   * @version $Id: ReplicationRegrTest.java 4654 2009-01-08 16:33:07Z jeffh $
   * @author amislove
   */
  protected class TestReplicationClient implements ReplicationClient {
    
    public MemoryStorage storage;
    
    public Node node;
    
    public TestReplicationClient(Node node) {
      this.storage = new MemoryStorage(FACTORY);
      this.node = node;
    }
    
    /**
     * This upcall is invoked to notify the application that is should
     * fetch the cooresponding keys in this set, since the node is now
     * responsible for these keys also.
     *
     * @param keySet set containing the keys that needs to be fetched
     */
    public void fetch(IdSet keySet, NodeHandle hint) {
      Iterator i = keySet.getIterator();
      
      while (i.hasNext()) {
        Id next = (Id) i.next();
        storage.store(next, null, next, new ListenerContinuation("Insertion of " + next, environment));
      }
    }
    
    /**
     * This upcall is to notify the application of the range of keys for 
     * which it is responsible. The application might choose to react to 
     * call by calling a scan(complement of this range) to the persistance
     * manager and get the keys for which it is not responsible and
     * call delete on the persistance manager for those objects.
     *
     * @param range the range of keys for which the local node is currently 
     *              responsible  
     */
    public void setRange(IdRange range) {
      IdRange notRange = range.getComplementRange();
      IdSet set = storage.scan(notRange);
      
      Iterator i = set.getIterator();
      
      while (i.hasNext()) {
        Id next = (Id) i.next();
        storage.unstore(next, new ListenerContinuation("Removal of " + next, environment));
      }
    }
    
    /**
     * This upcall should return the set of keys that the application
     * currently stores in this range. Should return a empty IdSet (not null),
     * in the case that no keys belong to this range.
     *
     * @param range the requested range
     */
    public IdSet scan(IdRange range) {
      return storage.scan(range);
    }
    
    public void insert(Id id) {
      storage.store(id, null, id, new ListenerContinuation("Insertion of id " + id, environment));
    }
    
    public String toString() {
      return "TestRepClient "+node.toString();
    }
  }  
}














