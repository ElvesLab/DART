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

package rice.p2p.past.testing;

import rice.*;

import rice.environment.Environment;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.commonapi.testing.*;
import rice.p2p.past.*;
import rice.p2p.past.messaging.*;
import rice.p2p.past.rawserialization.*;
import rice.p2p.replication.*;

import rice.persistence.*;

import java.io.*;
import java.util.*;
import java.net.*;
import java.io.Serializable;

/**
 * @(#) PastRegrTest.java
 *
 * Provides regression testing for the Past service
 *
 * @version $Id: PastRegrTest.java 3157 2006-03-19 12:16:58Z jeffh $
 *
 * @author Alan Mislove
 */

@SuppressWarnings("unchecked")
public class RawPastRegrTest extends CommonAPITest {

  // the instance name to use
  public static String INSTANCE = "PastRegrTest";
  
  // the replication factor in Past
  public static final int REPLICATION_FACTOR = 3;

  // the storage services in the ring
  protected StorageManager storages[];
  
  // the past impls in the ring
  protected PastImpl pasts[];

  protected boolean running = true;
  /**
   * Constructor which sets up all local variables
   */
  public RawPastRegrTest(Environment env) throws IOException {
    super(env);
    pasts = new PastImpl[NUM_NODES];
    storages = new StorageManager[NUM_NODES];
    
    if (PROTOCOL == PROTOCOL_DIRECT) {
      new Thread() {
        public void run() {
          while (running) {
            try {
              sleep(50);
              simulate();
            } catch (Exception e) {
              System.out.println(e + " blah");
            }
          }
        }
      }.start();
    }
  }

  /**
   * Method which should process the given newly-created node
   *
   * @param node The newly created node
   * @param num The number of this node
   */
  protected void processNode(int num, Node node) {
    try {
      storages[num] = new StorageManagerImpl(FACTORY,
                                             new PersistentStorage(FACTORY, "root-" + num, ".", 1000000, environment),
                                             new LRUCache(new MemoryStorage(FACTORY), 100000, environment));
      pasts[num] = new PastImpl(node, storages[num], REPLICATION_FACTOR, INSTANCE);
      pasts[num].setContentDeserializer(new PastContentDeserializer() {
      
        public PastContent deserializePastContent(InputBuffer buf, Endpoint endpoint,
            short contentType) throws IOException {
          switch(contentType) {
            case TestPastContent.TYPE:
              return new TestPastContent(buf, endpoint, this);
            case VersionedTestPastContent.TYPE:
              return new VersionedTestPastContent(buf, endpoint, this);              
            case NonOverwritingTestPastContent.TYPE:
              return new NonOverwritingTestPastContent(buf, endpoint, this);              
          }
          throw new IllegalArgumentException("Unknown type:"+contentType);
        }      
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    pasts[num].setContentHandleDeserializer(new PastContentHandleDeserializer() {    
      public PastContentHandle deserializePastContentHandle(InputBuffer buf, Endpoint endpoint,
          short contentType) throws IOException {
        switch(contentType) {
          case TestPastContentHandle.TYPE:
            return new TestPastContentHandle(buf, endpoint);
        }
        throw new IllegalArgumentException("Unknown type:"+contentType);
      }    
    });
  }

  /**
   * Method which should run the test - this is called once all of the
   * nodes have been created and are ready.
   */
  protected void runTest() {
    if (NUM_NODES < 2) {
      System.out.println("The DistPastRegrTest must be run with at least 2 nodes for proper testing.  Use the '-nodes n' to specify the number of nodes.");
      return;
    }
    
    // Run each test
    testRouteRequest();
  }

  /* ---------- Test methods and classes ---------- */

  /**
   * Tests routing a Past request to a particular node.
   */
  protected void testRouteRequest() {
    final PastImpl local = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    final PastImpl remote = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    final Id remoteId = remote.getLocalNodeHandle().getId();
    final PastContent file = new TestPastContent(remoteId);

    sectionStart("Simple Route Request");

    // Check file doesn't exist
    stepStart("Initial Lookup");
    local.lookup(remoteId, new TestCommand() {
      public void receive(Object result) throws Exception {
        assertTrue("File returned should be null", result == null);
        stepDone();

        // Insert file
        stepStart("File Insertion");
        local.insert(file, new TestCommand() {
          public void receive(Object result) throws Exception {
            assertTrue("Insert of file result should not be null", result != null);
            assertTrue("Insert of file should return Boolean[]", result instanceof Boolean[]);
       //     assertTrue("Insert of file should return correct sized Boolean[]", (((Boolean[]) result).length == REPLICATION_FACTOR) ||
       //               ((NUM_NODES < REPLICATION_FACTOR) &&
       //                (((Boolean[]) result).length) == NUM_NODES));

            for (int i=0; i<((Boolean[]) result).length; i++) {
              assertTrue("Insert of file should not return null at replica", ((Boolean[]) result)[i] != null);
              assertTrue("Insert of file should succeed at replica", ((Boolean[]) result)[i].booleanValue());
            }

            stepDone();

            // run replica maintenance
            runReplicaMaintence();

            // Check file exists
            stepStart("Remote File Lookup");
            local.lookup(remoteId, new TestCommand() {
              public void receive(Object result) throws Exception {
                assertTrue("File should not be null", result != null);
                assertEquals("Lookup of file should be correct",
                             file,
                             result);
                stepDone();

                // Lookup file locally
                stepStart("Local File Lookup");
                remote.getStorageManager().getObject(remoteId, new TestCommand() {
                  public void receive(Object result) throws Exception {
                    assertTrue("File should be inserted at known node",
                               result != null);
                    assertEquals("Retrieved local file should be the same",
                                 file, result);

                    stepDone();
                    sectionDone();

                    testVersionControl();
                  }
                });
                simulate();
              }
            });
            simulate();
          }
        });
        simulate();
      }
    });
    simulate();
  }

  /**
    * Tests overwriting an exiting object with a new one
   */
  protected void testVersionControl() {
    final PastImpl local = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    final PastImpl remote = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    final Id remoteId = remote.getLocalNodeHandle().getId();
    final PastContent oldFile = new VersionedTestPastContent(remoteId, 0);
    final PastContent newFile = new VersionedTestPastContent(remoteId, 1);
    final PastContent newNewFile = new NonOverwritingTestPastContent(remoteId, 2);
    
    sectionStart("Version Control");

    // Insert file
    stepStart("File Insertion");
    local.insert(oldFile, new TestCommand() {
      public void receive(Object result) throws Exception {
        assertTrue("Insert of file result should not be null", result != null);
        assertTrue("Insert of file should return Boolean[]", result instanceof Boolean[]);
     //   assertTrue("Insert of file should return correct sized Boolean[]", (((Boolean[]) result).length == REPLICATION_FACTOR) ||
     //              ((NUM_NODES < REPLICATION_FACTOR) &&
     //               (((Boolean[]) result).length) == NUM_NODES));

        for (int i=0; i<((Boolean[]) result).length; i++) {
          assertTrue("Insert of file should not return null at replica", ((Boolean[]) result)[i] != null);
          assertTrue("Insert of file should succeed at replica", ((Boolean[]) result)[i].booleanValue());
        }
        
        stepDone();

        // run replica maintenance
        runReplicaMaintence();

        // Check file exists
        stepStart("Remote File Lookup");
        local.lookup(remoteId, new TestCommand() {
          public void receive(Object result) throws Exception {
            assertTrue("File should not be null", result != null);
            assertEquals("Lookup of file should be correct",
                         oldFile,
                         result);
            stepDone();

            // Insert overwriting file
            stepStart("Overwriting File Insertion");
            local.insert(newFile, new TestCommand() {
              public void receive(Object result) throws Exception {
                assertTrue("Insert of file result should not be null", result != null);
                assertTrue("Insert of file should return Boolean[]", result instanceof Boolean[]);
          //      assertTrue("Insert of file should return correct sized Boolean[]", (((Boolean[]) result).length == REPLICATION_FACTOR) ||
          //                 ((NUM_NODES < REPLICATION_FACTOR) &&
          //                  (((Boolean[]) result).length) == NUM_NODES));

                for (int i=0; i<((Boolean[]) result).length; i++) {
                  assertTrue("Insert of file should not return null at replica", ((Boolean[]) result)[i] != null);
                  assertTrue("Insert of file should succeed at replica", ((Boolean[]) result)[i].booleanValue());
                }

                stepDone();

                // run replica maintenance
                runReplicaMaintence();

                // Check correct file exists
                stepStart("Remote Overwriting File Lookup");
                local.lookup(remoteId, new TestCommand() {
                  public void receive(Object result) throws Exception {
                    assertTrue("Overwriting file should not be null", result != null);
                    assertEquals("Lookup of overwriting file should be correct version",
                                 newFile,
                                 result);
                    stepDone();

                    // Insert overwriting file
                    stepStart("Non-overwriting File Insertion");
                    local.insert(newNewFile, new TestCommand() {
                      public void receive(Object result) throws Exception {
                        assertTrue("Insert of file result should not be null", result != null);
                        assertTrue("Insert of file should return Boolean[]", result instanceof Boolean[]);
                  //      assertTrue("Insert of file should return correct sized Boolean[]", (((Boolean[]) result).length == REPLICATION_FACTOR) ||
                  //                 ((NUM_NODES < REPLICATION_FACTOR) &&
                 //                   (((Boolean[]) result).length) == NUM_NODES));

                        for (int i=0; i<((Boolean[]) result).length; i++) {
                          assertTrue("Insert of file should not return null at replica", ((Boolean[]) result)[i] != null);
                          assertTrue("Insert of file should succeed at replica", ((Boolean[]) result)[i].booleanValue());
                        }
                        
                        stepDone();

                        // run replica maintenance
                        runReplicaMaintence();

                        // Check correct file exists
                        stepStart("Remote Non-Overwriting File Lookup");
                        local.lookup(remoteId, new TestCommand() {
                          public void receive(Object result) throws Exception {
                            assertTrue("Non-Overwriting file should not be null", result != null);
                            assertEquals("Lookup of non-overwriting file should be correct (second) version",
                                         newFile,
                                         result);
                            stepDone();
                            sectionDone();

                            testFetch();
                          }
                        });
                        simulate();
                      }
                    });
                    simulate();
                  }
                });
                simulate();
              }
            });
            simulate();
          }
        });
        simulate();
      }
    });
    simulate();
  }

  /**
    * Tests the parameter checking in Past.
   *
  protected void testParameterChecks() {
    final PastImpl local = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    final Id localId = local.getLocalNodeHandle().getId();
    
    sectionStart("Parameter Checks Testing");

    // Null insert should fail
    stepStart("Insertion Of Null");
    local.insert(null, new TestExceptionCommand() {
      public void receive(Object result) throws Exception {
        assertTrue("Exception returned should not be null", result != null);
        assertTrue("Exception should be return", result instanceof Exception);
        stepDone();

        // Null lookup should fail
        stepStart("Lookup Of Null");
        local.lookup(null, new TestExceptionCommand() {
          public void receive(Object result) throws Exception {
            assertTrue("Exception returned should not be null", result != null);
            assertTrue("Exception should be return", result instanceof Exception);
            stepDone();
            
            // Null fetch should fail
            stepStart("Fetch Of Null");
            local.fetch((PastContentHandle) null, new TestExceptionCommand() {
              public void receive(Object result) throws Exception {
                assertTrue("Exception returned should not be null", result != null);
                assertTrue("Exception should be return", result instanceof Exception);
                stepDone();

                // Null lookup handles should fail
                stepStart("Lookup Handles Of Null");
                local.lookupHandles(null, 1, new TestExceptionCommand() {
                  public void receive(Object result) throws Exception {
                    assertTrue("Exception returned should not be null", result != null);
                    assertTrue("Exception should be return", result instanceof Exception);
                    stepDone();
                    
                    // Lookup handles of -4 should fail
                    stepStart("Lookup Handles Of -4");
                    local.lookupHandles(localId, -4, new TestExceptionCommand() {
                      public void receive(Object result) throws Exception {
                        assertTrue("Exception returned should not be null", result != null);
                        assertTrue("Exception should be return", result instanceof Exception);
                        stepDone();

                        // Lookup handles of 0 should fail
                        stepStart("Lookup Handles Of 0");
                        local.lookupHandles(localId, 0, new TestExceptionCommand() {
                          public void receive(Object result) throws Exception {
                            assertTrue("Exception returned should not be null", result != null);
                            assertTrue("Exception should be return", result instanceof Exception);
                            stepDone();

                            sectionDone();
                            testFetch();
                          }
                        });
                        simulate();
                      }
                    });
                    simulate();
                  }
                });
                simulate();
              }
            });
            simulate();
          }
        });
        simulate();
      }
    });
    simulate();
  }*/

  /**
   * Tests the fetch function in Past.
   */
  protected void testFetch() {
    final PastImpl local = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    final PastImpl remote1 = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    PastImpl tmp = pasts[environment.getRandomSource().nextInt(NUM_NODES)];

    while (tmp == remote1) {
      tmp = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    }

    final PastImpl remote2 = tmp;
    
    final Id id = pasts[environment.getRandomSource().nextInt(NUM_NODES)].getLocalNodeHandle().getId();
    final PastContent file1 = new VersionedTestPastContent(id, 1);
    final PastContent file2 = new VersionedTestPastContent(id, 2);

    final PastContentHandle handle1 = new TestPastContentHandle(remote1, id);
    final PastContentHandle handle2 = new TestPastContentHandle(remote2, id);

    sectionStart("Fetch Testing");

    // Insert file
    stepStart("File 1 Insertion");
    remote1.getStorageManager().store(id, null, file1, new TestCommand() {
      public void receive(Object result) throws Exception {
        assertTrue("Storage of file 1 should succeed", ((Boolean)result).booleanValue());

        stepDone();

        // Insert second file
        stepStart("File 2 Insertion");
        remote2.getStorageManager().store(id, null, file2, new TestCommand() {
          public void receive(Object result) throws Exception {
            assertTrue("Storage of file 2 should succeed", ((Boolean)result).booleanValue());

            stepDone();

            // Retrieve first file
            stepStart("File 1 Fetch");
            local.fetch(handle1, new TestCommand() {
              public void receive(Object result) throws Exception {
                assertTrue("Result should be non-null", result != null);
                assertEquals("Result should be correct", file1, result);
                assertTrue("Result should not be file 2", (! file2.equals(result)));

                final Object received1 = result;

                stepDone();

                // Retrieve second file
                stepStart("File 2 Fetch");
                local.fetch(handle2, new TestCommand() {
                  public void receive(Object result) throws Exception {
                    assertTrue("Result should be non-null", result != null);
                    assertEquals("Result should be correct", file2, result);
                    assertTrue("Result should not be file 1", (! file1.equals(result)));

                    final Object received2 = result;

                    stepDone();

                    // ensure different
                    stepStart("File 1 and 2 Different");
                    assertTrue("Files should not be equal", (! received1.equals(received2)));
                    stepDone();

                    // remove file
                    stepStart("File 1 Removal");
                    remote1.getStorageManager().unstore(id, new TestCommand() {
                      public void receive(Object result) throws Exception {
                        assertTrue("Removal of file 1 should succeed", ((Boolean)result).booleanValue());

                        stepDone();

                        // remove second file
                        stepStart("File 2 Removal");
                        remote2.getStorageManager().unstore(id, new TestCommand() {
                          public void receive(Object result) throws Exception {
                            assertTrue("Removal of file 2 should succeed", ((Boolean)result).booleanValue());

                            stepDone();
                            sectionDone();

                            testLookupHandles();
                          }
                        });
                        simulate();
                      }
                    });
                    simulate();
                  }
                });
                simulate();
              }
            });
            simulate();
          }
        });
        simulate();
      }
    });
    simulate();
  }
  
  /**
   * Tests the lookup handles function in Past.
   */
  protected void testLookupHandles() {
    final PastImpl local = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    final PastImpl remote = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    final Id remoteId = remote.getLocalNodeHandle().getId();
    final PastContent file = new TestPastContent(remoteId);

    sectionStart("Lookup Handles Testing");

    // Insert file
    stepStart("File Insertion");
    local.insert(file, new TestCommand() {
      public void receive(Object result) throws Exception {
        assertTrue("Insert of file result should not be null", result != null);
        assertTrue("Insert of file should return Boolean[]", result instanceof Boolean[]);
   //     assertTrue("Insert of file should return correct sized Boolean[]", (((Boolean[]) result).length == REPLICATION_FACTOR) ||
   //               ((NUM_NODES < REPLICATION_FACTOR) &&
   //                 (((Boolean[]) result).length) == NUM_NODES));

//        System.out.println("PastRegrTest.testLookupHandles() insert result.length:"+((Boolean[]) result).length);
        for (int i=0; i<((Boolean[]) result).length; i++) {
          assertTrue("Insert of file should not return null at replica", ((Boolean[]) result)[i] != null);
          assertTrue("Insert of file should succeed at replica", ((Boolean[]) result)[i].booleanValue());
        }
        
        stepDone();
        
        // run replica maintenance
        runReplicaMaintence();

        // Check file exists (at 1 replica)
        stepStart("Remote Handles Lookup - 1 Replica");
        local.lookupHandles(remoteId, 1, new TestCommand() {
          public void receive(Object result) throws Exception {
            assertTrue("Replicas should not be null", result != null);
            assertTrue("Replicas should be handle[]", result instanceof PastContentHandle[]);
            assertTrue("Only 1 replica should be returned", ((PastContentHandle[]) result).length == 1);
            if (((PastContentHandle[]) result)[0] == null) {
              System.out.println("PastRegrTest.problem");
              
            }
            assertEquals("Replica should be for right object", remoteId, ((PastContentHandle[]) result)[0].getId());

            stepDone();

            // Check file exists (at all replicas)
            stepStart("Remote Handles Lookup - All Replicas");
            local.lookupHandles(remoteId, REPLICATION_FACTOR+1, new TestCommand() {
              public void receive(Object result) throws Exception {
                assertTrue("Replicas should not be null", result != null);
                assertTrue("Replicas should be handle[]", result instanceof PastContentHandle[]);

                PastContentHandle[] handles = (PastContentHandle[]) result;

                assertTrue("All replicas should be returned", (handles.length == REPLICATION_FACTOR+1) ||
                           ((NUM_NODES < REPLICATION_FACTOR+1) && (handles.length) == NUM_NODES));

                for (int i=0; i<handles.length; i++) {
                  assertTrue("Replica " + i + " should not be null", handles[i] != null);
                  assertEquals("Replica " + i + " should be for right object", remoteId, handles[i].getId());
                }

                for (int i=0; i<handles.length; i++) {
                  for (int j=0; j<handles.length; j++) {
                    if (i != j) {
                      assertTrue("Handles " + handles[i] + " and " + handles[j] + " should be different",
                                 (! handles[i].getNodeHandle().getId().equals(handles[j].getNodeHandle().getId())));
                    }
                  }
                }

                stepDone();

                // Check file exists (at a huge number of replicas)
                stepStart("Remote Handles Lookup - 12 Replicas");
                local.lookupHandles(remoteId, 12, new TestCommand() {
                  public void receive(Object result) throws Exception {
                    assertTrue("Replicas should not be null", result != null);
                    assertTrue("Replicas should be handle[]", result instanceof PastContentHandle[]);

                    PastContentHandle[] handles = (PastContentHandle[]) result;

                    assertTrue("All replicas should be returned, got " + handles.length, (handles.length >= REPLICATION_FACTOR+1) ||
                               ((NUM_NODES < REPLICATION_FACTOR+1) && (handles.length) == NUM_NODES));

                    int count = 0;
                    
                    for (int i=0; i<handles.length; i++) {
                      if (handles[i] != null) {
                        assertEquals("Replica " + i + " should be for right object", remoteId, handles[i].getId());
                        count++;
                      }
                    }

                    assertTrue("All replicas should be returned (got " + count + "/" + (REPLICATION_FACTOR+1) + ")", count == REPLICATION_FACTOR+1);

                    
                    for (int i=0; i<handles.length; i++) {
                      for (int j=0; j<handles.length; j++) {
                        if ((i != j) && (handles[i] != null) && (handles[j] != null)) {
                          assertTrue("Handles " + handles[i] + " and " + handles[j] + " should be different",
                                     (! handles[i].getNodeHandle().getId().equals(handles[j].getNodeHandle().getId())));
                        }
                      }
                    }

                    stepDone();
                    sectionDone();

                    testCaching();
                  }
                });
                simulate();
              }
            });
            simulate();
          }
        });
        simulate();
      }
    });
    simulate();
  }

  /**
    * Tests the dynamic caching function in Past.
   */
  protected void testCaching() {
    final PastImpl local = pasts[environment.getRandomSource().nextInt(NUM_NODES)];
    final Id id1 = generateId();
    final Id id2 = generateId();
    final PastContent file1 = new TestPastContent(id1);
    final PastContent file2 = new TestPastContent(id2);
    final PastContent file3 = new NonMutableTestPastContent(id2);

    sectionStart("Caching Testing");

    // Manually insert file
    stepStart("Manually Inserting Object Into Cache");

    // check cache
    local.getStorageManager().getCache().cache(id1, null, file1, new TestCommand() {
      public void receive(Object result) throws Exception {
        assertTrue("Object should not be null", result != null);
        assertTrue("Object should be True", result.equals(new Boolean(true)));

        stepDone();

        // Check file exists
        stepStart("Local Lookup Satisfied by Cache");
        local.lookup(id1, new TestCommand() {
          public void receive(Object result) throws Exception {
            assertTrue("File should not be null", result != null);
            assertEquals("Lookup of file should be correct",
                         file1,
                         result);
            stepDone();

            // Insert file
            stepStart("Caching Mutable Object");
            final LookupMessage lmsg = new LookupMessage(1, id2, local.getLocalNodeHandle(), id2);
            lmsg.receiveResult(file2);

            assertTrue("Message should continue to be routed",
                       local.forward(new TestRouteMessage(id2, null, lmsg)));

            stepDone();

            stepStart("Cache Shouldn't Contain Object");

            // check cache
            local.getStorageManager().getObject(id2, new TestCommand() {
              public void receive(Object result) throws Exception {
                assertTrue("Object should be null", result == null);

                stepDone();

                stepStart("Caching Non-Mutable Object");

                lmsg.receiveResult(file3);
                assertTrue("Message should continue to be routed",
                           local.forward(new TestRouteMessage(id2, null, lmsg)));

                stepDone();

                stepStart("Cache Should Contain Object");

                // check cache
                local.getStorageManager().getObject(id2, new TestCommand() {
                  public void receive(Object result) throws Exception {
                  //  assertTrue("Object should not be null", result != null);
                  //  assertTrue("Object should be correct", result.equals(file3));

                    stepDone();

                    // check lookup
                    LookupMessage lmsg = new LookupMessage(-1, id2, local.getLocalNodeHandle(), id2);

                    stepStart("Lookup Satisfied By Cache");
                  //  assertTrue("Message should not continue to be routed",
                  //             ! local.forward(new TestRouteMessage(id2, null, lmsg)));
                    stepDone();

                    sectionDone();
                    
                    cleanUp();
                  }
                });
                simulate();
              }
            });
            simulate();
          }
        });
        simulate();
      }
    });
    simulate();
  }

  /**
   * Private method which initiates the replica maintenance on all
   * of the nodes
   */
  private void runReplicaMaintence() {
    for (int i=0; i<NUM_NODES; i++) {
      pasts[i].getReplication().replicate();
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
   * Usage: DistPastTest [-port p] [-bootstrap host[:port]] [-nodes n] [-protocol (direct|socket)] [-help]
   */
  public static void main(String args[]) throws Exception {
//    System.setOut(new PrintStream(new FileOutputStream("pastrtest.txt")));
//    System.setErr(System.out);
//    while(true) {
      LinkedList delme = new LinkedList();
      delme.add(new File("FreePastry-Storage-Root"));
      while(!delme.isEmpty()) {
        File f = (File)delme.removeFirst();
        if (f.isDirectory()) {
          File[] subs = f.listFiles();
          if (subs.length == 0) {
            f.delete(); 
          } else {
            delme.addAll(Arrays.asList(subs));          
            delme.addLast(f);          
          }
        } else {
          f.delete(); 
        }
      }
      
      Environment env = parseArgs(args);
      env.getParameters().setDouble("p2p_past_successfulInsertThreshold",1.0);
      RawPastRegrTest pastTest = new RawPastRegrTest(env);
      pastTest.start();
      
//      synchronized(pastTest) {
//        pastTest.wait();  
//      }
//    }
  }

  protected void cleanUp() {
    running = false;
    environment.destroy();  
//    synchronized(this) {
//      this.notifyAll();
//    }
  }
  
  /**
   * Common superclass for test commands.
   */
  protected class TestCommand implements Continuation {
    public void receiveResult(Object result) {
      try {
        receive(result);
      }
      catch (Exception e) {
        receiveException(e);
      }
    }
    public void receive(Object result) throws Exception {}
    public void receiveException(Exception e) {
      stepException(e);
    }
  }

  /**
    * Common superclass for test commands which should throw an exception
   */
  protected class TestExceptionCommand implements Continuation {
    public void receiveResult(Object result) {
      stepDone(FAILURE, "Command should throw an exception - got " + result);
    }
    public void receive(Object result) throws Exception {}
    public void receiveException(Exception e) {
      try {
        receive(e);
      }
      catch (Exception ex) {
        receiveException(ex);
      }
    }
  }

  /**
   * Utility class for past content objects
   */
  protected static class TestPastContent implements RawPastContent {
    public static final short TYPE = 1;
        
    protected Id id;

    protected RawPastContent existing;
    
    public TestPastContent(Id id) {
      this.id = id;
    }

    public PastContent checkInsert(Id id, PastContent existingContent) throws PastException {
      existing = (RawPastContent)existingContent;
      return this;
    }

    public PastContentHandle getHandle(Past past) {
      return new TestPastContentHandle(past, id);
    }

    public Id getId() {
      return id;
    }

    public boolean isMutable() {
      return true;
    }

    public boolean equals(Object o) {
      if (! (o instanceof TestPastContent)) return false;

      return ((TestPastContent) o).id.equals(id);
    }

    public String toString() {
      return "TestPastContent(" + id + ")";
    }

    public short getType() {
      return TYPE;
    }

    public TestPastContent(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
      id = endpoint.readId(buf, buf.readShort());
      if (buf.readBoolean()) {
        short contentType = buf.readShort();
        existing = (RawPastContent)pcd.deserializePastContent(buf, endpoint, contentType);
      }
    }
    
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeShort(id.getType());
      id.serialize(buf);
      if (existing == null) {
        buf.writeBoolean(false); 
      } else {
        buf.writeBoolean(true);
        buf.writeShort(existing.getType());
        existing.serialize(buf);
      }      
    }
  }

  protected static class VersionedTestPastContent extends TestPastContent {
    public static final short TYPE = 2;
    
    protected int version = 0;

    public VersionedTestPastContent(Id id, int version) {
      super(id);
      this.version = version;
    }

    public boolean equals(Object o) {
      if (! (o instanceof VersionedTestPastContent)) return false;

      return (((VersionedTestPastContent) o).id.equals(id) &&
              (((VersionedTestPastContent) o).version == version));
    }

    public String toString() {
      return "VersionedTestPastContent(" + id + ", " + version + ")";
    }
    
    public short getType() {
      return TYPE; 
    }
    
    public VersionedTestPastContent(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
      super(buf, endpoint, pcd);
      version = buf.readInt();
    }
    
    public void serialize(OutputBuffer buf) throws IOException {
      super.serialize(buf);
      buf.writeInt(version);
    }
  }
  
  protected static class NonOverwritingTestPastContent extends VersionedTestPastContent {
    public static final short TYPE = 3;

    public NonOverwritingTestPastContent(Id id, int version) {
      super(id, version);
    }
    
    public PastContent checkInsert(Id id, PastContent existingContent) throws PastException {
      return existingContent;
    }
    
    public String toString() {
      return "NonOverwritingTestPastContent(" + id + ", " + version + ")";
    }
    
    public short getType() {
      return TYPE; 
    }
    
    public NonOverwritingTestPastContent(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
      super(buf, endpoint, pcd);
    }
    
    
    public boolean equals(Object o) {
      if (! (o instanceof NonOverwritingTestPastContent)) return false;

      return (((NonOverwritingTestPastContent) o).id.equals(id) &&
              (((NonOverwritingTestPastContent) o).version == version));
    }
  }

  protected static class NonMutableTestPastContent extends TestPastContent {

    public NonMutableTestPastContent(Id id) {
      super(id);
    }

    public boolean isMutable() {
      return false;
    }

    public boolean equals(Object o) {
      if (! (o instanceof NonMutableTestPastContent)) return false;

      return ((NonMutableTestPastContent) o).id.equals(id);
    }
  }

  /**
    * Utility class for past content object handles
   */
  protected static class TestPastContentHandle implements RawPastContentHandle {
    public static final short TYPE = 1;
    
    protected NodeHandle handle;

    protected Id id;

    public TestPastContentHandle(Past past, Id id) {
      this.handle = past.getLocalNodeHandle();
      this.id = id;
    }

    public Id getId() {
      return id;
    }

    public NodeHandle getNodeHandle() {
      return handle;
    }
    
    public short getType() {
      return TYPE; 
    }
    
    public TestPastContentHandle(InputBuffer buf, Endpoint endpoint) throws IOException {
      handle = endpoint.readNodeHandle(buf);
      id = endpoint.readId(buf, buf.readShort());
    }
    
    public void serialize(OutputBuffer buf) throws IOException {
      handle.serialize(buf);
      buf.writeShort(id.getType());
      id.serialize(buf);
    }
  }

  /**
   * Utility class which simulates a route message
   */
  protected static class TestRouteMessage implements RouteMessage {

    private Id id;

    private NodeHandle nextHop;

    private Message message;
    
    public TestRouteMessage(Id id, NodeHandle nextHop, Message message) {
      this.id = id;
      this.nextHop = nextHop;
      this.message = message;
    }
    
    public Id getDestinationId() {
      return id;
    }

    public NodeHandle getNextHopHandle() {
      return nextHop;
    }

    /**
     * @deprecated
     */
    public Message getMessage() {
      return message;
    }

    public Message getMessage(MessageDeserializer md) {
      return message;
    }

    public void setDestinationId(Id id) {
      this.id = id;
    }

    public void setNextHopHandle(NodeHandle nextHop) {
      this.nextHop = nextHop;
    }

    public void setMessage(Message message) {
      this.message = message;
    }
    
    public void setMessage(RawMessage message) {
      this.message = message;
    }
  }
}
