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

import java.io.IOException;
import java.util.*;

import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.RangeCannotBeDeterminedException;
import rice.pastry.*;
import rice.pastry.leafset.*;
import rice.pastry.messaging.Message;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * This class tests the correctness of the leafset in Pastry.
 * 
 * @version $Id: LeafSetTest.java 4654 2009-01-08 16:33:07Z jeffh $
 * 
 * @author Alan Mislove
 */
public class LeafSetTest {

  protected NodeIdFactory factory;

  protected RandomSource random;

  public LeafSetTest() {
    Environment env = new Environment();
    random = env.getRandomSource();
    factory = new RandomNodeIdFactory(env);
  }

  public void start() {
    testCumulativeRange();
    testNonCumulativeRange();
    testObservers();
  }

  public void testObservers() {
    int halfLeafSet = 3;
    LeafSet leafset = generateLeafSet(halfLeafSet * 2, halfLeafSet * 2, false);
    System.out.println(leafset);
    leafset.addNodeSetListener(new MyObserver(leafset));

    NodeHandle handle = leafset.get(halfLeafSet);
    System.out.println("Removing " + handle);
    // NodeId nid = (NodeId)handle.getId();
    // NodeHandle nh = handle;
    leafset.remove(handle);

    System.out.println("Adding " + handle);
    leafset.put(handle);

    System.out.println();

    halfLeafSet = 6;
    leafset = generateLeafSet(halfLeafSet * 2, halfLeafSet, false);
    System.out.println(leafset);
    leafset.addNodeSetListener(new MyObserver(leafset));

    for (int j = 0; j < 10; j++) {
      System.out.println();
      for (int i = 0; i < halfLeafSet * 5; i++) {
        handle = new TestNodeHandle(factory.generateNodeId());
        System.out.println("Adding " + handle);
        leafset.put(handle);
      }
      boolean rightSide = false;

      while (leafset.size() > 2) {
        // System.out.println("looping");
        rightSide = !rightSide;
        int r;
        if (rightSide) {
          r = random.nextInt(leafset.cwSize());
          if (r != 0) {
            handle = leafset.get(r);
            System.out.println("Removing " + handle);
            // nid = (NodeId)handle.getId();
            leafset.remove(handle);
            // leafset.remove(r);
          }
        } else {
          r = random.nextInt(leafset.ccwSize());
          if (r != 0) {
            handle = leafset.get(-r);
            System.out.println("Removing " + handle);
            // nid = (NodeId)handle.getId();
            leafset.remove(handle);
            // leafset.remove(-r);
          }
        }
      }
    }
  }

  class MyObserver implements NodeSetListener {
    LeafSet ls;

    public MyObserver(LeafSet ls) {
      this.ls = ls;
    }

    public void nodeSetUpdate(NodeSetEventSource set, NodeHandle handle, boolean added) {
      SimilarSet caller;
      if (set instanceof SimilarSet) {
        caller = (SimilarSet) set;
      }

      if (ls.overlaps() && (ls.ccwSize() != ls.cwSize())) {
        System.out.println("FAILURE: overlaps and different size" + ls);
      }
      if (added) {
        boolean consistent = !ls.directTest(handle);
        if (!consistent) {
          System.out.println("FAILURE:" + handle
              + " was added, but ls is inconsistent.");
          System.out.println(ls);
        } else {
          System.out.println("OK:" + handle + " was added");
        }
      } else { // node was removed
        boolean consistent = !ls.member(handle.getNodeId());
        if (!consistent) {
          System.out.println("FAILURE:" + handle
              + " was removed, but ls is inconsistent.");
          System.out.println(ls);
        } else {
          System.out.println("OK:" + handle + " was removed");
        }
      }
    }

  }

  /**
   * Throws an exception if the test condition is not met.
   */
  protected final void assertTrue(String intention, boolean test) {
    if (!test) {
      System.out.println(intention + " - failed.");
      System.exit(0);
    }
  }

  protected int min(int x, int y) {
    if (y < x)
      return y;
    else
      return x;
  }

  protected int abs(int x) {
    if (x < 0)
      return -x;
    else
      return x;
  }

  public void testCumulativeRange() {
    for (int nodes = 2; nodes < 20; nodes++) {
      for (int size = 2; size < 17; size += 2) {
        LeafSet leafset = generateLeafSet(size, nodes, false);

        System.out.println("Testing cumulative ranges with " + nodes
            + " nodes and leafset size of " + size);

        for (int pos = -min(size / 2, nodes / 2); pos <= min(size / 2,
            nodes / 2); pos++) {

          for (int q = 0; q < size; q++) {
            IdRange range = null;
            try {
              range = leafset.range(leafset.get(pos), q);
            } catch (RangeCannotBeDeterminedException rcbde) {

            }
            if ((q < size / 2 - abs(pos)) || (size + 1 > nodes)) {
              assertTrue("Range of node " + pos + " with q " + q + " nodes "
                  + nodes + " size " + size + " should be defined in leafset "
                  + leafset, range != null);

              if (q >= nodes - 1) {
                assertTrue("Range of node " + pos + " with q " + q + " nodes "
                    + nodes + " size " + size + " should be full in leafset "
                    + leafset, range.isFull());
              } else {
                assertTrue("Range of node " + pos + " with q " + q + " nodes "
                    + nodes + " size " + size + " should be full in leafset "
                    + leafset, !range.isFull());
              }
            } else {
              assertTrue("Range of node " + pos + " with q " + q + " nodes "
                  + nodes + " size " + size
                  + " should be not defined in leafset " + leafset,
                  range == null);
            }
          }
        }
      }
    }
  }

  public void testNonCumulativeRange() {
    for (int nodes = 1; nodes < 20; nodes++) {
      for (int size = 2; size < 17; size += 2) {
        LeafSet leafset = generateLeafSet(size, nodes, false);

        System.out.println("Testing non-cumulative ranges with " + nodes
            + " nodes and leafset size of " + size);

        for (int pos = -min(size / 2, nodes / 2); pos <= min(size / 2,
            nodes / 2); pos++) {

          for (int q = 0; q < size; q++) {
            IdRange range = null;
            try {
              range = leafset.range(leafset.get(pos), q, true);
            } catch (RangeCannotBeDeterminedException rcbde) {
            }
            if ((q < size / 2 - abs(pos)) || (size + 1 > nodes)) {
              assertTrue("Range of node " + pos + " with q " + q + " nodes "
                  + nodes + " size " + size + " should not be null in leafset "
                  + leafset, range != null);

              if (q >= nodes) {
                assertTrue("Range of node " + pos + " with q " + q + " nodes "
                    + nodes + " size " + size + " should be empty in leafset "
                    + leafset, range.isEmpty());
              } else {
                assertTrue("Range of node " + pos + " with q " + q + " nodes "
                    + nodes + " size " + size
                    + " should be defined in leafset " + leafset, !range
                    .isEmpty());
              }
            } else {
              assertTrue("Range of node " + pos + " with q " + q + " nodes "
                  + nodes + " size " + size
                  + " should be not defined in leafset " + leafset,
                  range == null);
            }
          }
        }

        if (size + 1 > nodes) {
          IdRange total = new IdRange();

          for (int q = 0; q < size; q++) {
            total = total.merge(leafset.range(leafset.get(0), q, true));
            total = total.merge(leafset.range(leafset.get(0), q, false));
          }

          assertTrue(
              "Sum of individual ranges should produce entire range with nodes "
                  + nodes + " size " + size + " in leafset " + leafset
                  + " with total " + total, total.isFull());
        }
      }
    }
  }

  /**
   * Returns a leafset of size size out of a network with the specified number
   * of nodes
   * 
   * @param size The size of the leaf set
   * @param nodes The number of nodes in the network
   * @param crossover Whether the leafset must cross over the '0' boundary not
   */
  protected LeafSet generateLeafSet(int size, int nodes, boolean crossover) {
    NodeHandle[] handles = new NodeHandle[nodes];

    for (int i = 0; i < nodes; i++) {
      handles[i] = new TestNodeHandle(factory.generateNodeId());
    }

    Arrays.sort(handles);

    int i = 0;

    if (crossover) {
      i = (nodes - size + random.nextInt(size)) % nodes;
    } else {
      i = random.nextInt(nodes);
    }

    int base = (i + size / 2) % nodes;
    LeafSet set = new LeafSet(handles[base], size, null);

    for (int j = 0; j < nodes; j++) {
      set.put(handles[j]);
    }

    return set;
  }

  public static void main(String args[]) {
    LeafSetTest test = new LeafSetTest();
    test.start();
  }

  public static class TestNodeHandle extends NodeHandle implements Comparable<TestNodeHandle> {
    private Id id;

    public TestNodeHandle(Id id) {
      this.id = id;
    }

    public Id getNodeId() {
      return id;
    }

    public int getLiveness() {
      return LIVENESS_ALIVE;
    }

    /**
     * @deprecated
     */
    public int proximity() {
      return 1;
    }

    public boolean ping() {
      return true;
    }

    public boolean equals(Object obj) {
      if (obj instanceof TestNodeHandle) {
        return ((TestNodeHandle) obj).id.equals(id);
      }

      return false;
    }

    public int hashCode() {
      return id.hashCode();
    }

    /**
     * @deprecated
     */
    public void receiveMessage(Message m) {
    };

    public int compareTo(TestNodeHandle o) {
      return id.compareTo(((TestNodeHandle) o).id);
    }

    public String toString() {
      return id.toString();
    }

    public void serialize(OutputBuffer buf) throws IOException {
      throw new RuntimeException("not implemented.");        
    }
  }
}
