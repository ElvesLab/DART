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
package rice.pastry.routing;

import rice.p2p.commonapi.rawserialization.*;
import rice.pastry.*;

import java.util.*;
import java.io.*;

/**
 * A set of nodes typically stored in the routing table. The set contains a
 * bounded number of the closest node handles. Since proximity value can change
 * unpredictably, we don't keep the set in sorted order.
 * 
 * @version $Id: RouteSet.java 4060 2007-12-21 17:49:36Z jeffh $
 * 
 * @author Andrew Ladd
 * @author Peter Druschel
 */

public class RouteSet implements NodeSetI, Serializable,
    Observer, Iterable<NodeHandle> {
  public static final short TYPE = 2;

  private static final long serialVersionUID = 8156336294555109590L;

  private NodeHandle[] nodes;

  private int theSize;

  private int closest;

  transient int row;
  transient int col;
  transient PastryNode localNode;
  
  /**
   * Constructor.
   * 
   * @param maxSize the maximum number of nodes that fit in this set.
   */

  public RouteSet(int maxSize, int row, int col, PastryNode local, NodeHandle initialVal) {
    this(maxSize, row, col, local);
    nodes[theSize++] = initialVal;    
    closest = 0;
  }
  
  public RouteSet(int maxSize, int row, int col, PastryNode local) {
    this.localNode = local;
    nodes = new NodeHandle[maxSize];
    theSize = 0;
    closest = -1;
    this.row = row;
    this.col = col;
  }

  public String toString() {
    
    String s = "RS";
    if (col >= 0) {
      s+="("+row+","+Id.tran[col]+"):";
    } else {
      s+=":"; 
    }
    for (int i = 0; i < nodes.length; i++) {
      s+=nodes[i]+","; 
    }
    return s;
  }
  
  /**
   * Puts a node into the set. The insertion succeeds either if the set is below
   * is maximal size or if the handle is closer than the most distant member in
   * the set.
   * 
   * @param handle the handle to put.
   * 
   * @return true if the put succeeded, false otherwise.
   */
  public boolean put(NodeHandle handle) {
    if (!handle.isAlive()) return false;
    int worstIndex = -1;
    int worstProximity = Integer.MIN_VALUE;

    // scan entries
    for (int i = 0; i < theSize; i++) {

      // if handle is already in the set, abort
      if (nodes[i].equals(handle))
        return false;

      if (!nodes[i].isAlive()) {
        // node is dead, replace immeadiately
        notifyTable(nodes[i], false);

        // in case we observe this handle, stop doing so
        nodes[i].deleteObserver(this);

        // insert new handle
        nodes[i] = handle;

        notifyTable(handle, true);
        handle.addObserver(this);
        return true;
      }
      
      // find entry with worst proximity
      int p = localNode.proximity(nodes[i]);
      if (p >= worstProximity) {
        worstProximity = p;
        worstIndex = i;
      }
    }

    if (theSize < nodes.length) {
      nodes[theSize++] = handle;

//      setChanged();
      notifyTable(handle, true);

      // ping handles while the set is not full
      handle.ping();
      handle.addObserver(this);

      return true;
    } else {
      if (localNode.proximity(handle) == Integer.MAX_VALUE) {
        // wait until the proximity value is available

        handle.ping(); // XXX - eventually, should only ping handles pinged from
                       // the deserializer
        handle.addObserver(this);

        return false;
      } else if (localNode.proximity(handle) < worstProximity) {
        // remove handle with worst proximity
//        setChanged();
        notifyTable(nodes[worstIndex], false);

        // in case we observe this handle, stop doing so
        nodes[worstIndex].deleteObserver(this);

        // insert new handle
        nodes[worstIndex] = handle;

//        setChanged();
        notifyTable(handle, true);
        handle.addObserver(this);

        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Is called by the Observer pattern whenever the liveness or proximity of a
   * registered node handle is changed.
   * 
   * @param o The node handle
   * @param arg the event type (PROXIMITY_CHANGE, DECLARED_LIVE, DECLARED_DEAD)
   */
  public void update(Observable o, Object arg) {
    // if the proximity is initialized for the time, insert the handle
    if (((Integer) arg) == NodeHandle.PROXIMITY_CHANGED) {
      put((NodeHandle) o);
    } else if (((Integer) arg) == NodeHandle.DECLARED_DEAD) {
      // changed to remove dead handles - AM
      remove((NodeHandle) o);
    }
  }

  /**
   * Removes a node from a set.
   * 
   * @param nid the node id to remove.
   * 
   * @return the removed handle or null.
   */
  public NodeHandle remove(Id nid) {
    for (int i = 0; i < theSize; i++) {
      if (nodes[i].getNodeId().equals(nid)) {
        NodeHandle handle = nodes[i];

        nodes[i] = nodes[--theSize];

        notifyTable(handle, false);

        // in case we observe this handle, stop doing so
        handle.deleteObserver(this);

        return handle;
      }
    }

    return null;
  }

  /**
   * Removes a node from a set.
   * 
   * @param nid the node id to remove.
   * 
   * @return the removed handle or null.
   */
  public NodeHandle remove(NodeHandle nh) {
    for (int i = 0; i < theSize; i++) {
      if (nodes[i].equals(nh)) {
        NodeHandle handle = nodes[i];

        nodes[i] = nodes[--theSize];

        notifyTable(handle, false);

        // in case we observe this handle, stop doing so
        handle.deleteObserver(this);

        return handle;
      }
    }

    return null;
  }

  transient RoutingTable observer;
  
  private void notifyTable(NodeHandle handle, boolean added) {
    if (observer != null)
      observer.nodeSetUpdate(this, handle, added);
  }
  
  public void setRoutingTable(RoutingTable rt) {
    observer = rt; 
  }
  
  /**
   * Membership test.
   * 
   * @param nid the node id to membership of.
   * 
   * @return true if it is a member, false otherwise.
   */
  public boolean member(NodeHandle nh) {
    for (int i = 0; i < theSize; i++)
      if (nodes[i].equals(nh))
        return true;

    return false;
  }

  /**
   * Membership test.
   * 
   * @param nid the node id to membership of.
   * 
   * @return true if it is a member, false otherwise.
   */
  public boolean member(Id nid) {
    for (int i = 0; i < theSize; i++)
      if (nodes[i].getNodeId().equals(nid))
        return true;

    return false;
  }

  /**
   * Return the current size of the set.
   * 
   * @return the size.
   */
  public int size() {
    return theSize;
  }
  
  public int capacity() {
    return nodes.length; 
  }

  /**
   * Pings all new nodes in the RouteSet. No longer- Called from
   * RouteMaintenance.
   */
  public void pingAllNew() {
    for (int i = 0; i < theSize; i++) {
      if (localNode.proximity(nodes[i]) == Integer.MAX_VALUE)
        nodes[i].ping();
    }
  }

  /**
   * Return the closest live node in the set.
   * 
   * @return the closest node, or null if no live node exists in the set.
   */
  public NodeHandle closestNode() {
    return closestNode(NodeHandle.LIVENESS_SUSPECTED);
  }

  /**
   * Return the closest live node in the set.
   * 
   * @return the closest node, or null if no live node exists in the set.
   */
  public NodeHandle closestNode(int minLiveness) {
    int bestProximity = Integer.MAX_VALUE;
    NodeHandle bestNode = null;

    for (int i = 0; i < theSize; i++) {
      if (nodes[i].getLiveness() > minLiveness)
        continue;

      int p = localNode.proximity(nodes[i]);
      if (p <= bestProximity) {
        bestProximity = p;
        bestNode = nodes[i];
        closest = i;
      }
    }

    // If a backup node handle bubbles up to the top, ping it.
    if (bestNode != null && bestProximity == Integer.MAX_VALUE)
      bestNode.ping();

    return bestNode;
  }

  /**
   * Returns the node in the ith position in the set.
   * 
   * @return the ith node.
   */
  public NodeHandle get(int i) {
    if (i < 0 || i >= theSize)
      throw new NoSuchElementException();

    return nodes[i];
  }

  /**
   * Returns the node handle with the matching node id or null if none exists.
   * 
   * @param nid the node id.
   * 
   * @return the node handle.
   */
  public NodeHandle get(Id nid) {
    for (int i = 0; i < theSize; i++)
      if (nodes[i].getNodeId().equals(nid))
        return nodes[i];

    return null;
  }

  /**
   * Get the index of the node id.
   * 
   * @return the node.
   */
  public int getIndex(Id nid) {
    for (int i = 0; i < theSize; i++)
      if (nodes[i].getNodeId().equals(nid))
        return i;

    return -1;
  }

  /**
   * Get the index of the node id.
   * 
   * @return the node.
   */
  public int getIndex(NodeHandle nh) {
    for (int i = 0; i < theSize; i++)
      if (nodes[i].equals(nh))
        return i;

    return -1;
  }

  /**
   * deserialize the routeSet pings the handle the was the closests on the
   * sending node
   */
  private void readObject(ObjectInputStream in) throws IOException,
      ClassNotFoundException {
    nodes = (NodeHandle[]) in.readObject();
    theSize = in.readInt();
    closest = in.readInt();
    if (closest != -1)
      nodes[closest].ping();
    closest = -1;
  }

  /**
   * serialize the RouteSet records the closest node
   */
  private void writeObject(ObjectOutputStream out) throws IOException,
      ClassNotFoundException {
    if (closest == -1)
      closestNode();

    // here, we don't want to advetise nodes which are dead, so we filter
    // our list based on only live nodes
    NodeHandle[] tmp = new NodeHandle[nodes.length];

    int j = 0;
    for (int i = 0; i < tmp.length; i++) {
      if ((nodes[i] != null) && (nodes[i].isAlive())) {
        tmp[j] = nodes[i];
        j++;
      }
    }

    out.writeObject(tmp);
    out.writeInt(j);

    int closest = -1;
    for (int i = 0; i < j; i++)
      if ((closest == -1) || (localNode.proximity(tmp[i]) < localNode.proximity(tmp[closest])))
        closest = i;

    out.writeInt(closest);
  }

  // Common API Support

  /**
   * Puts a NodeHandle into the set.
   * 
   * @param handle the handle to put.
   * 
   * @return true if the put succeeded, false otherwise.
   */
  public boolean putHandle(rice.p2p.commonapi.NodeHandle handle) {
    return put((NodeHandle) handle);
  }

  /**
   * Finds the NodeHandle associated with the Id.
   * 
   * @param id a node id.
   * @return the handle associated with that id or null if no such handle is
   *         found.
   */
  public rice.p2p.commonapi.NodeHandle getHandle(rice.p2p.commonapi.Id id) {
    return getHandle((Id) id);
  }

  /**
   * Gets the ith element in the set.
   * 
   * @param i an index.
   * @return the handle associated with that id or null if no such handle is
   *         found.
   */
  public rice.p2p.commonapi.NodeHandle getHandle(int i) {
    return get(i);
  }

  /**
   * Verifies if the set contains this particular id.
   * 
   * @param id a node id.
   * @return true if that node id is in the set, false otherwise.
   */
  public boolean memberHandle(rice.p2p.commonapi.Id id) {
    return member((Id) id);
  }

  /**
   * Removes a node id and its handle from the set.
   * 
   * @param nid the node to remove.
   * 
   * @return the node handle removed or null if nothing.
   */
  public rice.p2p.commonapi.NodeHandle removeHandle(rice.p2p.commonapi.Id id) {
    return remove((Id) id);
  }

  /**
   * Gets the index of the element with the given node id.
   * 
   * @param id the id.
   * 
   * @return the index or throws a NoSuchElementException.
   */
  public int getIndexHandle(rice.p2p.commonapi.Id id)
      throws NoSuchElementException {
    return getIndex((Id) id);
  }
    
  public short getType() {
    return TYPE; 
  }
  
 /**
  *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  *   +    maxSize    +    theSize    +    closest    +
  *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  *   + NodeHandle 1st                                                +
  *                    ...                                             
  *   +                                                               +
  *   +                                                               +
  *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  *                    ...                                             
  *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  *   + NodeHandle theSize-th                                         +
  *                    ...                                             
  *   +                                                               +
  *   +                                                               +
  *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  */
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)nodes.length);    
    buf.writeByte((byte)theSize);    
    buf.writeByte((byte)closest);    
    for (int i = 0; i < theSize; i++) {
      nodes[i].serialize(buf);
    }       
  }
  
  public RouteSet(InputBuffer buf, NodeHandleFactory nhf, PastryNode local) throws IOException {
    this.localNode = local;
    byte maxSize = buf.readByte();
    theSize = buf.readByte();
    closest = buf.readByte();
    nodes = new NodeHandle[maxSize];
    for (int i = 0; i < theSize; i++) {
      nodes[i] = nhf.readNodeHandle(buf);
    }    
    row = -1;
    col = -1;
  }

  public void destroy() {
    for (int i = 0; i < nodes.length; i++) {
      if (nodes[i] != null) {
        nodes[i].deleteObserver(this);
        nodes[i] = null;
      }
    }
    
  }

  public Iterator<NodeHandle> iterator() {
    return Arrays.asList(nodes).iterator();
  }

  public boolean isEmpty() {
    return theSize <= 0;
  }
  
}