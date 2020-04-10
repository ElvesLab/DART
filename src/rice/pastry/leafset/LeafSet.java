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

package rice.pastry.leafset;

import rice.p2p.commonapi.rawserialization.*;
import rice.pastry.*;
import rice.pastry.routing.*;

import java.util.*;
import java.io.*;

/**
 * A class for representing and manipulating the leaf set.
 *
 * The leafset is not strictly a set: when the ring is small, a node may appear in both the cw and the ccw half of the "set".
 *
 * @version $Id: LeafSet.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Andrew Ladd
 * @author Peter Druschel
 */
public class LeafSet extends Observable implements Serializable, Iterable<NodeHandle> {
  static private final long serialVersionUID = 3960030608598552977L;

  private Id baseId;
  private NodeHandle baseHandle;
  private SimilarSet cwSet;
  private SimilarSet ccwSet;

  transient boolean observe = true;
  private int theSize;

  // can get backup entries from this when we remove
  transient RoutingTable routingTable;
  
  private LeafSet(LeafSet that) {
    this(that, true); 
  }
  
  private LeafSet(LeafSet that, boolean observe) {
    this.observe = observe;
    this.baseId = that.baseId;
    this.baseHandle = that.baseHandle;
    this.ccwSet = that.ccwSet.copy(this);
    this.cwSet = that.cwSet.copy(this);
    this.theSize = that.theSize;
  }

  /**
   * Constructor.
   *
   * @param localHandle the local node
   * @param size the size of the leaf set.
   * @param rt (to fall back on for more entries on delete operations)
   */
  public LeafSet(NodeHandle localNode, int size, RoutingTable rt) {
    this(localNode, size, true);
    this.routingTable = rt;
  }
  
  public LeafSet(NodeHandle localNode, int size, boolean observe) {
    this.observe = observe;
    
    baseHandle = localNode;
    baseId = localNode.getNodeId();
    theSize = size;

    cwSet = new SimilarSet(this, localNode, size/2, true);
    ccwSet = new SimilarSet(this, localNode, size/2, false);
  }

  /**
   * Internal method for reading in this data object
   *
   * @param ois The current input stream
   */
  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
    ois.defaultReadObject();
    
    observe = true;
  }
  
  /**
   * Puts a NodeHandle into the set.
   *
   * @param handle the handle to put.
   * @return true if successful, false otherwise.
   */
  public boolean put(NodeHandle handle) {
    return put(handle, false);
  }
  
  public boolean put(NodeHandle handle, boolean suppressNotification)
  {
    Id nid = handle.getNodeId();
    if (nid.equals(baseId)) return false;
    if (member(handle)) return false;

    boolean res = cwSet.put(handle, suppressNotification) | ccwSet.put(handle, suppressNotification);
    return res;
  }

  /**
   * Test if a put of the given NodeHandle would succeed.
   *
   * @param handle the handle to test.
   * @return true if a put would succeed, false otherwise.
   */
  public boolean test(NodeHandle handle)
  {
    Id nid = handle.getNodeId();
    if (nid.equals(baseId)) return false;
    if (member(handle)) return false;

    return cwSet.test(handle) | ccwSet.test(handle);
  }

  /**
   * Test if the leafset overlaps
   *
   * @return true if the most distant cw member appears in the ccw set or vice versa, false otherwise
   */
  public boolean overlaps() {
    if (size() > 0 && ( ccwSet.member(cwSet.get(cwSet.size()-1)) ||
                        cwSet.member(ccwSet.get(ccwSet.size()-1)) ) )
      return true;

    else return false;
  }

  public boolean isComplete() {
    if (size() == maxSize()) return true;
    if (overlaps()) return true;
    return false;
  }

  /**
   * There are 2 possible indexes (if the ring is small), the cw index and the ccw, this returns the nearest index, and if they are the same, the cw index.
   * 
   * Note: previous to FP2.1a3, this always returned the cw index if it existed.
   * 
   * @param nh
   * @return
   * @throws NoSuchElementException
   */
  public int getIndex(NodeHandle nh) throws NoSuchElementException {

    if (baseId.equals(nh.getId())) return 0;

    int cwIndex = cwSet.getIndex(nh);
    int ccwIndex = ccwSet.getIndex(nh);
    if (cwIndex >= 0 && ccwIndex >= 0) {
      //the ring overlaps
      
      
      if (cwIndex <= ccwIndex) { 
        // cw is closer (or equal)
        return cwIndex + 1;
      }
      
      // ccw is closer
      return -ccwIndex - 1;
    }
    
    if (cwIndex >= 0) return cwIndex + 1;
    if (ccwIndex >= 0) return -ccwIndex - 1;

    throw new NoSuchElementException();
  }


  /**
   * Finds the NodeHandle at a given index.
   *
   * @param index an index.
   * @return the handle associated with that index.
   */
  public NodeHandle get(int index)
  {
    if (index == 0) return baseHandle;
    if (index >= 0) return cwSet.get(index - 1);
    else return ccwSet.get(- index - 1);
  }


  /**
   * Verifies if the set contains this particular handle.
   *
   * @param nid a NodeHandle.
   * @return true if that NodeHandle is in the set, false otherwise.
   */
  public boolean member(NodeHandle nid) {
    return cwSet.member(nid) || ccwSet.member(nid);
  }
  
  public boolean contains(NodeHandle nh) {
    return member(nh);
  }

  /**
   * Verifies if the set contains this particular id.
   *
   * @param nid a node id.
   * @return true if that node id is in the set, false otherwise.
   */
  public boolean member(Id nid)
  {
    return cwSet.member(nid) || ccwSet.member(nid);
  }


  /**
   * Removes a node id and its handle from the set.
   *
   * @param nid the node to remove.
   * @return the node handle removed or null if nothing.
   */
  public void remove(NodeHandle nh)
  {
    cwSet.remove(nh);
    ccwSet.remove(nh);
    
    // don't do this inside SimalerSet.remove() otherwise you may get an 
    // add notification before a removal
    cwSet.findMoreEntriesFromRoutingTable();
    ccwSet.findMoreEntriesFromRoutingTable();
  }

  /**
   * Gets the maximal size of the leaf set.
   *
   * @return the size.
   */
  public int maxSize() { return theSize; }

  /**
   * Gets the current size of the leaf set.  Note that if the leafset overlaps,
   * there will be duplicates.  If you want the unique nodes, use getUniqueCount().
   * 
   * @return the size.
   */
  public int size() { 
    return cwSize() + ccwSize(); // old code
    
    // 8.16.5 decided not to do this because it will break too much code
//    HashSet superset = new HashSet();
//    superset.addAll(cwSet.getCollection());
//    superset.addAll(ccwSet.getCollection());
//    return superset.size();     
  }

  /**
   * Gets the current clockwise size.
   *
   * @return the size.
   */
  public int cwSize() { return cwSet.size(); }

  /**
   * Gets the current counterclockwise size.
   *
   * @return the size.
   */
  public int ccwSize() { return ccwSet.size(); }

  /**
   * complement - given an index of a node in the leafset, produces
   * the index of the same Id in the opposite half of the
   * leafset
   *
   * @param index the index of the entry to complement
   * @param the index of the same node in the opposite half of the leafset, or inx if it does not exist there
   */
  private int complement(int inx) {
    int res;

    if (inx == 0) return 0;
    if (inx < 0) {
      if (inx < -ccwSize()) return inx;
      res = cwSet.getIndex(ccwSet.get(-inx - 1)) + 1;
    }
    else {
      if (inx > cwSize()) return inx;
      res = -ccwSet.getIndex(cwSet.get(inx - 1)) - 1;
    }

    if (res == 0) res = inx;
    return res;
  }

  /**
   * Numerically closests node to a given a node in the leaf set.
   *
   * @param nid a node id.
   * @return the index of the numerically closest node (0 if baseId is the closest).
   */
  public int mostSimilar(Id nid) {
    Id.Distance cwMinDist;
    Id.Distance ccwMinDist;
    int cwMS;
    int ccwMS;
    int res;

    if (baseId.clockwise(nid)) {
      cwMS = cwSet.mostSimilar(nid);
      //ccwMS = ccwSet.size()-1;
      if (cwMS < cwSet.size()-1)
        return cwMS + 1;
      ccwMS = ccwSet.mostSimilar(nid);
    }
    else {
      ccwMS = ccwSet.mostSimilar(nid);
      //cwMS = cwSet.size()-1;
      if (ccwMS < ccwSet.size()-1)
        return -ccwMS - 1;
      cwMS = cwSet.mostSimilar(nid);
    }

    cwMinDist = cwSet.get(cwMS).getNodeId().distance(nid);
    ccwMinDist = ccwSet.get(ccwMS).getNodeId().distance(nid);

    int cmp = cwMinDist.compareTo(ccwMinDist);
    if (cmp < 0 || (cmp == 0 && nid.clockwise(cwSet.get(cwMS).getNodeId())) )
      return cwMS + 1;
    else
      return -ccwMS - 1;

    /*
     if (cwMinDist.compareTo(ccwMinDist) <= 0)
      return cwMS + 1;
     else
      return -ccwMS - 1;
     */

  }

  /**
   * compute an ordered set of nodes that are neighbors of this local node,
   * in order of numerical closeness to the local node
   *
   * @param max the maximal size of the set requested
   * @return the ordered set of nodehandles
   */
  public NodeSet neighborSet(int max) {
    NodeSet set;
    int cwSize = cwSize();
    int ccwSize = ccwSize();
    NodeHandle cwExtreme = get(cwSize);
    NodeHandle ccwExtreme = get(-ccwSize);
    set = replicaSet(baseId,max);
    if(!set.member(cwExtreme) && !set.member(ccwExtreme)) {
      // the value of max did not cause us to reach either end of the leafset
      // in the method replicaSet
      return set;
    }
    else {
      if(!set.member(cwExtreme)) {
        // All the nodes in the ccwSet are already members
        for(int i=1; i <= cwSize; i++) {
          set.put(get(i));
        }
      }
      if(!set.member(ccwExtreme)) {
        // All the nodes in the cwSet are already members
        for(int i=1; i <= ccwSize; i++) {
          set.put(get(-i));
        }
      }
      return set;
    }
  }

  /**
   * compute an ordered set of nodes, in order of numerical closeness to a given key
   *
   * @param key the key
   * @param max the maximal size of the set requested
   * @return the ordered set of nodehandles
   */
  public NodeSet replicaSet(Id key, int max) {
    NodeSet set = new NodeSet();
    if (max < 1) return set;

    if ( !overlaps() && size() > 0 &&
         !key.isBetween(get(-ccwSet.size()).getNodeId(), get(cwSet.size()).getNodeId()) &&
         !key.equals(get(cwSet.size()).getNodeId()) )
      // can't determine root of key, return empty set
      return set;

    // compute the nearest node
    int nearest = mostSimilar(key);

    // add the key's root
    set.put(get(nearest));

    int cw = nearest;
    int ccw = nearest;
    int wrapped = 0;

    while (set.size() < max && wrapped < 3) {
      int tmp;

      // determine next nearest node
      NodeHandle cwNode = get(cw);
      NodeHandle ccwNode = get(ccw);
      Id.Distance cwDist = cwNode.getNodeId().distance(key);
      Id.Distance ccwDist = ccwNode.getNodeId().distance(key);

      if (cwDist.compareTo(ccwDist) <= 0) {
        // cwNode is closer to key
        set.put(cwNode);

        tmp = cw;
        if (cw == cwSet.size()) {
          if ((cw = complement(cw)) == tmp) return set;
          wrapped++;
        }
        cw++;

      } else {
        // ccwNode is closer to key
        set.put(ccwNode);

        tmp = ccw;
        if (-ccw == ccwSet.size()) {
          if ((ccw = complement(ccw)) == tmp) return set;
          wrapped++;
        }
        ccw--;

      }
    }


    return set;
  }

  /**
   * Returns the number of unique nodes in the leafset
   *
   * @return the number of unique nodes in the leafset
   */
  public int getUniqueCount() {
    return getUniqueSet().size()+1; // the local node     
//
//    Vector v = new Vector();
//
//    for (int i=-ccwSize(); i<=cwSize(); i++) {
//      if (! v.contains(get(i).getNodeId())) {
//        v.add(get(i).getNodeId());
//      }
//    }
//
//    return v.size();
  }
  
  /**
   * Unordered iterator, does not contain local node.  Contains each element only once.
   * 
   * TODO: Make this in order from nearest neighbor to farthest neighbor, not by replica, but take cw[0], cc2[0], cw[1], ccw[1] etc...
   * 
   * @return
   */
  public Iterator<NodeHandle> iterator() {
    return getUniqueSet().iterator();         
  }

  /**
   * Set of nodes in the leafset, not the local node, each node only once.
   * @return
   */
  public Collection<NodeHandle> getUniqueSet() {
    HashSet<NodeHandle> superset = new HashSet<NodeHandle>();
    superset.addAll(cwSet.getCollection());
    superset.addAll(ccwSet.getCollection());
    return superset;         
  }


  
  /**
   * Perform (x mod y).  This is necessary because the java % operator
   * is actually remainder instead of mod.  Useless.
   *
   * @return x mod y
   */
  private int mod(int x, int y) {
    if ((x < 0) ^ (y < 0))
      return y + (x % y);
    else
      return x % y;
  }

  /**
   * range
   * computes the range of keys for which node n is a i-root, 0<=i<=r
   * a node is the r-root for a key of the node becomes the numerically closest node to the key when
   * i-roots for the key fail, O<=i<r, where a key's 0-root is the numerically closest node to the key.
   *
   * @param n the nodehandle
   * @param r
   * @return the range of keys, or null if n is not a member of the leafset, or if the range cannot be computed
   */
  public IdRange range(NodeHandle n, int r) { 
   // first, we check the arguments
   if (r < 0) throw new IllegalArgumentException("Range must be greater than or equal to zero. Attempted "+r);
   if (! (member(n) || baseHandle.equals(n))) throw new LSRangeCannotBeDeterminedException("Node "+n+" is not in this leafset.",r,Integer.MIN_VALUE, getUniqueCount(), n, this);

   // get the position of the node and the number of nodes in the network
   int pos = getIndex(n);
   int num = getUniqueCount();
   NodeHandle cw, ccw; // these are the nodes in the Leafset that represent the extents of the range

   // if this leafset overlaps (or we are the only node in the network), then we have
   // global knowledge about the network and can determine any range.  Otherwise, we
   // need to determine whether or not we can determine the range
   if (overlaps() || (num == 1)) {

     // if the rank is more than the number of nodes in the network, then we return
     // the whole range.
     if (r+1 >= num) {
       return new IdRange(n.getNodeId(), n.getNodeId());
     }

     // we determine the locations if it's pair nodes
     // note that what we are doing is the following:
     //   we want the nodes at indices {pos-r, pos+r}, but these indices
     //   might be beyond the range of the leafset.  since the range of the unique
     //   leafset is [-ccwSize .. num-ccwSize-1], we shift the range up to
     //   [0 .. num-1] by adding ccwSize.  then, we want to mod p+-r
     //   in order to bring it within this range.  this is done by modding
     //   num (as this will result in the range [0 .. num-1] as
     //   desired.  Last, we shift the range back down by substracting
     //   ccwSize.
     ccw = get(mod(pos - r - 1 + ccwSet.size(), num) - ccwSet.size());
     cw = get(mod(pos + r + 1 + ccwSet.size(), num) - ccwSet.size()); 
    } else {
      // now, we need to find the pair nodes for this node range
      ccw = get(pos - r - 1);
      cw = get(pos + r + 1);
    }

   // if either of it's pair nodes are null, then we cannot determine the range
   if ((ccw == null) || (cw == null)) {
     throw new LSRangeCannotBeDeterminedException("This leafset doesn't have enough information to provide the correct range.",r,pos, getUniqueCount(), n, this);
   }

   // otherwise, we then construct the ranges which comprise the main range, and finally
   // return the overlap
   IdRange cwRange = (new IdRange(n.getNodeId(), cw.getNodeId())).ccwHalf();
   IdRange ccwRange = (new IdRange(ccw.getNodeId(), n.getNodeId())).cwHalf();

   return ccwRange.merge(cwRange);
  }

  /**
   * range
   * computes the ranges of keys for which node n is a r-root
   * a node is the r-root for a key of the node becomes the numerically closest node to the key when
   * i-roots for the key fail, O<=i<r, where a key's 0-root is the numerically closest node to the key.
   * there can be two contiguous ranges of keys; the cw parameter selects which one is returned.
   *
   * @param n the nodehandle
   * @param r
   * @param cw if true returns the clockwise range, else the counterclockwise range
   * @return the range of keys, or null if n is not a member of the leafset, or if the range cannot be computed
   */
  public IdRange range(NodeHandle n, int r, boolean cw) {
    IdRange rr = range(n, r);
    if (r == 0) return rr;
    IdRange rprev = null;
    try {
      rprev = range(n, r-1);
    } catch (LSRangeCannotBeDeterminedException rcbde) {
      // keeps previous functionality now that we are throwing rcbde rather than returning null
    }
    if (rr == null || rprev == null) return rr;
    
    //IdRange res = rr.diff(rprev, cw);
    IdRange res;

    if (!cw) res = rr.diff(rprev);
    else     res = rprev.diff(rr);
    return res;
  }


  /**
   * Merge a remote leafset into this
   *
   * @param remotels the remote leafset
   * @param from the node from which we received the leafset
   * @param routeTable the routing table
   * @param security the security manager
   * @param testOnly if true, do not change the leafset
   * @param insertedHandles if not null, a Set that contains, upon return of this method, the nodeHandles that would be inserted into this LeafSet if testOnly is true
   * @return true if the local leafset changed
   */
  public boolean merge(LeafSet remotels, NodeHandle from, RoutingTable routeTable,
                       boolean testOnly, Set<NodeHandle> insertedHandles) {
//    System.out.println(this+".merge("+remotels+","+from+",...,"+testOnly+","+insertedHandles+")");
    
    boolean changed, result = false;
    int cwSize = remotels.cwSize();
    int ccwSize = remotels.ccwSize();
    
    // this is the only way to get the notification correct
    Set<NodeHandle> myInsertedHandles = new HashSet<NodeHandle>();

    // merge the received leaf set into our own
    // to minimize inserts/removes, we do this from nearest to farthest nodes

    // get indeces of localId in the leafset
    int cw = remotels.cwSet.getIndex(baseId);
    int ccw = remotels.ccwSet.getIndex(baseId);

    if (cw < 0) {
      // localId not in cw set

      if (ccw < 0) {
        // localId not in received leafset, that means local node is joining

        if (remotels.size() < 2) {
          cw = ccw = 0;
        }
        else {

          // find the num. closest to localId in the remotels
          int closest = remotels.mostSimilar(baseId);
          Id closestId = remotels.get(closest).getNodeId();

          if (closest == -remotels.ccwSize() || closest == remotels.cwSize()) {
            // doesn't hurt to merge it anyways
            //return;
          }

          if (closest == 0) {
            // from is closest
            if (baseId.clockwise(closestId)) {
              cw = closest;
              ccw = remotels.complement(closest - 1);
            }
            else {
              cw = remotels.complement(closest + 1);
              ccw = closest;
            }
          }
          else if (closest < 0) {
            // from is cw
            if (baseId.clockwise(closestId)) {
              cw = closest;
              ccw = remotels.complement(closest - 1);
            }
            else {
              cw = closest + 1;
              ccw = remotels.complement(closest);
            }
          }
          else {
            // from is ccw
            if (baseId.clockwise(closestId)) {
              cw = remotels.complement(closest);
              ccw = closest - 1;
            }
            else {
              ccw = closest;
              cw = remotels.complement(closest + 1);
            }
          }

        }
      }
      else {
        // localId in ccw set

        ccw = -ccw  - 2;
        cw = ccw + 2;
      }
    }
    else {
      // localId in cw set

      if (ccw < 0) {
        cw = cw + 2;
        ccw = cw - 2;
      }
      else {
        // localId is in both halves
        int tmp = ccw;
        ccw = cw;
        cw = -tmp;
      }
    }

    for (int i=cw; i<=cwSize; i++) {
      NodeHandle nh;

      if (i == 0) nh = from;
      else nh = remotels.get(i);

      if (nh.isAlive() == false) continue;
      //if (member(nh.getNodeId())) continue;

      if (testOnly) {
        // see it is missing
        changed = cwSet.test(nh);
      }
      else {
        // merge into our cw leaf set half
        changed = cwSet.put(nh, true);

        if (changed) myInsertedHandles.add(nh);
        
        // update RT as well
        routeTable.put(nh);
      }

      result |= changed;
      if (insertedHandles != null && changed) insertedHandles.add(nh);
    }

    for (int i=ccw; i>= -ccwSize; i--) {
      NodeHandle nh;

      if (i == 0) nh = from;
      else nh = remotels.get(i);

      if (nh.isAlive() == false) continue;
      //if (member(nh.getNodeId())) continue;

      if (testOnly) {
        // see if it is missing
        changed = ccwSet.test(nh);
      }
      else {
        // merge into our leaf set
        changed = ccwSet.put(nh, true);

        if (changed) myInsertedHandles.add(nh);
        
        // update RT as well
        routeTable.put(nh);
      }

      result |= changed;
      if (insertedHandles != null && changed) insertedHandles.add(nh);
    }

    // if there is overlap, insert nearest nodes regardless of orientation
    if (overlaps()) {
      for (int i=-ccwSize; i <= cwSize; i++) {
        NodeHandle nh;

        if (i == 0) nh = from;
        else nh = remotels.get(i);

        if (nh.isAlive() == false) continue;

        if (testOnly) {
          // merge into our leaf set
          changed = test(nh);
        }
        else {
          // merge into our leaf set
          changed = put(nh);
          
          if (changed) myInsertedHandles.add(nh);
          
          // update RT as well
          routeTable.put(nh);
        }

        result |= changed;
        if (insertedHandles != null && changed) insertedHandles.add(nh);
      }
    }

    Iterator<NodeHandle> i = myInsertedHandles.iterator();
    while(i.hasNext()) {
      cwSet.notifyListeners(i.next(), true); 
    }
    
    return result;
  }
  /**
   * Add observer method.
   *
   * @deprecated use addNodeSetListener
   * @param o the observer to add.
   */
  public void addObserver(Observer o) {
    cwSet.addObserver(o);
    ccwSet.addObserver(o);
  }

  /**
   * Delete observer method.
   *
   * @deprecated use deleteNodeSetListener
   * @param o the observer to delete.
   */
  public void deleteObserver(Observer o) {
    cwSet.deleteObserver(o);
    ccwSet.deleteObserver(o);
  }

  /**
   * Add observer method.
   *
   * @param o the observer to add.
   */
  public void addNodeSetListener(NodeSetListener listener) {
    cwSet.addNodeSetListener(listener);
    ccwSet.addNodeSetListener(listener);
  }

  /**
   * Delete observer method.
   *
   * @param o the observer to delete.
   */
  public void deleteNodeSetListener(NodeSetListener listener) {
    cwSet.removeNodeSetListener(listener);
    ccwSet.removeNodeSetListener(listener);
  }

  /**
   * Returns a string representation of the leaf set
   *
   */
  public String toString()
  {
    String s = "leafset: ";
    for (int i=-ccwSet.size(); i<0; i++)
      s = s + get(i).getNodeId();
    s = s + " [ " + baseId + " ] ";
    for (int i=1; i<=cwSet.size(); i++)
      s = s + get(i).getNodeId();

    s+=" complete:"+isComplete();
    s+=" size:"+size();
    if (size() > 0)    
    s+=" s1:"+ ccwSet.member(cwSet.get(cwSet.size()-1));
    s+=" s2:"+ cwSet.member(ccwSet.get(ccwSet.size()-1));

    return s;
  }

  protected boolean isProperlyRemoved(NodeHandle handle) {
    return !member(handle);
  }

  protected boolean testOtherSet(SimilarSet set, NodeHandle handle) {
    //if (true) return false;
    SimilarSet otherSet = ccwSet;
    if (otherSet == set) {
      otherSet = cwSet;
    }
    
    return otherSet.test(handle);
  }
  
  public boolean directTest(NodeHandle handle) {
    return cwSet.test(handle) || ccwSet.test(handle);
  }
  
  public LeafSet copy() {
    return new LeafSet(this);
  }

  /**
   * So that small LeafSets (who have overlapping nodes) don't waste bandwidth, 
   * leafset first defines the NodeHandles to be loaded into an array, then 
   * specifies their locations. We do this because
   * a NodeHandle takes up a lot more space than the index in the leafset, and 
   * it may be in the leafset 1 or 2 times.  
   * 
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + byte theSize  +numUniqueHandls+ byte cwSize   + byte ccwSize  +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + NodeHandle baseHandle                                         +
   *                    ...                                             
   *   +                                                               +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + NodeHandle 1st                                                +
   *                    ...                                             
   *   +                                                               +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *                    ...                                             
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + NodeHandle numUniqueHandls-th                                 +
   *                    ...                                             
   *   +                                                               +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + byte cw 1st   +  cw  2nd      + ...           + ccw 1st       +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + ccw 2nd       +  ...          + ...           + ccw Nth       +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   
   */
  public static LeafSet build(InputBuffer buf, NodeHandleFactory nhf) throws IOException {
    byte theSize = buf.readByte();
    byte numUniqueHandles = buf.readByte();
    byte cwSize = buf.readByte();
    byte ccwSize = buf.readByte();
    NodeHandle baseHandle = nhf.readNodeHandle(buf);
    NodeHandle[] nhTable = new NodeHandle[numUniqueHandles];
    for (int i = 0; i < numUniqueHandles; i++) {
      nhTable[i] = nhf.readNodeHandle(buf); 
    }

    NodeHandle[] cwTable = new NodeHandle[cwSize];
    NodeHandle[] ccwTable = new NodeHandle[ccwSize];
    
    for (int i = 0; i < cwSize; i++) {
      cwTable[i] = nhTable[buf.readByte()];
    }
    
    for (int i = 0; i < ccwSize; i++) {
      ccwTable[i] = nhTable[buf.readByte()];
    }
    
    return new LeafSet(baseHandle,theSize, true, cwTable, ccwTable);
  }
  
  public LeafSet(NodeHandle localNode, int size, boolean observe, NodeHandle[] cwTable, NodeHandle[] ccwTable) {
    this.observe = observe;
    
    baseHandle = localNode;
    baseId = localNode.getNodeId();
    theSize = size;

    cwSet = new SimilarSet(this, localNode, size/2, true, cwTable);
    ccwSet = new SimilarSet(this, localNode, size/2, false, ccwTable);
  }

  /**
   * So that small LeafSets (who have overlapping nodes) don't waste bandwidth, 
   * leafset first defines the NodeHandles to be loaded into an array, then 
   * specifies their locations. We do this because
   * a NodeHandle takes up a lot more space than the index in the leafset, and 
   * it may be in the leafset 1 or 2 times.  
   * 
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + byte theSize  +numUniqueHandls+ byte cwSize   + byte ccwSize  +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + NodeHandle baseHandle                                         +
   *                    ...                                             
   *   +                                                               +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + NodeHandle 1st                                                +
   *                    ...                                             
   *   +                                                               +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *                    ...                                             
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + NodeHandle numUniqueHandls-th                                 +
   *                    ...                                             
   *   +                                                               +
   *   +                                                               +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + byte cw 1st   +  cw  2nd      + ...           + ccw 1st       +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   + ccw 2nd       +  ...          + ...           + ccw Nth       +
   *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   *   
   *   TODO 2.23.2006 the synchronization of LeafSet is nonexistent
   *   and it's difficult to add because the listeneer interface should not 
   *   be called while holding a lock, but the lock should be acquired once while
   *   making the change
   *   
   */  
  public synchronized void serialize(OutputBuffer buf) throws IOException {
    HashSet<NodeHandle> superset = new HashSet<NodeHandle>();
    superset.addAll(cwSet.getCollection());
    superset.addAll(ccwSet.getCollection());    
    ArrayList<NodeHandle> list = new ArrayList<NodeHandle>(superset);
        
    buf.writeByte((byte)theSize);
    buf.writeByte((byte)list.size());
    buf.writeByte((byte)cwSize());
    buf.writeByte((byte)ccwSize());

    baseHandle.serialize(buf);
    
    Iterator<NodeHandle> it = list.iterator();
    while(it.hasNext()) {
      NodeHandle nh = (NodeHandle)it.next(); 
      nh.serialize(buf);
    }
    
    for (int i = 0; i < cwSet.size(); i++) {
      buf.writeByte((byte)list.indexOf(cwSet.get(i))); 
    }
    
    for (int i = 0; i < ccwSet.size(); i++) {
      buf.writeByte((byte)list.indexOf(ccwSet.get(i))); 
    }    
  }
  
  /**
   * 
   * If overlaps() a NodeHandle may show up twice.  Does not return self.
   * 
   * @return list of NodeHandle
   */
  public synchronized List<NodeHandle> asList() {
    List<NodeHandle> l = new ArrayList<NodeHandle>();
    for (int i=-ccwSize(); i<=cwSize(); i++) {
      if (i != 0) {
        l.add(get(i));
      }
    }      
    return l;
  }

  public void destroy() {
    cwSet.destroy();
    ccwSet.destroy();
  }
}

