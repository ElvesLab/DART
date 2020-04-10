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

package rice.p2p.past.gc;

import java.util.*;

import rice.environment.random.RandomSource;
import rice.p2p.commonapi.*;

/**
 * @(#) GCIdFactory.java 
 *
 * This class provides the ability to build Ids which can support a multi-ring
 * hierarchy.
 *
 * @version $Id: GCIdFactory.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Peter Druschel
 */
public class GCIdFactory implements IdFactory {
  
  /**
   * The underlying IdFactory
   */
  protected IdFactory factory;
  
  /**
   * Constructor
   *
   * @param factory the underlying factory to use
   */
  public GCIdFactory(IdFactory factory) {
    this.factory = factory;
  }
  
  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildNormalId(byte[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildId(byte[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildId(int[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Builds a protocol-specific Id by using the hash of the given string as source data.
   *
   * @param string The string to use as source data
   * @return The built Id.
   */
  public Id buildId(String string) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Builds a random protocol-specific Id.
   *
   * @param rng A random number generator
   * @return The built Id.
   */
  public Id buildRandomId(Random rng) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  public Id buildRandomId(RandomSource rng) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }

  /**
   * Builds an Id by converting the given toString() output back to an Id.  Should
   * not normall be used.
   *
   * @param string The toString() representation of an Id
   * @return The built Id.
   */
  public Id buildIdFromToString(String string) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Builds an Id by converting the given toString() output back to an Id.  Should
   * not normally be used.
   *
   * @param chars The character array
   * @param offset The offset to start reading at
   * @param length The length to read
   * @return The built Id.
   */
  public Id buildIdFromToString(char[] chars, int offset, int length) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");  
  }
  
  /**
   * Builds an IdRange based on a prefix.  Any id which has this prefix should
   * be inside this IdRange, and any id which does not share this prefix should
   * be outside it.
   *
   * @param string The toString() representation of an Id
   * @return The built Id.
   */
  public IdRange buildIdRangeFromPrefix(String string) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");  
  }
  
  /**
   * Returns the length a Id.toString should be.
   *
   * @return The correct length;
   */
  public int getIdToStringLength() {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build Ids!");
  }
  
  /**
   * Builds a protocol-specific Id.Distance given the source data.
   *
   * @param material The material to use
   * @return The built Id.Distance.
   */
  public Id.Distance buildIdDistance(byte[] material) {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build IdDistances!");  }
  
  /**
   * Creates an IdRange given the CW and CCW ids.
   *
   * @param cw The clockwise Id
   * @param ccw The counterclockwise Id
   * @return An IdRange with the appropriate delimiters.
   */
  public IdRange buildIdRange(Id cw, Id ccw) {
    return new GCIdRange(factory.buildIdRange(cw, ccw));
  }
  
  /**
   * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  public IdSet buildIdSet() {
    return new GCIdSet(factory);
  }
  
  /**
   * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  public IdSet buildIdSet(SortedMap map) {
    //return new GCIdSet(factory.buildIdSet(new GCSortedMap(map)), factory);
    throw new UnsupportedOperationException("GCIdFactory.buildIDSet()");
  }
  
  /**
   * Creates an empty NodeHandleSet.
   *
   * @return an empty NodeHandleSet
   */
  public NodeHandleSet buildNodeHandleSet() {
    throw new UnsupportedOperationException("GCIdFactory cannot be used to build NodeHandleSets!");
  }
  
  @SuppressWarnings("unchecked")
  protected class GCSortedMap implements SortedMap {
    protected SortedMap map;
    
    public GCSortedMap(SortedMap map) {
      this.map = map;
    }
    
    public Comparator comparator() { return null; }
    public Object firstKey() { throw new UnsupportedOperationException("firstKey not supported!"); }
    public SortedMap headMap(Object toKey) { throw new UnsupportedOperationException("headMap not supported!"); }
    public Object lastKey() { throw new UnsupportedOperationException("lastKey not supported!"); }
    public SortedMap subMap(Object fromKey, Object toKey) { throw new UnsupportedOperationException("subMap not supported!"); }
    public SortedMap tailMap(Object fromKey) { throw new UnsupportedOperationException("tailMap not supported!");}
    public void clear() { throw new UnsupportedOperationException("clear not supported!"); }
    public boolean containsKey(Object key) { throw new UnsupportedOperationException("containsKey not supported!"); }
    public boolean containsValue(Object value) { throw new UnsupportedOperationException("containsValue not supported!"); }
    public Set entrySet() { return new GCEntrySet(map.entrySet()); }
    public boolean equals(Object o) { throw new UnsupportedOperationException("equals not supported!"); }
    public Object get(Object key) { throw new UnsupportedOperationException("get not supported!"); }
    public int hashCode() { throw new UnsupportedOperationException("hashCode not supported!");  }
    public boolean isEmpty() { throw new UnsupportedOperationException("isEmpty not supported!");  }
    public Set keySet() { throw new UnsupportedOperationException("keyset not supported!"); }
    public Object put(Object key, Object value) { throw new UnsupportedOperationException("put not supported!"); }
    public void putAll(Map t) { throw new UnsupportedOperationException("putAll not supported!"); }
    public Object remove(Object key) { throw new UnsupportedOperationException("remove not supported!"); }
    public int size() { return map.size(); }
    public Collection values() { throw new UnsupportedOperationException("values not supported!"); }
  }
  
  @SuppressWarnings("unchecked")
  protected class GCEntrySet implements Set {
    protected Set set;
    
    public GCEntrySet(Set set) {
      this.set = set;
    }
    
    public boolean add(Object o) { throw new UnsupportedOperationException("add not supported!"); }
    public boolean addAll(Collection c) { throw new UnsupportedOperationException("addAll not supported!"); }
    public void clear() { throw new UnsupportedOperationException("clear not supported!"); }
    public boolean contains(Object o) { throw new UnsupportedOperationException("contains not supported!"); }
    public boolean containsAll(Collection c) { throw new UnsupportedOperationException("containsAll not supported!"); }
    public boolean equals(Object o) { throw new UnsupportedOperationException("equals not supported!"); }
    public int hashCode() { throw new UnsupportedOperationException("hashCode not supported!"); }
    public boolean isEmpty() { throw new UnsupportedOperationException("isEmpty not supported!"); }
    public Iterator iterator() { return new Iterator() {
      protected Iterator i = set.iterator();
      public boolean hasNext() { return i.hasNext(); }
      public Object next() { return new GCMapEntry((Map.Entry) i.next()); }
      public void remove() { i.remove(); }
    };
    }
    public boolean remove(Object o) { throw new UnsupportedOperationException("remove not supported!"); }
    public boolean removeAll(Collection c) { throw new UnsupportedOperationException("removeAll not supported!"); }
    public boolean retainAll(Collection c) { throw new UnsupportedOperationException("retainAll not supported!"); }
    public int size() { throw new UnsupportedOperationException("size not supported!"); }
    public Object[] toArray() { throw new UnsupportedOperationException("toArray not supported!"); }
    public Object[] toArray(Object[] a) { throw new UnsupportedOperationException("toArray not supported!"); }
  }
  
  @SuppressWarnings("unchecked")
  protected class GCMapEntry implements Map.Entry {
    protected Map.Entry entry;
    
    public GCMapEntry(Map.Entry entry) {
      this.entry = entry;
    }
    
    public boolean equals(Object o) { throw new UnsupportedOperationException("equals not supported!"); }
    public Object getKey() { return ((GCId) entry.getKey()).getId(); }
    public Object getValue() { return entry.getValue(); }
    public int hashCode() { throw new UnsupportedOperationException("hashCode not supported!"); }
    public Object setValue(Object value) { throw new UnsupportedOperationException("setValue not supported!"); }
  }
}

