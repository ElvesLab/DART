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

package rice.p2p.multiring;

import rice.environment.random.RandomSource;
import rice.p2p.commonapi.*;
import java.util.*;

/**
 * @(#) IdFactory.java 
 *
 * This class provides the ability to build Ids which can support a multi-ring
 * hierarchy.
 *
 * @version $Id: MultiringIdFactory.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Peter Druschel
 */
public class MultiringIdFactory implements IdFactory {
  
  /**
   * The multiring node supporting this endpoint
   */
  protected Id ringId;
  
  /**
   * The underlying IdFactory
   */
  protected IdFactory factory;
  
  /**
   * Constructor
   *
   * @param factory the underlying factory to use
   */
  public MultiringIdFactory(Id ringId, IdFactory factory) {
    this.ringId = ringId;
    this.factory = factory;
  }
  
  /**
   * Method which returns the underlying Id which represents the local
   * node's ring
   *
   * @return The Id represetning the local ring
   */
  public Id getRingId() {
    return ringId;
  }
  
  /**
   * Builds a ringId by using the provided Id and ringIds.
   *
   * @param ringId The id to use as the ringid
   * @param material The id material to use 
   * @return The built Id.
   */
  public RingId buildRingId(Id ringId, byte[] material) {
    return RingId.build(ringId, factory.buildId(material));
  }
  
  /**
   * Builds a ringId by using the provided Id and ringIds.
   *
   * @param ringId The id to use as the ringid
   * @param id The id to use as the id
   * @return The built Id.
   */
  public RingId buildRingId(Id ringId, Id id) {
    return RingId.build(ringId, id);
  }
  
  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildNormalId(byte[] material) {
    return factory.buildId(material);
  }
  
  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildNormalId(String material) {
    return factory.buildId(material);
  }
  
  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildId(byte[] material) {
    return RingId.build(getRingId(), factory.buildId(material));
  }
  
  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildId(int[] material) {
    return RingId.build(getRingId(), factory.buildId(material));
  }
  
  /**
   * Builds a protocol-specific Id by using the hash of the given string as source data.
   *
   * @param string The string to use as source data
   * @return The built Id.
   */
  public Id buildId(String string) {
    return RingId.build(getRingId(), factory.buildId(string));
  }
  
  /**
   * Builds a random protocol-specific Id.
   *
   * @param rng A random number generator
   * @return The built Id.
   */
  public rice.p2p.commonapi.Id buildRandomId(Random rng) {
    return RingId.build(getRingId(), factory.buildRandomId(rng));
  }

  public rice.p2p.commonapi.Id buildRandomId(RandomSource rng) {
    return RingId.build(getRingId(), factory.buildRandomId(rng));
  }

  /**
   * Builds an Id by converting the given toString() output back to an Id.  Should
   * not normall be used.
   *
   * @param string The toString() representation of an Id
   * @return The built Id.
   */
  public Id buildIdFromToString(String string) {
    string = string.substring(1);
    Id ring = factory.buildIdFromToString(string.substring(0, string.indexOf(",")));

    string = string.substring(string.indexOf(", ")+2);
    Id normal = factory.buildIdFromToString(string.substring(0, string.length()-1));
    
    return RingId.build(ring, normal);
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
    Id ring = factory.buildIdFromToString(chars, 1, find(chars, ',')-1);
    Id normal = factory.buildIdFromToString(chars, 2+find(chars, ','), find(chars, ')') - (2+find(chars, ',')));
    
    return RingId.build(ring, normal);    
  }
  
  protected static int find(char[] chars, char value) {
    for (int i=0; i<chars.length; i++)
      if (chars[i] == value)
        return i;
    
    return chars.length;
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
    if (string.indexOf(", ") < 0) 
      return new MultiringIdRange(ringId, factory.buildIdRangeFromPrefix(string));
    
    string = string.substring(1);
    Id ring = factory.buildIdFromToString(string.substring(0, string.indexOf(", ")));
    
    string = string.substring(string.indexOf(", ")+2);
    IdRange range = factory.buildIdRangeFromPrefix(string);
    
    return new MultiringIdRange(ring, range);
  }
  
  /**
   * Returns the length a Id.toString should be.
   *
   * @return The correct length;
   */
  public int getIdToStringLength() {
    return 4 + (2 * factory.getIdToStringLength());
  }
  
  /**
   * Builds a protocol-specific Id.Distance given the source data.
   *
   * @param material The material to use
   * @return The built Id.Distance.
   */
  public Id.Distance buildIdDistance(byte[] material) {
    return factory.buildIdDistance(material);
  }
  
  /**
   * Creates an IdRange given the CW and CCW ids.
   *
   * @param cw The clockwise Id
   * @param ccw The counterclockwise Id
   * @return An IdRange with the appropriate delimiters.
   */
  public IdRange buildIdRange(Id cw, Id ccw) {
    return new MultiringIdRange(getRingId(), factory.buildIdRange(((RingId) cw).getId(), ((RingId) ccw).getId()));
  }
  
  /**
   * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  public IdSet buildIdSet() {
    return new MultiringIdSet(getRingId(), factory.buildIdSet());
  }
  
  /**
   * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  public IdSet buildIdSet(SortedMap map) {
    return new MultiringIdSet(getRingId(), factory.buildIdSet(new MultiringSortedMap(map)));
  }
  
  /**
   * Creates an empty NodeHandleSet.
   *
   * @return an empty NodeHandleSet
   */
  public NodeHandleSet buildNodeHandleSet() {
    return new MultiringNodeHandleSet(getRingId(), factory.buildNodeHandleSet());
  }
  
  @SuppressWarnings("unchecked")
  protected class MultiringSortedMap implements SortedMap {
    protected SortedMap map;
    
    public MultiringSortedMap(SortedMap map) {
      this.map = map;
    }
    
    public Comparator comparator() { return null; }
    public Object firstKey() { return (map.firstKey() == null ? null : ((RingId) map.firstKey()).getId()); }
    public SortedMap headMap(Object toKey) { return map.headMap(RingId.build(ringId, (Id) toKey)); }
    public Object lastKey() { return (map.lastKey() == null ? null : ((RingId) map.lastKey()).getId()); }
    public SortedMap subMap(Object fromKey, Object toKey) { return map.subMap(RingId.build(ringId, (Id) fromKey), RingId.build(ringId, (Id) toKey)); }
    public SortedMap tailMap(Object fromKey) { return map.tailMap(RingId.build(ringId, (Id) fromKey)); }
    public void clear() { throw new UnsupportedOperationException("clear not supported!"); }
    public boolean containsKey(Object key) { return map.containsKey(RingId.build(ringId, (Id) key)); }
    public boolean containsValue(Object value) { throw new UnsupportedOperationException("containsValue not supported!"); }
    public Set entrySet() { return new MultiringEntrySet(map.entrySet()); }
    public boolean equals(Object o) { throw new UnsupportedOperationException("equals not supported!"); }
    public Object get(Object key) { throw new UnsupportedOperationException("get not supported!"); }
    public int hashCode() { throw new UnsupportedOperationException("hashCode not supported!");  }
    public boolean isEmpty() { throw new UnsupportedOperationException("isEmpty not supported!");  }
    public Set keySet() { return new MultiringKeySet(map.keySet()); }
    public Object put(Object key, Object value) { return map.put(RingId.build(ringId, (Id) key), value); }
    public void putAll(Map t) { throw new UnsupportedOperationException("putAll not supported!"); }
    public Object remove(Object key) { return map.remove(RingId.build(ringId, (Id) key)); }
    public int size() { return map.size(); }
    public Collection values() { throw new UnsupportedOperationException("values not supported!"); }
  }

  @SuppressWarnings("unchecked")
  protected class MultiringEntrySet implements Set {
    protected Set set;
    
    public MultiringEntrySet(Set set) {
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
      public Object next() { return new MultiringMapEntry((Map.Entry) i.next()); }
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
  protected class MultiringKeySet implements Set {
    protected Set set;
    
    public MultiringKeySet(Set set) {
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
      public Object next() { return ((RingId) i.next()).getId(); }
      public void remove() { i.remove(); }
    };
    }
    public boolean remove(Object o) { throw new UnsupportedOperationException("remove not supported!"); }
    public boolean removeAll(Collection c) { throw new UnsupportedOperationException("removeAll not supported!"); }
    public boolean retainAll(Collection c) { throw new UnsupportedOperationException("retainAll not supported!"); }
    public int size() { throw new UnsupportedOperationException("size not supported!"); }
    public Object[] toArray() { 
      Object[] result = new Object[set.size()];
      Iterator i = set.iterator();
      int j = 0;
      
      while (i.hasNext()) 
        result[j++] = ((RingId) i.next()).getId();
      
      return result;
    }
    public Object[] toArray(Object[] a) { 
      Object[] result = (Object[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), set.size());
      Iterator i = set.iterator();
      int j = 0;
      
      while (i.hasNext()) 
        result[j++] = ((RingId) i.next()).getId();
      
      return result;
    }
  }
    
  protected class MultiringMapEntry implements Map.Entry {
    protected Map.Entry entry;
    
    public MultiringMapEntry(Map.Entry entry) {
      this.entry = entry;
    }
    
    public boolean equals(Object o) { throw new UnsupportedOperationException("equals not supported!"); }
    public Object getKey() { return ((RingId) entry.getKey()).getId(); }
    public Object getValue() { return entry.getValue(); }
    public int hashCode() { throw new UnsupportedOperationException("hashCode not supported!"); }
    public Object setValue(Object value) { throw new UnsupportedOperationException("setValue not supported!"); }
  }
}

