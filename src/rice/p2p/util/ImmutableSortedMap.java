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

package rice.p2p.util;

import java.io.*;
import java.util.*;

/**
 * @(#) ImmutableSortedMap.java
 *
 * Class which is an implementation of a sorted map which wraps and protects an
 * internal sorted map.  Any attempts to modify the underlying map will result in
 * a UnsupportedOperationException.
 *
 * @version $Id: ImmutableSortedMap.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public class ImmutableSortedMap implements SortedMap, Serializable {
  
  // the internal map
  protected SortedMap map;
  
  /**
   * Constructor which takes an existing map
   *
   * @param map the existing map
   */
  public ImmutableSortedMap(SortedMap map) {
    this.map = map;
  }
  
  /**
    * Returns the comparator associated with this sorted map, or
   * <tt>null</tt> if it uses its keys' natural ordering.
   *
   * @return the comparator associated with this sorted map, or
   *          <tt>null</tt> if it uses its keys' natural ordering.
   */
  public Comparator comparator() {
    return map.comparator();
  }
  
  /**
   * Returns a view of the portion of this sorted map whose keys range from
   * <tt>fromKey</tt>, inclusive, to <tt>toKey</tt>, exclusive.  
   *
   * @param fromKey low endpoint (inclusive) of the subMap.
   * @param toKey high endpoint (exclusive) of the subMap.
   * @return a view of the specified range within this sorted map.
   */
  public SortedMap subMap(Object fromKey, Object toKey) {
    return new ImmutableSortedMap(map.subMap(fromKey, toKey));
  }
  
  /**
   * Returns a view of the portion of this sorted map whose keys are
   * strictly less than toKey.  
   *
   * @param toKey high endpoint (exclusive) of the subMap.
   * @return a view of the specified initial range of this sorted map.
   */
  public SortedMap headMap(Object toKey) {
    return new ImmutableSortedMap(map.headMap(toKey));
  }
  
  /**
   * Returns a view of the portion of this sorted map whose keys are greater
   * than or equal to <tt>fromKey</tt>.  
   *
   * @param fromKey low endpoint (inclusive) of the tailMap.
   * @return a view of the specified final range of this sorted map.
   */
  public SortedMap tailMap(Object fromKey) {
    return new ImmutableSortedMap(map.tailMap(fromKey));
  }
  
  /**
   * Returns the first (lowest) key currently in this sorted map.
   *
   * @return the first (lowest) key currently in this sorted map.
   * @throws    NoSuchElementException if this map is empty.
   */
  public Object firstKey() {
    return map.firstKey();
  }
  
  /**
   * Returns the last (highest) key currently in this sorted map.
   *
   * @return the last (highest) key currently in this sorted map.
   * @throws     NoSuchElementException if this map is empty.
   */
  public Object lastKey() {
    return map.lastKey();
  }
  
  /**
   * Returns the number of key-value mappings in this map.  If the
   * map contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
   * <tt>Integer.MAX_VALUE</tt>.
   *
   * @return the number of key-value mappings in this map.
   */
  public int size() {
    return map.size();
  }
  
  /**
   * Returns <tt>true</tt> if this map contains no key-value mappings.
   *
   * @return <tt>true</tt> if this map contains no key-value mappings.
   */
  public boolean isEmpty() {
    return map.isEmpty();
  }
  
  /**
   * Returns <tt>true</tt> if this map contains a mapping for the specified
   * key.  
   *
   * @param key key whose presence in this map is to be tested.
   * @return <tt>true</tt> if this map contains a mapping for the specified
   */
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }
  
  /**
   * Returns <tt>true</tt> if this map maps one or more keys to the
   * specified value.  
   *
   * @param value value whose presence in this map is to be tested.
   * @return <tt>true</tt> if this map maps one or more keys to the
   *         specified value.
  */
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }
  
  /**
   * Returns the value to which this map maps the specified key.  Returns
   * <tt>null</tt> if the map contains no mapping for this key.  
   *
   * @param key key whose associated value is to be returned.
   * @return the value to which this map maps the specified key, or
   *         <tt>null</tt> if the map contains no mapping for this key.
   */
  public Object get(Object key) {
    return map.get(key);
  }
  
  // Modification Operations
  
  /**
   * Associates the specified value with the specified key in this map
   * (optional operation).  If the map previously contained a mapping for
   * this key, the old value is replaced by the specified value. 
   *
   * @param key key with which the specified value is to be associated.
   * @param value value to be associated with the specified key.
   * @return previous value associated with specified key, or <tt>null</tt>
   *         if there was no mapping for key.  A <tt>null</tt> return can
   *         also indicate that the map previously associated <tt>null</tt>
   *         with the specified key, if the implementation supports
   *         <tt>null</tt> values.
   * 
   * @throws UnsupportedOperationException if the <tt>put</tt> operation is
   *            not supported by this map.
   */
  public Object put(Object key, Object value) {
    throw new UnsupportedOperationException("put not supported by immutablemap");
  }
  
  /**
   * Removes the mapping for this key from this map if it is present
   * (optional operation).   More formally, if this map contains a mapping
   * from key <tt>k</tt> to value <tt>v</tt> such that
   * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping
   * is removed.  
   *
   * @param key key whose mapping is to be removed from the map.
   * @return previous value associated with specified key, or <tt>null</tt>
   *         if there was no mapping for key.
   */
  public Object remove(Object key) {
    throw new UnsupportedOperationException("remove not supported by immutablemap");
  }
  
  /**
   * Copies all of the mappings from the specified map to this map
   * (optional operation).  
   *
   * @param t Mappings to be stored in this map.
   */
  public void putAll(Map t) {
    throw new UnsupportedOperationException("putall not supported by immutablemap");
  }
  
  /**
   * Removes all mappings from this map (optional operation).
   *
   * @throws UnsupportedOperationException clear is not supported by this
   *       map.
   */
  public void clear() {
    throw new UnsupportedOperationException("clear not supported by immutablemap");
  }
  
  /**
   * Returns a set view of the keys contained in this map.  The set is
   * backed by the map, so changes to the map are reflected in the set, and
   * vice-versa.  If the map is modified while an iteration over the set is
   * in progress, the results of the iteration are undefined.  The set
   * supports element removal, which removes the corresponding mapping from
   * the map, via the <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
   * <tt>removeAll</tt> <tt>retainAll</tt>, and <tt>clear</tt> operations.
   * It does not support the add or <tt>addAll</tt> operations.
   *
   * @return a set view of the keys contained in this map.
   */
  public Set keySet() {
    return map.keySet();
  }
  
  /**
   * Returns a collection view of the values contained in this map.  The
   * collection is backed by the map, so changes to the map are reflected in
   * the collection, and vice-versa.  If the map is modified while an
   * iteration over the collection is in progress, the results of the
   * iteration are undefined.  The collection supports element removal,
   * which removes the corresponding mapping from the map, via the
   * <tt>Iterator.remove</tt>, <tt>Collection.remove</tt>,
   * <tt>removeAll</tt>, <tt>retainAll</tt> and <tt>clear</tt> operations.
   * It does not support the add or <tt>addAll</tt> operations.
   *
   * @return a collection view of the values contained in this map.
   */
  public Collection values() {
    return map.values();
  }
  
  /**
   * Returns a set view of the mappings contained in this map.  Each element
   * in the returned set is a {@link Map.Entry}.  The set is backed by the
   * map, so changes to the map are reflected in the set, and vice-versa.
   * If the map is modified while an iteration over the set is in progress,
   * the results of the iteration are undefined.  The set supports element
   * removal, which removes the corresponding mapping from the map, via the
   * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>, <tt>removeAll</tt>,
   * <tt>retainAll</tt> and <tt>clear</tt> operations.  It does not support
   * the <tt>add</tt> or <tt>addAll</tt> operations.
   *
   * @return a set view of the mappings contained in this map.
   */
  public Set entrySet() {
    return map.entrySet();
  }
}
  
