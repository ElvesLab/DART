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
import java.math.*;
import java.util.*;

/**
 * @(#) ReverseTreeMap.java
 *
 * Class which is an implementation of a reverse tree map, maintaining tree
 * maps for both the keys and values
 *
 * @version $Id: ReverseTreeMap.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public class ReverseTreeMap implements Serializable {
  
  /**
   * The internal normal map
   */
  protected SortedMap normal = new RedBlackMap();
  
  /**
   * The internal value -> list of keys map
   */
  protected SortedMap reverse = new RedBlackMap();
  
  /**
   * The internal list of keys with null as a mapping
   */
  protected SortedMap nulls = new RedBlackMap();

  /**
   * Constructor
   *
   */
  public ReverseTreeMap() {
  }

  /**
   * Constructor
   *
   */
  protected ReverseTreeMap(SortedMap normal, SortedMap reverse, SortedMap nulls) {
    this.normal = normal;
    this.reverse = reverse;
    this.nulls = nulls;
  }
  
  /**
   * returns the size
   *
   * @return the size
   */
  public int size() {
    return normal.size();
  }
  
  /**
   * Returns whether or not we contain the key
   *
   * @param key THe key
   * @return Whether or not we have it
   */
  public boolean containsKey(Object key) {
    return normal.containsKey(key);
  }
  
  /**
   * Returns the value associated with the given key
   *
   * @param key The key
   * @return The value
   */
  public synchronized Object get(Object key) {
    return normal.get(key);
  }
  
  /**
   * Return the set of keys
   *
   * @return The set of keys
   */
  public Set keySet() {
    return normal.keySet();
  }
  
  /**
   * Removes any and all mappings with the given key
   *
   * @param key The key
   */
  public void remove(Object key) {
    Object value = normal.remove(key);
    
    if (value != null) {
      RedBlackMap o = (RedBlackMap) reverse.get(value);
      o.remove(key);
      
      if (o.size() == 0) {
        reverse.remove(value);
      }
    } else {
      nulls.remove(key);
    }
  }
  
  
  /**
   * Method which caputures the insert and adds it to the
   * reverse map
   *
   * @param key The key 
   * @param value The value
   */
  public void put(Object key, Object value) {
 //   if (reverse instanceof RedBlackMap)
 //     value = (((RedBlackMap) reverse).getKey(value) == null ? value : ((RedBlackMap) reverse).getKey(value));
    
    Object old = normal.put(key, value);
    
    // remove any old reverse mapping
    if (old != null) {
      RedBlackMap o = (RedBlackMap) reverse.get(old);        
      o.remove(key);
      
      if (o.size() == 0) {
        reverse.remove(old);
      }
    } else {
      nulls.remove(key);
    }
    
    // and add the new reverse mapping
    if (value != null) {
      RedBlackMap v = (RedBlackMap) reverse.get(value);
    
      // creating a treeset if necessary
      if (v == null) {
        v = new RedBlackMap();
        reverse.put(value, v);
      }
    
      v.put(key, value);
    } else {
      nulls.put(key, null);
    }
  }
  
  /**
   * Method which returns the key map
   *
   * @retun The key Map
   */
  public SortedMap keyMap() {
    return normal; 
  }
  
  /**
   * Method which returns a *cloned* head map, or all of the values
   * up to the specified value
   *
   * @param value The start value
   * @return a head map
   */
  public SortedMap keyHeadMap(Object value) {
    return normal.headMap(value);
  }
  
  /**
   * Method which returns a *cloned* tail map, or all of the values
   * after a the specified value
   *
   * @param value The start value
   * @return a head map
   */
  public SortedMap keyTailMap(Object value) {
    return normal.tailMap(value);
  }
  
  /**
   * Method which returns a *cloned* sub map, or all of the values
   * between the specified values
   *
   * @param start The start value
   * @param end The end value
   * @return a head map
   */
  public SortedMap keySubMap(Object start, Object end) {
    return normal.subMap(start, end);
  }

  /**
   * Method which returns a *cloned* sub map, or all of the values
   * not between the specified values
   *
   * @param start The start value
   * @param end The end value
   * @return a head map
   */
//  public SortedMap keySubNotMap(Object start, Object end) {
//    return new UnionSortedMap(normal.headMap(start), normal.tailMap(end));
//  }
    
  /**
   * Method which returns a headset of the values, or all the
   * keys with values up to the specified value
   *
   * @param value The maximal value
   */
  public SortedMap valueHeadMap(Object value) {
    return convert(reverse.headMap(value));
  }
  
  /**
   * Method which returns a tailset of the values, or all the
   * keys with values after to the specified value
   *
   * @param value The minimal value
   */
  public SortedMap valueTailMap(Object value) {
    return convert(reverse.tailMap(value));
  }

  /**
   * Method which returns a headset of the values, or all the
   * keys with values up to the specified value
   *
   * @param value The maximal value
   */
  public SortedMap valueSubMap(Object start, Object end) {
    return convert(reverse.subMap(start, end));
  }   
  
  /**
   * Method which returns all of the keys which have null values
   *
   */
  public SortedMap valueNullMap() {
    return nulls;
  }   
  
  /**
   * Internal method which converts a sortedmap to an iterator
   *
   * @param map THe map to convert
   * @return The converted map
   */
  protected SortedMap convert(SortedMap map) {
    SortedMap result = new RedBlackMap();
    Iterator i = map.keySet().iterator();
    
    while (i.hasNext()) 
      result.putAll((SortedMap) map.get(i.next()));
      
    return result;
  }
  
/*  protected class UnionSortedMap implements SortedMap {
    protected SortedMap map1;
    protected SortedMap map2;
    
    public UnionSortedMap(SortedMap map1, SortedMap map2) {
      this.map1 = map1;
      this.map2 = map2;
    }
    
    public Comparator comparator() { return null; }
    public Object firstKey() { return (map1.firstKey() == null ? map2.firstKey() : map1.firstKey()); }
    public SortedMap headMap(Object toKey) { throw new UnsupportedOperationException("headMap not supported!"); }
    public Object lastKey() { return (map2.lastKey() == null ? map1.lastKey() : map2.lastKey()); }
    public SortedMap subMap(Object fromKey, Object toKey) { throw new UnsupportedOperationException("subMap not supported!"); }
    public SortedMap tailMap(Object fromKey) { throw new UnsupportedOperationException("tailMap not supported!");}
    public void clear() { throw new UnsupportedOperationException("clear not supported!"); }
    public boolean containsKey(Object key) { return map1.containsKey(key) || map2.containsKey(key); }
    public boolean containsValue(Object value) { return map1.containsValue(value) || map2.containsValue(value); }
    public Set entrySet() { throw new UnsupportedOperationException("entrySet not supported!");  }
    public boolean equals(Object o) { throw new UnsupportedOperationException("equals not supported!"); }
    public Object get(Object key) { return (map1.containsKey(key) ? map1.get(key) : map2.get(key)); }
    public int hashCode() { throw new UnsupportedOperationException("hashCode not supported!");  }
    public boolean isEmpty() { return map1.isEmpty() && map2.isEmpty(); }
    public Set keySet() { return new UnionKeySet(map1.keySet(), map2.keySet()); }
    public Object put(Object key, Object value) { throw new UnsupportedOperationException("put not supported!"); }
    public void putAll(Map t) { throw new UnsupportedOperationException("putAll not supported!"); }
    public Object remove(Object key) { throw new UnsupportedOperationException("remove not supported!"); }
    public int size() { return map1.size() + map2.size(); }
    public Collection values() { throw new UnsupportedOperationException("values not supported!"); }
  }  
  
  protected class UnionKeySet implements Set {
    protected Set set1;
    protected Set set2;
    
    public UnionKeySet(Set set1, Set set2) {
      this.set1 = set1;
      this.set2 = set2;
    }
    
    public boolean add(Object o) { throw new UnsupportedOperationException("add not supported!"); }
    public boolean addAll(Collection c) { throw new UnsupportedOperationException("addAll not supported!"); }
    public void clear() { throw new UnsupportedOperationException("clear not supported!"); }
    public boolean contains(Object o) { return (set1.contains(o) || set2.contains(o)); }
    public boolean containsAll(Collection c) { throw new UnsupportedOperationException("containsAll not supported!"); }
    public boolean equals(Object o) { throw new UnsupportedOperationException("equals not supported!"); }
    public int hashCode() { throw new UnsupportedOperationException("hashCode not supported!"); }
    public boolean isEmpty() { return set1.isEmpty() && set2.isEmpty(); }
    public Iterator iterator() { return new Iterator() {
      protected Iterator i = set1.iterator();
      protected Iterator j = set2.iterator();
      public boolean hasNext() { return (i.hasNext() || j.hasNext()); }
      public Object next() { return (i.hasNext() ? i.next() : j.next()); }
      public void remove() { throw new UnsupportedOperationException("remove not supported!"); }
    };
    }
    public boolean remove(Object o) { throw new UnsupportedOperationException("remove not supported!"); }
    public boolean removeAll(Collection c) { throw new UnsupportedOperationException("removeAll not supported!"); }
    public boolean retainAll(Collection c) { throw new UnsupportedOperationException("retainAll not supported!"); }
    public int size() { return set1.size() + set2.size(); }
    public Object[] toArray() { throw new UnsupportedOperationException("toArray not supported!"); }
    public Object[] toArray(Object[] a) { throw new UnsupportedOperationException("toArray not supported!"); }
  } */
}
