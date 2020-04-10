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

import java.util.*;
import java.io.*;

/**
 * Implementation of a cache of strings
 */
public class StringCache {
  
  /**
   * The default initial capacity - MUST be a power of two.
   */
  static final int DEFAULT_INITIAL_CAPACITY = 16;
  
  /**
   * The maximum capacity, used if a higher value is implicitly specified
   * by either of the constructors with arguments.
   * MUST be a power of two <= 1<<30.
   */
  static final int MAXIMUM_CAPACITY = 1 << 30;
  
  /**
   * The load factor used when none specified in constructor.
   **/
  static final float DEFAULT_LOAD_FACTOR = 0.75f;
  
  /**
   * The table, resized as necessary. Length MUST Always be a power of two.
   */
  transient Entry[] table;
  
  /**
   * The number of key-value mappings contained in this identity hash map.
   */
  transient int size;
  
  /**
   * The next size value at which to resize (capacity * load factor).
   * @serial
   */
  int threshold;
  
  /**
   * The load factor for the hash table.
   *
   * @serial
   */
  final float loadFactor;
  
  /**
   * The number of times this HashMap has been structurally modified
   * Structural modifications are those that change the number of mappings in
   * the HashMap or otherwise modify its internal structure (e.g.,
   * rehash).  This field is used to make iterators on Collection-views of
   * the HashMap fail-fast.  (See ConcurrentModificationException).
   */
  transient volatile int modCount;
  
  /**
   * Constructs an empty <tt>IntHashMap</tt> with the default initial capacity
   * (16) and the default load factor (0.75).
   */
  public StringCache() {
    this.loadFactor = DEFAULT_LOAD_FACTOR;
    threshold = (int)(DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
    table = new Entry[DEFAULT_INITIAL_CAPACITY];
  }
  
  /**
    * Returns a hash value for the specified object.  In addition to 
   * the object's own hashCode, this method applies a "supplemental
   * hash function," which defends against poor quality hash functions.
   * This is critical because HashMap uses power-of two length 
   * hash tables.<p>
   *
   * The shift distances in this function were chosen as the result
   * of an automated search over the entire four-dimensional search space.
   */
  static int hash(char[] chars, int off, int len) {
    int h = 0;
    
    for (int i=0; i<len; i++)
      h = 31*h + chars[off++];

    return h;
  }
  
  /** 
    * Check for equality of non-null reference x and possibly-null y. 
    */
  static boolean eq(char[] chars, int off, int len, String s) {
    if (len != s.length())
      return false;
    
    for (int i=0; i<len; i++)
      if (chars[i+off] != s.charAt(i))
        return false;
    
    return true;
  }
  
  /**
    * Returns index for hash code h. 
   */
  static int indexFor(int h, int length) {
    return h & (length-1);
  }
  
  /**
   * Returns the number of key-value mappings in this map.
   *
   * @return the number of key-value mappings in this map.
   */
  public int size() {
    return size;
  }
  
  /**
   * Returns <tt>true</tt> if this map contains no key-value mappings.
   *
   * @return <tt>true</tt> if this map contains no key-value mappings.
   */
  public boolean isEmpty() {
    return size == 0;
  }
  
  /**
   * Returns the value to which the specified key is mapped in this identity
   * hash map, or <tt>null</tt> if the map contains no mapping for this key.
   * A return value of <tt>null</tt> does not <i>necessarily</i> indicate
   * that the map contains no mapping for the key; it is also possible that
   * the map explicitly maps the key to <tt>null</tt>. The
   * <tt>containsKey</tt> method may be used to distinguish these two cases.
   *
   * @param   key the key whose associated value is to be returned.
   * @return  the value to which this map maps the specified key, or
   *          <tt>null</tt> if the map contains no mapping for this key.
   * @see #put(Object, Object)
   */
  public String get(char[] chars) {
    return get(chars, 0, chars.length);
  }
  
  /**
   * Returns the value to which the specified key is mapped in this identity
   * hash map, or <tt>null</tt> if the map contains no mapping for this key.
   * A return value of <tt>null</tt> does not <i>necessarily</i> indicate
   * that the map contains no mapping for the key; it is also possible that
   * the map explicitly maps the key to <tt>null</tt>. The
   * <tt>containsKey</tt> method may be used to distinguish these two cases.
   *
   * @param   key the key whose associated value is to be returned.
   * @return  the value to which this map maps the specified key, or
   *          <tt>null</tt> if the map contains no mapping for this key.
   * @see #put(Object, Object)
   */
  public String get(char[] chars, int offset, int length) {
    int hash = hash(chars, offset, length);
    int i = indexFor(hash, table.length);
    Entry e = table[i]; 
    
    while (true) {
      if (e == null) {
        String s = new String(chars, offset, length);    
        addEntry(hash, s, i);
        return s;
      }
      
      if (e.hash == hash && eq(chars, offset, length, e.value)) 
        return e.value;

      e = e.next;
    }
  }
    
  /**
   * Rehashes the contents of this map into a new array with a
   * larger capacity.  This method is called automatically when the
   * number of keys in this map reaches its threshold.
   *
   * If current capacity is MAXIMUM_CAPACITY, this method does not
   * resize the map, but but sets threshold to Integer.MAX_VALUE.
   * This has the effect of preventing future calls.
   *
   * @param newCapacity the new capacity, MUST be a power of two;
   *        must be greater than current capacity unless current
   *        capacity is MAXIMUM_CAPACITY (in which case value
   *        is irrelevant).
   */
  void resize(int newCapacity) {
    Entry[] oldTable = table;
    int oldCapacity = oldTable.length;
    if (oldCapacity == MAXIMUM_CAPACITY) {
      threshold = Integer.MAX_VALUE;
      return;
    }
    
    Entry[] newTable = new Entry[newCapacity];
    transfer(newTable);
    table = newTable;
    threshold = (int)(newCapacity * loadFactor);
  }
  
  /** 
   * Transfer all entries from current table to newTable.
   */
  void transfer(Entry[] newTable) {
    Entry[] src = table;
    int newCapacity = newTable.length;
    for (int j = 0; j < src.length; j++) {
      Entry e = src[j];
      if (e != null) {
        src[j] = null;
        do {
          Entry next = e.next;
          int i = indexFor(e.hash, newCapacity);  
          e.next = newTable[i];
          newTable[i] = e;
          e = next;
        } while (e != null);
      }
    }
  }
    
  /**
   * Removes all mappings from this map.
   */
  public void clear() {
    modCount++;
    Entry tab[] = table;
    for (int i = 0; i < tab.length; i++) 
      tab[i] = null;
    size = 0;
  }
    
  static class Entry {
    String value;
    final int hash;
    Entry next;
    
    /**
      * Create new entry.
     */
    Entry(int h, String v, Entry n) { 
      value = v; 
      next = n;
      hash = h;
    }
    
    public String getValue() {
      return value;
    }
    
    public boolean equals(Object o) {
      if (!(o instanceof Entry))
        return false;
   
      Entry e = (Entry) o;
      String v1 = getValue();
      String v2 = e.getValue();
      return (v1 == v2 || (v1 != null && v1.equals(v2)));
    }
    
    public int hashCode() {
      return 27 ^ value.hashCode();
    }
    
    public String toString() {
      return getValue();
    }
  }
  
  /**
   * Add a new entry with the specified key, value and hash code to
   * the specified bucket.  It is the responsibility of this 
   * method to resize the table if appropriate.
   *
   * Subclass overrides this to alter the behavior of put method.
   */
  void addEntry(int hash, String value, int bucketIndex) {
    table[bucketIndex] = new Entry(hash, value, table[bucketIndex]);
    if (size++ >= threshold) 
      resize(2 * table.length);
  }
    
  private static final long serialVersionUID = 362498820763181265L;
}
