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

import java.math.*;
import java.security.*;
import java.util.*;

import rice.p2p.commonapi.*;
import rice.p2p.util.*;

/**
 * @(#) GCIdSet.java
 *
 * Internal representation of a set of GCIds
 * 
 * @version $Id: GCIdSet.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public class GCIdSet implements IdSet {
  
  // internal representation of the ids
  protected IdSet ids;
  
  // interal list of the timeouts
  protected SortedMap timeouts;
  
  /**
   * Constructor
   */
  protected GCIdSet(IdFactory factory) {
    this.ids = factory.buildIdSet();
    this.timeouts = new RedBlackMap();
  }
  
  /**
   * Constructor
   */
  protected GCIdSet(IdSet set, SortedMap timeouts) {
    this.ids = set;
    this.timeouts = timeouts;
  }
  
  /**
   * return the number of elements
   */
  public int numElements() {
    return ids.numElements();
  }
  
  /**
   * add a member
   * @param id the id to add
   */
  public void addId(Id id) {
    GCId gcid = (GCId) id;
    
    ids.addId(gcid.getId());
    timeouts.put(gcid.getId(), new GCPastMetadata(gcid.getExpiration()));
  }
  
  /**
   * remove a member
   * @param id the id to remove
   */
  public void removeId(Id id) {
    GCId gcid = (GCId) id;
    ids.removeId(gcid.getId());
    timeouts.remove(gcid.getId());
  }
  
  /**
   * test membership
   * @param id the id to test
   * @return true of id is a member, false otherwise
   */
  public boolean isMemberId(Id id) {
    GCId gcid = (GCId) id;
    return ids.isMemberId(gcid.getId());
  }
  
  /**
   * return a subset of this set, consisting of the member ids in a given range
   * @param from the lower end of the range (inclusive)
   * @param to the upper end of the range (exclusive)
   * @return the subset
   */
  public IdSet subSet(IdRange range) {
    return new GCIdSet(ids.subSet(((GCIdRange) range).getRange()), timeouts);
  }
  
  /**
   * return an iterator over the elements of this set
   * @return the interator
   */
  public Iterator<Id> getIterator() {
    return new Iterator<Id>() {
      Iterator<Id> i = ids.getIterator();
      
      public boolean hasNext() { return i.hasNext(); }
      public Id next() { return getGCId((Id) i.next()); }
      public void remove() { throw new UnsupportedOperationException("Remove on GCIdSet()!"); }
    };
  }
  
  protected GCId getGCId(Id id) {
    GCPastMetadata metadata = (GCPastMetadata) timeouts.get(id);
    
    if (metadata != null)
      return new GCId(id, metadata.getExpiration());
    else
      return new GCId(id, GCPastImpl.DEFAULT_EXPIRATION);
  }
  
  /**
   * return this set as an array
   * @return the array
   */
  public Id[] asArray() {
    Id[] array = ids.asArray();
    
    for (int i=0; i<array.length; i++)
      array[i] = getGCId(array[i]);
    
    return array;
  }
  
  /**
   * return a hash of this set
   *
   * @return the hash of this set
   */
  public byte[] hash() {
    throw new UnsupportedOperationException("hash on GCIdSet()!");
  }
  
  /**
   * Determines equality
   *
   * @param other To compare to
   * @return Equals
   */
  public boolean equals(Object o) {
    GCIdSet other = (GCIdSet) o;
    
    if (numElements() != other.numElements())
      return false;
    
    Iterator<Id> i = ids.getIterator();
    while (i.hasNext())
      if (! other.isMemberId((Id) i.next()))
        return false;
    
    return true;
  }
  
  /**
   * Returns the hashCode
   *
   * @return hashCode
   */
  public int hashCode() {
    return ids.hashCode();
  }
  
  /**
   * Prints out the string
   *
   * @return A string
   */
  public String toString() {
    return "{GCIdSet of size " + numElements() + "}";
  }
  
  /**
   * Clones this object
   *
   * @return a clone
   */
  public Object clone() {
    return new GCIdSet(ids, timeouts);
  }
  
  /**
    * Returns a new, empty IdSet of this type
   *
   * @return A new IdSet
   */
  public IdSet build() {
    return new GCIdSet(ids.build(), new RedBlackMap());
  }
}
