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

import java.util.*;

import rice.p2p.commonapi.*;

/**
 * @(#) MultringIdSet.java
 *
 * Represents a set of ids in a multiring heirarchy
 * 
 * @version $Id: MultiringIdSet.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public class MultiringIdSet implements IdSet {
  
  /**
   * Serialver for backwards compatibility
   */
  static final long serialVersionUID = -7675959536005571206L;
  
  /**
   * The actual IdSet
   */
  protected IdSet set;
  
  /**
  * The ringId of the ids in the set
   */
  protected Id ringId;
  
  /**
   * Constructor
   */
  protected MultiringIdSet(Id ringId, IdSet set) {
    this.ringId = ringId;
    this.set = set;
    
    if ((ringId instanceof RingId) || (set instanceof MultiringIdSet))
      throw new IllegalArgumentException("Illegal creation of MRIdSet: " + ringId.getClass() + ", " + set.getClass());
  }
  
  /**
   * Returns the internal set
   *
   * @return The internal set
   */
  protected IdSet getSet() {
    return set;
  }
  
  /**
   * return the number of elements
   */
  public int numElements() {
    return set.numElements();
  }
  
  /**
   * add a member
   * @param id the id to add
   */
  public void addId(Id id) {
    set.addId(((RingId) id).getId());
  }
  
  /**
    * remove a member
   * @param id the id to remove
   */
  public void removeId(Id id) {
    set.removeId(((RingId) id).getId());
  }
  
  /**
    * test membership
   * @param id the id to test
   * @return true of id is a member, false otherwise
   */
  public boolean isMemberId(Id id) {
    return set.isMemberId(((RingId) id).getId());
  }
  
  /**
   * return a subset of this set, consisting of the member ids in a given range
   * @param from the lower end of the range (inclusive)
   * @param to the upper end of the range (exclusive)
   * @return the subset
   */
  public IdSet subSet(IdRange range) {
    if (range == null)
      return (IdSet) this.clone();
    else
      return new MultiringIdSet(ringId, set.subSet(((MultiringIdRange) range).getRange()));
  }
  
  /**
   * return an iterator over the elements of this set
   * @return the iterator
   */
  public Iterator<Id> getIterator() {
    return new Iterator<Id>() {
      protected Iterator<Id> i = set.getIterator();
      
      public boolean hasNext() {
        return i.hasNext();
      }
      
      public Id next() {
        return RingId.build(ringId, (Id) i.next());
      }
      
      public void remove() {
        i.remove();
      }
    };
  }
  
  /**
   * return this set as an array
   * @return the array
   */
  public Id[] asArray() {
    Id[] result = set.asArray();
    
    for (int i=0; i<result.length; i++)
      result[i] = RingId.build(ringId, result[i]);
    
    return result;
  }
  
  /**
   * return a hash of this set
   *
   * @return the hash of this set
   */
  public byte[] hash() {
    return set.hash();
  }
  
  /**
   * Determines equality
   *
   * @param other To compare to
   * @return Equals
   */
  public boolean equals(Object o) {
    MultiringIdSet other = (MultiringIdSet) o;
    return (other.getSet().equals(set) && other.ringId.equals(ringId));
  }
  
  /**
   * Returns the hashCode
   *
   * @return hashCode
   */
  public int hashCode() {
    return (set.hashCode() + ringId.hashCode());
  }
  
  /**
   * Prints out the string
   *
   * @return A string
   */
  public String toString() {
    return "{RingId " + ringId + " " + set.toString() + "}";
  }
  
  /**
   * Clones this object
   *
   * @return a clone
   */
  public Object clone() {
    return new MultiringIdSet(ringId, (IdSet) set.clone());
  }
  
  /**
   * Returns a new, empty IdSet of this type
   *
   * @return A new IdSet
   */
  public IdSet build() {
    return new MultiringIdSet(ringId, set.build());
  }
}
