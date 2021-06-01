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

package rice.p2p.commonapi;

import java.io.Serializable;
import java.util.*;

/**
 * @(#) IdSet.java
 *
 * Represents a set of ids.
 * 
 * @version $Id: IdSet.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public interface IdSet extends Serializable {

  /**
   * return the number of elements
   */
  public int numElements();

  /**
   * add a member
   * @param id the id to add
   */
  public void addId(Id id);

  /**
   * remove a member
   * @param id the id to remove
   */
  public void removeId(Id id);

  /**
   * test membership
   * @param id the id to test
   * @return true of id is a member, false otherwise
   */
  public boolean isMemberId(Id id);

  /**
   * return a subset of this set, consisting of the member ids in a given range
   * @param from the lower end of the range (inclusive)
   * @param to the upper end of the range (exclusive)
   * @return the subset
   */
  public IdSet subSet(IdRange range);

  /**
   * return an iterator over the elements of this set
   * @return the interator
   */
  public Iterator<Id> getIterator();
  
  /**
   * return this set as an array
   * @return the array
   */
  public Id[] asArray();
  
  /**
   * return a hash of this set
   *
   * @return the hash of this set
   */
  public byte[] hash();
  
  /**
   * Override clone() to make it publicly accessible
   *
   * @return A clone of this set
   */
  public Object clone();
  
  /**
   * Returns a new, empty IdSet of this type
   *
   * @return A new IdSet
   */
  public IdSet build();
}
