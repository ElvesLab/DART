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

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;

/**
 * @(#) IdBloomFilter.java
 *
 * Class which is an implementation of a bloom filter which takes Ids as elements.  
 * This class simply wraps a normal BloomFilter, but provides convenient methods
 * for constructing and checking for the existence of Ids.
 *
 * @version $Id: IdBloomFilter.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
public class IdBloomFilter implements Serializable {
  
  // serialver for backwards compatibility
  private static final long serialVersionUID = -9122948172786936161L;
  
  // note cannot configure these because Serializable
  /**
   * The number of bits per key in bloom filters
   */
  public static int NUM_BITS_PER_KEY = 4;

  /**
   * The number of different hash functions to use in bloom filters
   */
  public static int NUM_HASH_FUNCTIONS = 2;
  
  /**
   * An internal byte[] for managing ids in a memory-efficent manner
   * 
   * The deal with this is that we will be creating a ton of arrays in addId/check.  
   * So, what we do is allocate only 1 on a lazy basis.  The call to checkArray()
   * will construct a new array if there isn't one yet, then it will encode the 
   * id into the array.
   */
  protected transient byte[] array;
  
  /**
   * The parameters to the hash functions for this bloom filter
   */
  protected BloomFilter filter;
    
  /**
    * Constructor which takes the number of hash functions to use
   * and the length of the set to use.
   *
   * @param num The number of hash functions to use
   * @param length The length of the underlying bit set
   */
  public IdBloomFilter(IdSet set) {
    int size = (set.numElements() < 64 ? 64 : set.numElements());
    this.filter = new BloomFilter(NUM_HASH_FUNCTIONS, NUM_BITS_PER_KEY * size);
    Iterator<Id> i = set.getIterator();  
    
    while (i.hasNext())
      addId((Id) i.next());
  }
  
  /**
   * Internal method for checking to see if the array exists, and if not,
   * instantiating it.  It also places the given Id into the array.
   * 
   * See the documentation of the member variable array for more info.
   *
   * @param id An id to build the array from
   */
  protected void checkArray(Id id) {
    if (array == null) 
      array = id.toByteArray();
    else
      id.toByteArray(array, 0);
  }
  
  /**
   * Method which adds an Id to the underlying bloom filter
   *
   * @param id The id to add
   */
  protected void addId(Id id) {
    checkArray(id);
    filter.add(array);
  }
  
  /**
   * Method which returns whether or not an Id *may* be in the set.  Specifically, 
   * if this method returns false, the element is definitely not in the set.  Otherwise, 
   * if true is returned, the element may be in the set, but it is not guaranteed.
   *
   * @param id The id to check for
   */
  public boolean check(Id id) {
    checkArray(id);
    return filter.check(array);
  }
  
  /**
   * Method which checks an entire IdSet to see if they exist in this bloom filter, and
   * returns the response by adding elements to the other provided id set.
   *
   * @param set THe set to check for
   * @param result The set to put the non-existing objects into
   * @param max The maximum number of keys to return
   */
  public void check(IdSet set, IdSet result, int max) {
    Iterator<Id> it = set.getIterator();
    int count = 0;
    
    while (it.hasNext() && (count < max)) {
      Id next = (Id) it.next();
      
      if (! check(next)) {
        result.addId(next);
        count++;
      }
    }
  }

  public IdBloomFilter(InputBuffer buf) throws IOException {
//    array = new byte[buf.readInt()];
//    buf.read(array);

    filter = new BloomFilter(buf);
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
//    buf.writeInt(array.length);
//    buf.write(array, 0, array.length);
    
    filter.serialize(buf); 
  }
}
