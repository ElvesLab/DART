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
package rice.p2p.glacier;

import java.io.IOException;
import java.util.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;

/**
 * DESCRIBE THE CLASS
 *
 * @version $Id: VersionKeyRange.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author ahae
 */
public class VersionKeyRange implements IdRange {

  /**
   * The actual IdRange
   */
  protected IdRange range;

  /**
   * Constructor
   *
   * @param ringId DESCRIBE THE PARAMETER
   * @param range DESCRIBE THE PARAMETER
   */
  protected VersionKeyRange(IdRange range) {
    this.range = range;
  }

  /**
   * get counterclockwise edge of range
   *
   * @return the id at the counterclockwise edge of the range (inclusive)
   */
  public Id getCCWId() {
    return new VersionKey(range.getCCWId(), 0L);
  }

  /**
   * get clockwise edge of range
   *
   * @return the id at the clockwise edge of the range (exclusive)
   */
  public Id getCWId() {
    return new VersionKey(range.getCWId(), 0L);
  }

  /**
   * get the complement of this range
   *
   * @return This range's complement
   */
  public IdRange getComplementRange() {
    throw new RuntimeException("VersionKeyRange.getComplementRange() is not supported!");
  }

  /**
   * returns whether or not this range is empty
   *
   * @return Whether or not this range is empty
   */
  public boolean isEmpty() {
    return range.isEmpty();
  }

  /**
   * test if a given key lies within this range
   *
   * @param key the key
   * @return true if the key lies within this range, false otherwise
   */
  public boolean containsId(Id key) {
    return range.containsId(((VersionKey)key).id);
  }

  /**
   * merges the given range with this range
   *
   * @param merge DESCRIBE THE PARAMETER
   * @return The merge
   */
  public IdRange mergeRange(IdRange merge) {
    throw new RuntimeException("VersionKeyRange.mergeRange() is not supported!");
  }

  /**
   * diffs the given range with this range
   *
   * @param diff DESCRIBE THE PARAMETER
   * @return The merge
   */
  public IdRange diffRange(IdRange diff) {
    throw new RuntimeException("VersionKeyRange.diffRange() is not supported!");
  }

  /**
   * intersects the given range with this range
   *
   * @param intersect DESCRIBE THE PARAMETER
   * @return The merge
   */
  public IdRange intersectRange(IdRange intersect) {
    throw new RuntimeException("VersionKeyRange.intersectRange() is not supported!");
  }

  /**
   * Determines equality
   *
   * @param o DESCRIBE THE PARAMETER
   * @return Equals
   */
  public boolean equals(Object o) {
    return ((VersionKeyRange)o).range.equals(range);
  }

  /**
   * Returns the hashCode
   *
   * @return hashCode
   */
  public int hashCode() {
    return range.hashCode();
  }

  /**
   * Prints out the string
   *
   * @return A string
   */
  public String toString() {
    return "[VKRange " + range + "]";
  }
  
  public void VersionKeyRange(InputBuffer buf, Endpoint endpoint) throws IOException {
    range = endpoint.readIdRange(buf); 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    range.serialize(buf);
  }
}


