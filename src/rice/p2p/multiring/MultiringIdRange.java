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

import java.io.IOException;
import java.util.*;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;

/**
 * @(#) MutliringIdRange.java
 *
 * Represents a contiguous range of Ids in a multiring heirarchy.
 * 
 * @version $Id: MultiringIdRange.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public class MultiringIdRange implements IdRange {
  
  /**
   * The actual IdRange
   */
  protected IdRange range;
  
  /**
   * The ringId of the nodes in the range
   */
  protected Id ringId;
  
  /**
   * Constructor
   */
  protected MultiringIdRange(Id ringId, IdRange range) {
    this.ringId = ringId;
    this.range = range;
    
    if ((ringId instanceof RingId) || (range instanceof MultiringIdRange))
      throw new IllegalArgumentException("Illegal creation of MRIdRange: " + ringId.getClass() + ", " + range.getClass());
  }
  
  /**
   * Returns the internal range
   *
   * @return The internal range
   */
  protected IdRange getRange() {
    return range;
  }
  
  /**
   * test if a given key lies within this range
   *
   * @param key the key
   * @return true if the key lies within this range, false otherwise
   */
  public boolean containsId(Id key) {
    if (key instanceof RingId) {
      RingId rkey = (RingId) key;
      if (!rkey.getRingId().equals(this.ringId)) {        
        //System.err.println("ERROR: Testing membership for keys in a different ring (got id " + key + "), range " + this);
        return false;
      }
      
      return range.containsId(rkey.getId());
    } else throw new IllegalArgumentException("Cannot test membership for keys other than RingId");
  }
  
  /**
  * get counterclockwise edge of range
   *
   * @return the id at the counterclockwise edge of the range (inclusive)
   */
  public Id getCCWId() {
    return RingId.build(ringId, range.getCCWId());
  }
  
  /**
    * get clockwise edge of range
   *
   * @return the id at the clockwise edge of the range (exclusive)
   */
  public Id getCWId() {
    return RingId.build(ringId, range.getCWId());
  }
  
  /**
    * get the complement of this range
   *
   * @return This range's complement
   */
  public IdRange getComplementRange() {
    return new MultiringIdRange(ringId, range.getComplementRange());
  }
  
  /**
    * merges the given range with this range
   *
   * @return The merge
   */
  public IdRange mergeRange(IdRange merge) {
    return new MultiringIdRange(ringId, range.mergeRange(((MultiringIdRange) merge).getRange()));
  }
  
  /**
    * diffs the given range with this range
   *
   * @return The merge
   */
  public IdRange diffRange(IdRange diff) {
    return new MultiringIdRange(ringId, range.diffRange(((MultiringIdRange) diff).getRange()));
  }
  
  /**
    * intersects the given range with this range
   *
   * @return The merge
   */
  public IdRange intersectRange(IdRange intersect) {
    return new MultiringIdRange(ringId, range.intersectRange(((MultiringIdRange) intersect).getRange()));
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
   * Determines equality
   *
   * @param other To compare to
   * @return Equals
   */
  public boolean equals(Object o) {
    MultiringIdRange other = (MultiringIdRange) o;
    return (other.getRange().equals(range) && other.ringId.equals(ringId));
  }
  
  /**
   * Returns the hashCode
   *
   * @return hashCode
   */
  public int hashCode() {
    return (range.hashCode() + ringId.hashCode());
  }
  
  /**
   * Prints out the string
   *
   * @return A string
   */
  public String toString() {
    return "{RingId " + ringId + " " + range.toString() + "}";
  }

  public MultiringIdRange(InputBuffer buf, Endpoint endpoint) throws IOException {
    ringId = endpoint.readId(buf, buf.readShort());
    range = endpoint.readIdRange(buf);
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeShort(ringId.getType());
    ringId.serialize(buf);
    range.serialize(buf);
  }
}



