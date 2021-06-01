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

import java.io.IOException;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.util.*;

/**
 * @(#) GCId.java
 *
 * This class is the internal representation of an Id with an
 * expiration time in the GC Past system.  Should not be used
 * by applications.
 *
 * @version $Id: GCId.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
public class GCId implements Id {
  public static final short TYPE = 3;
  
  /**
   * The id which this ringId represents
   */
  protected Id id;
  
  /**
   * The ringId which this ringId represents
   */
  protected long expiration;
  
  /**
   * Constructor
   *
   * @param node The node to base this node off of
   */
  public GCId(Id id, long expiration) {
    this.id = id;
    this.expiration = expiration;
  }
  
  /**
   * Returns this gcid's id
   *
   * @return The id of this ringid
   */
  public Id getId() {
    return id;
  }
  
  /**
   * Returns this gcid's expiration time
   *
   * @return The expiration time of this gcid
   */
  public long getExpiration() {
    return expiration;
  }
  
  /**
   * Checks if this Id is between two given ids ccw (inclusive) and cw (exclusive) on the circle
   *
   * @param ccw the counterclockwise id
   * @param cw the clockwise id
   * @return true if this is between ccw (inclusive) and cw (exclusive), false otherwise
   */
  public boolean isBetween(Id ccw, Id cw) {
    return id.isBetween(((GCId) ccw).getId(), ((GCId) cw).getId());
  }
  
  /**
   * Checks to see if the Id nid is clockwise or counterclockwise from this, on the ring. An Id is
   * clockwise if it is within the half circle clockwise from this on the ring. An Id is considered
   * counter-clockwise from itself.
   *
   * @param nid The id to compare to
   * @return true if clockwise, false otherwise.
   */
  public boolean clockwise(Id nid) {
    return id.clockwise(((GCId) nid).getId());
  }
  
  /**
   * Returns an Id corresponding to this Id plus a given distance
   *
   * @param offset the distance to add
   * @return the new Id
   */
  public Id addToId(Distance offset) {
    return new GCId(id.addToId(offset), expiration);
  }
  
  /**
   * Returns the shorter numerical distance on the ring between a pair of Ids.
   *
   * @param nid the other node id.
   * @return the distance between this and nid.
   */
  public Distance distanceFromId(Id nid) {
    return id.distanceFromId(((GCId) nid).getId());
  }
  
  /**
   * Returns the longer numerical distance on the ring between a pair of Ids.
   *
   * @param nid the other node id.
   * @return the distance between this and nid.
   */
  public Distance longDistanceFromId(Id nid) {
    return id.longDistanceFromId(((GCId) nid).getId());
  }
  
  /**
   * Returns a (mutable) byte array representing this Id
   *
   * @return A byte[] representing this Id
   */
  public byte[] toByteArray() {
    byte[] result = new byte[getByteArrayLength()];
    
    toByteArray(result, 0);
    
    return result;
  }
  
  /**
   * Stores the byte[] value of this Id in the provided byte array
   *
   * @return A byte[] representing this Id
   */
  public void toByteArray(byte[] array, int offset) {
    id.toByteArray(array, offset);
    MathUtils.longToByteArray(expiration, array, offset+id.getByteArrayLength());
  }
  
  /**
   * Returns the length of the byte[] representing this Id
   *
   * @return The length of the byte[] representing this Id
   */
  public int getByteArrayLength() {
    return id.getByteArrayLength() + 8;
  }
  
  /**
   * Returns whether or not this object is equal to the provided one
   *
   * @param o The object to compare to
   * @return Whether or not this is object is equal
   */
  public boolean equals(Object o) {
    if (! (o instanceof GCId))
      return false;
    
    return (((GCId) o).id.equals(id) && (((GCId) o).expiration == expiration));
  }
  
  /**
   * Returns the hashCode
   *
   * @return hashCode
   */
  public int hashCode() {
    return id.hashCode();
  }
  
  /**
   * Returns a string representing this ring Id.
   *
   * @return A string with all of this Id
   */
  public String toString() {
    return id + "-" + expiration;
  }
  
  /**
   * Returns a string representing the full length of this Id.
   *
   * @return A string with all of this Id
   */
  public String toStringFull() {
    return id.toStringFull() + "-" + expiration;
  }
  
  /**
   * Returns this id compared to the target
   *
   * @return The comparison
   */
  public int compareTo(Id o) {
    return id.compareTo(((GCId) o).id);
  }

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    // to be peeked
    buf.writeShort(getType());
    buf.writeLong(expiration);
    buf.writeShort(id.getType());
    id.serialize(buf);
  }

  public GCId(InputBuffer buf, Endpoint endpoint) throws IOException {
    buf.readShort();
    expiration = buf.readLong();     
    id = endpoint.readId(buf, buf.readShort());
  }
}




