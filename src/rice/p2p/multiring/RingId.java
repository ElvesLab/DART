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

import java.lang.ref.*;
import java.io.*;
import java.util.*;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;

/**
 * @(#) RingId.java
 *
 * This class represents an Id with a specific ring attached.
 *
 * @version $Id: RingId.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
public class RingId implements Id {
  public static short TYPE = 2;
  
  /**
   * Serialver for backward compatibility
   */
  private static final long serialVersionUID = -4390496639871320200L;
  
  /**
   * Support for coalesced Ids - ensures only one copy of each Id is in memory
   */
  private static WeakHashMap RINGID_MAP = new WeakHashMap();
  
  /**
   * The id which this ringId represents
   */
  protected Id id;
  
  /**
   * The ringId which this ringId represents
   */
  protected Id ringId;
  
  /**
   * Constructor
   *
   * @param node The node to base this node off of
   */
  private RingId(Id ringId, Id id) {
    this.id = id;
    this.ringId = ringId;
    
    if ((id == null) || (ringId == null))
      throw new IllegalArgumentException("RingId created with args " + ringId + " " + id);
    if (id instanceof RingId)
      throw new IllegalArgumentException("RingId created with id as a RingId!" + ringId + " " + id);
    if (ringId instanceof RingId)
      throw new IllegalArgumentException("RingId created with ringId as a RingId!" + ringId + " " + id);
  }  
  
  /**
   * Constructor.
   *
   * @param material an array of length at least IdBitLength/32 containing raw Id material.
   */
  public static RingId build(Id ringId, Id id) {   
    return resolve(new RingId(ringId, id));
  }
  
  /**
   * Method which resolves the given id to the canonical one
   *
   * @param id The id to resolver
   * @return The canonicaial one
   */
  private static RingId resolve(RingId id) {
    synchronized (RINGID_MAP) {
      WeakReference ref = (WeakReference) RINGID_MAP.get(id);
      RingId result = null;
      
      if ((ref != null) && ((result = (RingId) ref.get()) != null)) {
        return result;
      } else {
        RINGID_MAP.put(id, new WeakReference(id));
        return id;
      }
    }    
  }
  
  /**
   * Define readResolve, which will replace the deserialized object with the canootical
   * one (if one exists) to ensure RingId coalescing.
   *
   * @return The real RingId
   */
  private Object readResolve() throws ObjectStreamException {
    // commented out to preserve backward compatibility with serialization 
    // should be able to reenable in the future
    //return resolve(this);
    return this;
  }
  
  /**
   * Method which splits apart a ringid string and 
   * returns the RingID
   *
   * @param s The ring to parse
   * @return The result
   */
  public static RingId build(String s) {
    String[] sArray = s.split("\\(|\\)| |,");
    return build(rice.pastry.Id.build(sArray[1]), rice.pastry.Id.build(sArray[3]));
  }
  
  /**
   * Returns this ringid's id
   *
   * @return The id of this ringid
   */
  public Id getId() {
    return id;
  }
  
  /**
   * Returns this ringid's ring id
   *
   * @return The ring id of this ringid
   */
  public Id getRingId() {
    return ringId;
  }
  
  /**
   * Checks if this Id is between two given ids ccw (inclusive) and cw (exclusive) on the circle
   *
   * @param ccw the counterclockwise id
   * @param cw the clockwise id
   * @return true if this is between ccw (inclusive) and cw (exclusive), false otherwise
   */
  public boolean isBetween(Id ccw, Id cw) {
    if (!(ccw instanceof RingId) || (!((RingId)ccw).getRingId().equals(ringId)) ||
        !(cw instanceof RingId) || (!((RingId)cw).getRingId().equals(ringId)))
      throw new IllegalArgumentException("Defined only for RingIds from the same ring!");

    return id.isBetween(((RingId)ccw).getId(), ((RingId)cw).getId());
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
    if (!(nid instanceof RingId) || (!((RingId)nid).getRingId().equals(ringId)))
      throw new IllegalArgumentException("Defined only for RingIds from the same ring!");

    return id.clockwise(((RingId)nid).getId());
  }
  
  /**
   * Returns an Id corresponding to this Id plus a given distance
   *
   * @param offset the distance to add
   * @return the new Id
   */
  public Id addToId(Distance offset) {
    return build(ringId, id.addToId(offset));
  }
  
  /**
   * Returns the shorter numerical distance on the ring between a pair of Ids.
   *
   * @param nid the other node id.
   * @return the distance between this and nid.
   */
  public Distance distanceFromId(Id nid) {
    if (!(nid instanceof RingId) || (!((RingId)nid).getRingId().equals(ringId)))
      throw new IllegalArgumentException("Defined only for RingIds from the same ring!");

    return id.distanceFromId(((RingId)nid).getId());
  }
  
  /**
   * Returns the longer numerical distance on the ring between a pair of Ids.
   *
   * @param nid the other node id.
   * @return the distance between this and nid.
   */
  public Distance longDistanceFromId(Id nid) {
    if (!(nid instanceof RingId) || (!((RingId)nid).getRingId().equals(ringId)))
      throw new IllegalArgumentException("Defined only for RingIds from the same ring!");

    return id.longDistanceFromId(((RingId)nid).getId());
  }
  
  /**
   * Returns a (mutable) byte array representing this Id
   *
   * @return A byte[] representing this Id
   */
  public byte[] toByteArray() {
    return id.toByteArray();
  }
  
  /**
   * Stores the byte[] value of this Id in the provided byte array
   *
   * @return A byte[] representing this Id
   */
  public void toByteArray(byte[] array, int offset) {
    id.toByteArray(array, offset);
  }
  
  /**
   * Returns the length of the byte[] representing this Id
   *
   * @return The length of the byte[] representing this Id
   */
  public int getByteArrayLength() {
    return id.getByteArrayLength();
  }
  
  /**
   * Returns whether or not this object is equal to the provided one
   *
   * @param o The object to compare to
   * @return Whether or not this is object is equal
   */
  public boolean equals(Object o) {
    if (! (o instanceof RingId))
      return false;
    
    return (((RingId) o).id.equals(id) && ((RingId) o).ringId.equals(ringId));
  }
  
  /**
   * Returns the hashCode
   *
   * @return hashCode
   */
  public int hashCode() {
    return (id.hashCode() * ringId.hashCode());
  }
  
  /**
   * Returns a string representing this ring Id.
   *
   * @return A string with all of this Id
   */
  public String toString() {
    return "(" + ringId + ", " + id + ")";
  }
  
  /**
   * Returns a string representing the full length of this Id.
   *
   * @return A string with all of this Id
   */
  public String toStringFull() {
    return "(" + ringId.toStringFull() + ", " + id.toStringFull() + ")";
  }
  
  /**
   * Returns this id compared to the target
   *
   * @return The comparison
   */
  public int compareTo(Id o) {
    return id.compareTo(((RingId)o).id);
  }

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeShort(ringId.getType());
    ringId.serialize(buf);
    buf.writeShort(id.getType());
    id.serialize(buf);
  }

  public RingId(InputBuffer buf, Endpoint endpoint) throws IOException {    
    ringId = endpoint.readId(buf, buf.readShort()); 
    id = endpoint.readId(buf, buf.readShort()); 
  }
}




