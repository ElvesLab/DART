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
package rice.pastry;

import java.io.*;

import rice.p2p.commonapi.rawserialization.*;

/**
 * Represents a contiguous range of Pastry ids. *
 * 
 * @version $Id: IdRange.java 3613 2007-02-15 14:45:14Z jstewart $
 * 
 * @author Peter Druschel
 */

public class IdRange implements rice.p2p.commonapi.IdRange, Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = -361018850912613915L;

  private boolean empty;

  private Id ccw;

  private Id cw;

  /**
   * Constructor.
   * 
   * @param ccw the id at the counterclockwise edge of the range (inclusive)
   * @param cw the id at the clockwise edge of the range (exclusive)
   */
  public IdRange(Id ccw, Id cw) {
    empty = false;
    this.ccw = ccw;
    this.cw = cw;
  }

  /**
   * Constructor, constructs an empty IdRange
   *  
   */
  public IdRange() {
    empty = true;
    ccw = Id.build();
    cw = ccw;
  }

  /*
   * Constructs an empty/full range
   * 
   * @param type - if type is true then constructs an empty range, else
   * constructs a full range
   */
  public IdRange(boolean type) {
    empty = type;
    ccw = Id.build();
    cw = ccw;

  }

  /**
   * Copy constructor.
   */
  public IdRange(IdRange o) {
    this.empty = o.empty;
    this.ccw = o.ccw;
    this.cw = o.cw;
  }

  public int hashCode() {
    if (empty) return 0;
    if (isFull()) return 1;
    return ccw.hashCode()^cw.hashCode();
  }
  
  /**
   * equality operator
   * 
   * @param obj the other IdRange
   * @return true if the IdRanges are equal
   */
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (!(obj instanceof IdRange)) return false;
    IdRange o = (IdRange) obj;

    if (empty && o.empty)
      return true;
    if (isFull() && o.isFull())
      return true;
    if (empty == o.empty && ccw.equals(o.ccw) && cw.equals(o.cw))
      return true;
    else
      return false;
  }

  /**
   * return the size of the range
   * 
   * @return the numerical distance of the range
   */
  private Id.Distance size() {
    if (ccw.clockwise(cw))
      return ccw.distance(cw);
    else
      return ccw.longDistance(cw);
  }

  /**
   * test if the range is empty
   * 
   * @return true if the range is empty
   */
  public boolean isEmpty() {
    return empty;
  }

  /**
   * test if the range is the full circle
   * 
   * @return true if the range is full circle
   */
  public boolean isFull() {
    return ccw.equals(cw) && !empty;
  }

  /**
   * test if this range is adjacent to another range
   * 
   * @param o another range
   * @return true if the range is asjacent to o
   */
  public boolean isAdjacent(IdRange o) {
    return (ccw.equals(o.cw) || o.ccw.equals(cw)) && !empty && !o.empty
        && !isFull() && !o.isFull();
  }

  /**
   * test if a given key lies within this range
   * 
   * @param key the key
   * @return true if the key lies within this range, false otherwise
   */
  public boolean contains(Id key) {
    if (ccw.equals(cw) && !empty)
      return true;
    else
      return key.isBetween(ccw, cw);
  }

  /**
   * get counterclockwise edge of range
   * 
   * @return the id at the counterclockwise edge of the range (inclusive)
   */
  public Id getCCW() {
    return ccw;
  }

  /**
   * get clockwise edge of range
   * 
   * @return the id at the clockwise edge of the range (exclusive)
   */
  public Id getCW() {
    return cw;
  }

  /**
   * set counterclockwise edge of range
   * 
   * @param ccw the new id at the counterclockwise edge of the range (inclusive)
   */
//  private void setCCW(Id ccw) {
//    this.ccw = ccw;
//    empty = false;
//  }

  /**
   * set clockwise edge of range
   * 
   * @param cw the new id at the clockwise edge of the range (exclusive)
   */
//  private void setCW(Id cw) {
//    this.cw = cw;
//    empty = false;
//  }

  /**
   * merge two ranges if this and other don't overlap, are not adjacent, and
   * this is not empty, then the result is this
   * 
   * @param o the other range
   * @return the resulting range
   */
  public IdRange merge(IdRange o) {

    if (o.empty || (ccw.equals(cw) && !empty))
      return this;
    if (empty || (o.ccw.equals(o.cw) && !o.empty))
      return o;

    boolean ccwIn = ccw.isBetween(o.ccw, o.cw) || ccw.equals(o.cw);
    boolean cwIn = cw.isBetween(o.ccw, o.cw);
    boolean occwIn = o.ccw.isBetween(ccw, cw) || o.ccw.equals(cw);
    boolean ocwIn = o.cw.isBetween(ccw, cw);

    if (ccwIn && cwIn && occwIn && ocwIn) {
      // ranges cover entire ring
      return new IdRange(ccw, ccw);
    }

    if (ccwIn) {
      if (cwIn)
        return o;
      else
        return new IdRange(o.ccw, cw);
    }

    if (cwIn) {
      return new IdRange(ccw, o.cw);
    }

    if (occwIn) {
      return this;
    }

    // no intersection
    return this;

  }

  /**
   * get the complement of this range on the ring
   * 
   * @return the complement range
   */
  public IdRange complement() {
    if (ccw.equals(cw) && !empty)
      return new IdRange();
    else
      return new IdRange(cw, ccw);
  }

  /**
   * intersect two ranges returns an empty range if the ranges don't intersect
   * 
   * two ranges may intersect in two ranges on the circle; this method produces
   * one such range of intersection if one exists the other range of
   * intersection can be computed by invoking o.intersect(this)
   * 
   * @param o the other range
   * @return the result range
   */
  public IdRange intersect(IdRange o) {

    if (empty || o.empty)
      return new IdRange();
    if (ccw.equals(cw))
      return o;
    if (o.ccw.equals(o.cw))
      return this;

    boolean ccwIn = ccw.isBetween(o.ccw, o.cw);
    boolean cwIn = cw.isBetween(o.ccw, o.cw) && !cw.equals(o.ccw);
    boolean occwIn = o.ccw.isBetween(ccw, cw);
    boolean ocwIn = o.cw.isBetween(ccw, cw) && !o.cw.equals(ccw);

    if (ccwIn && cwIn && occwIn && ocwIn) {
      // ranges intersect in two ranges, return ccw range
      return new IdRange(ccw, o.cw);
    }

    if (ccwIn) {
      if (cwIn)
        return this;
      else
        return new IdRange(ccw, o.cw);
    }

    if (cwIn) {
      return new IdRange(o.ccw, cw);
    }

    if (occwIn) {
      return o;
    }

    // no intersection
    return new IdRange();

  }

  /**
   * compute the difference between two ranges (exclusive or of keys in the two
   * ranges)
   * 
   * two ranges may differ in two ranges on the circle; this method produces one
   * such range of difference if one exists the other range of difference can be
   * computed by invoking o.diff(this)
   * 
   * @param o the other range
   * @return the result range
   */
  public IdRange diff(IdRange o) {
    IdRange res = intersect(o.complement());
    if (res.isEmpty())
      res = complement().intersect(o);
    return res;
  }

  /**
   * subtract the other range from this computes the ranges of keys that are in
   * this but not in o
   * 
   * subtracting a range may produce two ranges on the circle; this method
   * produces one such ranges under control of the cwPart parameter
   * 
   * @param o the other range
   * @param cwPart if true, returns the clockwise part of the range subtraction,
   *          else the counterclockwise part
   * @return the result range
   */
  public IdRange subtract(IdRange o, boolean cwPart) {
    if (!cwPart)
      return intersect(o.complement());
    else
      return o.complement().intersect(this);
  }

  /**
   * get counterclockwise half of the range
   * 
   * @return the range corresponding to the ccw half of this range
   */
  public IdRange ccwHalf() {
    if (empty)
      return new IdRange();
    if (isFull())
      return new IdRange(Id.build(Id.Null), Id.build(Id.Half));

    Id newCW = ccw.add(size().shift(1, 0, true));
    return new IdRange(ccw, newCW);
  }

  /**
   * get clockwise half of the range
   * 
   * @return the range corresponding to the cw half of this range
   */
  public IdRange cwHalf() {
    if (empty)
      return new IdRange();
    if (isFull())
      return new IdRange(Id.build(Id.Half), Id.build(Id.Null));

    Id newCCW = ccw.add(size().shift(1, 0, true));
    return new IdRange(newCCW, cw);
  }

  /**
   * Returns a string representation of the range.
   */

  public String toString() {
    if (empty)
      return "IdRange: empty";
    else
      return "IdRange: from:" + ccw + " to:" + cw;//+" " + (isFull() ? "full" : " size:"+size());
  }

  // Common API Support

  /**
   * test if a given key lies within this range
   * 
   * @param key the key
   * @return true if the key lies within this range, false otherwise
   */
  public boolean containsId(rice.p2p.commonapi.Id key) {
    return contains((Id) key);
  }

  /**
   * get counterclockwise edge of range
   * 
   * @return the id at the counterclockwise edge of the range (inclusive)
   */
  public rice.p2p.commonapi.Id getCCWId() {
    return getCCW();
  }

  /**
   * get clockwise edge of range
   * 
   * @return the id at the clockwise edge of the range (exclusive)
   */
  public rice.p2p.commonapi.Id getCWId() {
    return getCW();
  }

  /**
   * get the complement of this range
   * 
   * @return This range's complement
   */
  public rice.p2p.commonapi.IdRange getComplementRange() {
    return complement();
  }

  /**
   * merges the given range with this range
   * 
   * @return The merge
   */
  public rice.p2p.commonapi.IdRange mergeRange(rice.p2p.commonapi.IdRange range) {
    return merge((IdRange) range);
  }

  /**
   * diffs the given range with this range
   * 
   * @return The merge
   */
  public rice.p2p.commonapi.IdRange diffRange(rice.p2p.commonapi.IdRange range) {
    return diff((IdRange) range);
  }

  /**
   * intersects the given range with this range
   * 
   * @return The merge
   */
  public rice.p2p.commonapi.IdRange intersectRange(
      rice.p2p.commonapi.IdRange range) {
    return intersect((IdRange) range);
  }

  public IdRange(InputBuffer buf) throws IOException {    
    cw = Id.build(buf);
    ccw = Id.build(buf);
    empty = buf.readBoolean();
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    cw.serialize(buf);
    ccw.serialize(buf);
    buf.writeBoolean(empty);
  }
}

