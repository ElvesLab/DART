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
import java.lang.ref.*;
import java.util.*;

import rice.environment.random.RandomSource;
import rice.p2p.commonapi.rawserialization.RawSerializable;

/**
 * Represents a Pastry identifier for a node, object or key. A single identifier and the bit length
 * for Ids is stored in this class. Ids are stored little endian.  NOTE: Ids are immutable, and are
 * coalesced for memory efficiency.  New Ids are to be constructed from the build() methods, which
 * ensure that only one copy of each Id is in memory at a time.
 *
 * @version $Id: Id.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Andrew Ladd
 * @author Peter Druschel
 * @author Alan Mislove
 */
public class Id implements rice.p2p.commonapi.Id, RawSerializable {

  public static final short TYPE = 1;
  
  /**
   * Support for coalesced Ids - ensures only one copy of each Id is in memory
   */
  private static WeakHashMap ID_MAP = new WeakHashMap();
  
  /**
   * The static translation array
   */
  public static final String tran[] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"};
  
  /**
   * This is the bit length of the node ids. If it is n, then there are 2^n possible different Ids.
   * We currently assume that it is divisible by 32.
   */
  public final static int IdBitLength = 160;
  public final static int nlen = IdBitLength / 32;
  
  /**
   * serialver for backwards compatibility
   */
  static final long serialVersionUID = 2166868464271508935L;
  
  /**
   * Distance constants
   */
  public final static int[] Null = {0, 0, 0, 0, 0};
  public final static int[] One = {1, 0, 0, 0, 0};
  public final static int[] NegOne = {-1, -1, -1, -1, -1};
  public final static int[] Half = {0, 0, 0, 0, 0x80000000};
  
  /**
   * The actual contents of this Id
   */
  private int Id[];

  /**
   * Constructor.
   *
   * @param material an array of length at least IdBitLength/32 containing raw Id material.
   */
  protected Id(int material[]) {
    Id = new int[nlen];
    for (int i = 0; (i < nlen) && (i < material.length); i++) 
      Id[i] = material[i];
  }

  /**
   * return the number of digits in a given base
   *
   * @param base the number of bits in the base
   * @return the number of digits in that base
   */
  public static int numDigits(int base) {
    return IdBitLength / base;
  }

  /**
   * Creates a random Id. For testing purposed only -- should NOT be used to generate real node or
   * object identifiers (low quality of random material).
   *
   * @param rng random number generator
   * @return a random Id
   */
  public static Id makeRandomId(Random rng) {
    byte material[] = new byte[IdBitLength / 8];
    rng.nextBytes(material);
    return build(material);
  }
  
  public static Id makeRandomId(RandomSource rng) {
    byte material[] = new byte[IdBitLength / 8];
    rng.nextBytes(material);
    return build(material);
  }
  
  
  
  // ----- SUPPORT FOR ID COALESCING -----
  
  /**
   * Constructor.
   *
   * @param material an array of length at least IdBitLength/32 containing raw Id material.
   */
  public static Id build(int material[]) {
    return resolve(ID_MAP, new Id(material));
  }
  
  /**
   * Id (Version 0)
   * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   * +   160 Bit                                                     +
   * +                                                               +
   * +                                                               +
   * +                                                               +
   * +                                                               +
   * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   * @param buf
   * @throws IOException
   */
  public static Id build(rice.p2p.commonapi.rawserialization.InputBuffer buf) throws IOException {
    int[] material = new int[nlen];
    for (int i = 0; i < material.length; i++) {
      material[i] = buf.readInt();
    }
    return build(material);     
  }
  
  public void serialize(rice.p2p.commonapi.rawserialization.OutputBuffer buf) throws IOException {
    for (int i = 0; i < Id.length; i++) {
      buf.writeInt(Id[i]);
    }
  }

  
  /**
   * Constructor, which takes the output of a toStringFull() and converts it back
   * into an Id.  Should not normally be used.
   *
   * @param hex The hexadecimal representation from the toStringFull()
   */
  public static Id build(String hex) {
    while (hex.length() < IdBitLength/4) 
      hex = hex + "0";
    
    return build(hex.toUpperCase().toCharArray(), 0, hex.length());
  }
  
  /**
   * Constructor, which takes the output of a toStringFull() and converts it back
   * into an Id.  Should not normally be used.
   *
   * @param hex The hexadecimal representation from the toStringFull()
   */
  public static Id build(char[] chars, int offset, int length) {
    int[] array = new int[nlen];
    
    for (int i=0; i<nlen; i++) 
      for (int j=0; j<8; j++) 
        array[nlen-1-i] = (array[nlen-1-i] << 4) | trans(chars[offset + 8*i + j]);
    
    return build(array);
  }  
  
  /**
   * Internal method for mapping digit -> num
   *
   * @param digit The printed char
   * @param num The byte number
   */
  protected static byte trans(char c) {
    if (('0' <= c) && ('9' >= c))
      return (byte) (c - '0');
    else
      return (byte) (c - 'A' + 10);
  }
  
  /**
   * Constructor.
   *
   * @param material an array of length at least IdBitLength/8 containing raw Id material.
   */
  public static Id build(byte[] material) {
    return build(trans(material));
  }
  
  /**
   * Internal method for mapping byte[] -> int[]
   *
   * @param material The input byte[]
   * @return THe int[]
   */
  protected static int[] trans(byte[] material) {
    int[] array = new int[nlen];
    
    for (int j = 0; (j < IdBitLength / 8) && (j < material.length); j++) {
      int k = material[j] & 0xff;
      array[j / 4] |= k << ((j % 4) * 8);
    }
    
    return array;
  }
  
  /**
   * Constructor. It constructs a new Id with a value of 0 for all bits.
   */
  public static Id build() {
    return build(new int[nlen]);
  }
  
  /**
   * Static method for converting the hex representation into an array of
   * ints.
   *
   * @param hex The hexadecimal representation
   * @return The corresponding int array
   */
  protected static int[] trans(String hex) {
    int[] ints = new int[nlen];
    
    for (int i=0; i<nlen; i++) {
      String s = hex.substring(i*8, (i+1)*8);
      ints[nlen-1-i] = new java.math.BigInteger(s, 16).intValue();
    }
    
    return ints;
  } 
  
  /**
   * Method which performs the coalescing and interaction with the weak hash map
   *
   * @param id The Id to coalesce
   * @return The Id to use
   */
  protected static Id resolve(WeakHashMap map, Id id) {
    synchronized (map) {
      WeakReference ref = (WeakReference) map.get(id);
      Id result = null;
      
      if ((ref != null) && ((result = (Id) ref.get()) != null)) {
        return result;
      } else {
        map.put(id, new WeakReference(id));
        return id;
      }
    }
  }
  
  /**
   * Define readResolve, which will replace the deserialized object with the canootical
   * one (if one exists) to ensure Id coalescing.
   *
   * @return The real Id
   */
  private Object readResolve() throws ObjectStreamException {
    return resolve(ID_MAP, this);
  }
  
  
  // ----- NORMAL ID METHODS -----
  
  /**
   * gets the Id just clockwise from this
   *
   * @return The CW value
   */
  public Id getCW() {
    Distance one = new Distance(One);
    return add(one);
  }

  /**
   * gets the Id just counterclockwise from this
   *
   * @return The CCW value
   */
  public Id getCCW() {
    Distance negone = new Distance(NegOne);
    return add(negone);
  }

  /**
   * Checks if this Id is between two given ids ccw (inclusive) and cw (exclusive) on the circle
   *
   * @param ccw the counterclockwise id
   * @param cw the clockwise id
   * @return true if this is between ccw (inclusive) and cw (exclusive), false otherwise
   */
  public boolean isBetween(Id ccw, Id cw) {
    if (ccw.equals(cw)) {
      return false;
    }

    if (ccw.clockwise(cw)) {
      return this.clockwise(cw) && !this.clockwise(ccw);
    } else {
      return !this.clockwise(ccw) || this.clockwise(cw);
    }
  }

  /**
   * Gets the ith digit in base 2^b. i = 0 is the least significant digit.
   *
   * @param i which digit to get.
   * @param b which power of 2 is the base to get it in.
   * @return the ith digit in base 2^b.
   */
  public int getDigit(int i, int b) {
    int bitIndex = b * i + (IdBitLength % b);
    int index = bitIndex / 32;
    int shift = bitIndex % 32;

    long val = Id[index];
    if (shift + b > 32) {
      val = (val & 0xffffffffL) | (((long) Id[index + 1]) << 32);
    }

    return ((int) (val >> shift)) & ((1 << b) - 1);
  }

  /**
   * produces a Id whose prefix up to row is identical to this, followed by a digit with value
   * column, followed by a suffix of digits with value suffixDigits.
   *
   * @param row the length of the prefix
   * @param column the value of the following digit
   * @param suffixDigit the value of the suffix digits
   * @param b power of 2 of the base
   * @return the resulting Id
   */
  public Id getDomainPrefix(int row, int column, int suffixDigit, int b) {
    Id res = new Id(Id);

    res.setDigit(row, column, b);
    for (int i = 0; i < row; i++) {
      res.setDigit(i, suffixDigit, b);
    }

    return build(res.Id);
  }


  /**
   * produces a set of ids (keys) that are evenly distributed around the id ring. One invocation
   * produces the i-th member of a set of size num. The set is evenly distributed around the ring,
   * with an offset given by this Id. The set is useful for constructing, for instance, Scribe trees
   * with disjoint sets of interior nodes.
   *
   * @param num the number of Ids in the set (must be <= 2^b)
   * @param b the routing base (as a power of 2)
   * @param i the index of the requested member of the set (0<=i<num; the 0-th member is this)
   * @return the resulting set member, or null in case of illegal arguments
   */
  public Id getAlternateId(int num, int b, int i) {
    if (num > (1 << b) || i < 0 || i >= num) {
      return null;
    }

    Id res = new Id(Id);

    int digit = res.getDigit(numDigits(b) - 1, b) + ((1 << b) / num) * i;
    res.setDigit(numDigits(b) - 1, digit, b);

    return build(res.Id);
  }

  // ----- COMMON API SUPPORT -----

  /**
   * Checks if this Id is between two given ids ccw (inclusive) and cw (exclusive) on the circle
   *
   * @param ccw the counterclockwise id
   * @param cw the clockwise id
   * @return true if this is between ccw (inclusive) and cw (exclusive), false otherwise
   */
  public boolean isBetween(rice.p2p.commonapi.Id ccw, rice.p2p.commonapi.Id cw) {
    return isBetween((Id) ccw, (Id) cw);
  }


  /**
   * Sets the ith bit to a given value i = 0 is the least significant bit.
   *
   * @param i which bit to set.
   * @param v new value of bit
   */
//  private void setBit(int i, int v) {
//    int index = i / 32;
//    int shift = i % 32;
//    int val = Id[index];
//    int mask = (1 << shift);
//
//    if (v == 1) {
//      Id[index] = val | mask;
//    } else {
//      Id[index] = val & ~mask;
//    }
//  }


  /**
   * Sets the ith digit in base 2^b. i = 0 is the least significant digit.
   *
   * @param i which digit to get.
   * @param v the new value of the digit
   * @param b which power of 2 is the base to get it in.
   */
  private void setDigit(int i, int v, int b) {
    int bitIndex = b * i + (IdBitLength % b);
    int index = bitIndex / 32;
    int shift = bitIndex % 32;
    int mask = (1 << b) - 1;

    if (shift + b > 32) {
      // digit overlaps a word boundary

      long newd = ((long) (v & mask)) << shift;
      long vmask = ~(((long) mask) << shift);
      long val = Id[index];
      val = (val & 0xffffffffL) | (((long) Id[index + 1]) << 32);

      val = (val & vmask) | newd;

      Id[index] = (int) val;
      Id[index + 1] = (int) (val >> 32);
    } else {
      int newd = (v & mask) << shift;
      int vmask = ~(mask << shift);
      Id[index] = (Id[index] & vmask) | newd;
    }
  }

  /**
   * Blits the Id into a target array.
   *
   * @param target an array of length at least IdBitLength/8 for the Id to be stored in.
   */
  public void blit(byte target[]) {
    blit(target, 0);
  }
  
  /**
    * Blits the distance into a target array, starting at the given offset.
   *
   * @param offset The offset to start at
   * @param target an array of length at least IdBitLength/8 for the distance to be stored in.
   */
  public void blit(byte target[], int offset) {
    for (int j = 0; j < IdBitLength / 8; j++) {
      int k = Id[j / 4] >> ((j % 4) * 8);
      target[offset+j] = (byte) (k & 0xff);
    }
  }

  /**
   * Copy the Id into a freshly generated array.
   *
   * @return a fresh copy of the Id material
   */
  public byte[] copy() {
    byte target[] = new byte[IdBitLength / 8];
    blit(target);
    return target;
  }


  /**
   * Equality operator for Ids.
   *
   * @param obj a Id object
   * @return true if they are equal, false otherwise.
   */
  public boolean equals(Object obj) {
    if ((obj == null) || (! (obj instanceof Id)))
      return false;
    
    Id nid = (Id) obj;

    for (int i = 0; i < nlen; i++) {
      if (Id[i] != nid.Id[i]) {
        return false;
      }
    }

    return true;
  }

  /**
   * Comparison operator for Ids. The comparison that occurs is a numerical comparison.
   *
   * @param obj the Id to compare with.
   * @return negative if this < obj, 0 if they are equal and positive if this > obj.
   */
  public int compareTo(rice.p2p.commonapi.Id obj) {
    Id oth = (Id) obj;

    for (int i = nlen - 1; i >= 0; i--) {
      if (Id[i] != oth.Id[i]) {
        long t = Id[i] & 0x0ffffffffL;
        long o = oth.Id[i] & 0x0ffffffffL;
        if (t < o) {
          return -1;
        } else {
          return 1;
        }
      }
    }

    return 0;
  }

  /**
   * Returns the byte array representation of this Id
   *
   * @return The byte array representation of this id
   */
  public byte[] toByteArray() {
    return copy();
  }

  /**
   * Stores the byte[] value of this Id in the provided byte array
   *
   * @return A byte[] representing this Id
   */
  public void toByteArray(byte[] array, int offset) {
    blit(array, offset);
  }
  
  /**
    * Returns the length of the byte[] representing this Id
   *
   * @return The length of the byte[] representing this Id
   */
  public int getByteArrayLength() {
    return (int) IdBitLength / 8;
  }
  
  /**
   * Hash codes for Ids.
   *
   * @return a hash code.
   */
  public int hashCode() {
    int h = 0;

    /// Hash function is computed by XORing the bits of the Id.
    for (int i = 0; i < nlen; i++) {
      h ^= Id[i];
    }

    return h;
  }

  /**
   * Returns an Id corresponding to this Id plus a given distance
   *
   * @param offset the distance to add
   * @return the new Id
   */
  public Id add(Distance offset) {
    int[] array = new int[nlen];
    long x;
    long y;
    long sum;
    int carry = 0;

    for (int i = 0; i < nlen; i++) {
      x = Id[i] & 0x0ffffffffL;
      y = offset.difference[i] & 0x0ffffffffL;

      sum = x + y + carry;

      if (sum >= 0x100000000L) {
        carry = 1;
      } else {
        carry = 0;
      }

      array[i] = (int) sum;
    }

    return build(array);
  }

  /**
   * Returns the shorter numerical distance on the ring between a pair of Ids.
   *
   * @param nid the other node id.
   * @return the distance between this and nid.
   */
  public Distance distance(Id nid) {
    int[] dist = absDistance(nid);

    if ((dist[nlen - 1] & 0x80000000) != 0) {
      invert(dist);
    }

    Distance d = new Distance(dist);

    return d;
  }

  public Distance distance(Id nid, Distance d) {
    int[] dist = d.difference;
    absDistance(nid, dist);

    if ((dist[nlen - 1] & 0x80000000) != 0) {
      invert(dist);
    }

//    Distance d = new Distance(dist);

    return d;
  }

  /**
   * Returns the longer numerical distance on the ring between a pair of Ids.
   *
   * @param nid the other node id.
   * @return the distance between this and nid.
   */
  public Distance longDistance(Id nid) {
    int[] dist = absDistance(nid);

    if ((dist[nlen - 1] & 0x80000000) == 0) {
      invert(dist);
    }

    Distance d = new Distance(dist);

    return d;
  }


  /**
   * Xor operator for Ids. Sets this Id to the bit-wise XOR of itself and otherId
   *
   * @param otherId a Id object
   */
//  public void xor(Id otherId) {
//    for (int i = 0; i < nlen; i++) {
//      Id[i] ^= otherId.Id[i];
//    }
//  }


  /**
   * Equivalence relation for Ids.
   *
   * @param nid the other node id.
   * @return true if they are equal, false otherwise.
   */
  public boolean equals(Id nid) {
    if (nid == null) {
      return false;
    }

    for (int i = 0; i < nlen; i++) {
      if (Id[i] != nid.Id[i]) {
        return false;
      }
    }

    return true;
  }

  /**
   * Checks to see if the Id nid is clockwise or counterclockwise from this, on the ring. An Id is
   * clockwise if it is within the half circle clockwise from this on the ring. An Id is considered
   * counter-clockwise from itself.
   *
   * @param nid The Id we are comparing to
   * @return true if clockwise, false otherwise.
   */
  public boolean clockwise(Id nid) {
    boolean diffMSB = ((Id[nlen - 1] & 0x80000000) != (nid.Id[nlen - 1] & 0x80000000));
    int x;
    int y;
    int i;

    if ((x = (Id[nlen - 1] & 0x7fffffff)) != (y = (nid.Id[nlen - 1] & 0x7fffffff))) {
      return ((y > x) ^ diffMSB);
    } else {
      for (i = nlen - 2; i >= 0; i--) {
        if (Id[i] != nid.Id[i]) {
          break;
        }
      }

      if (i < 0) {
        return diffMSB;
      } else {
        long xl;
        long yl;

        xl = Id[i] & 0xffffffffL;
        yl = nid.Id[i] & 0xffffffffL;

        return ((yl > xl) ^ diffMSB);
      }
    }
  }

  /**
   * Checks if the ith bit is flipped. i = 0 is the least significant bit.
   *
   * @param i which bit to check.
   * @return true if the bit is set, false otherwise.
   */
  public boolean checkBit(int i) {
    int index = i / 32;
    int shift = i % 32;
    int val = Id[index];
    int mask = (1 << shift);

    if ((val & mask) != 0) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns the index of the most significant differing bit (MSDB).
   *
   * @param nid another node id to compare with.
   * @return the index of the msdb (0 is the least significant) / will return negative if they do
   *      not differ.
   */
  public int indexOfMSDB(Id nid) {
    for (int i = nlen - 1; i >= 0; i--) {
      int cmp = Id[i] ^ nid.Id[i];

      if (cmp != 0) {
        int tmp;
        int j = 0;
        if ((tmp = cmp & 0xffff0000) != 0) {
          cmp = tmp;
          j += 16;
        }
        if ((tmp = cmp & 0xff00ff00) != 0) {
          cmp = tmp;
          j += 8;
        }
        if ((tmp = cmp & 0xf0f0f0f0) != 0) {
          cmp = tmp;
          j += 4;
        }
        if ((tmp = cmp & 0xcccccccc) != 0) {
          cmp = tmp;
          j += 2;
        }
        if ((tmp = cmp & 0xaaaaaaaa) != 0) {
          cmp = tmp;
          j += 1;
        }
        return 32 * i + j;
      }
    }

    return -1;
  }

  /**
   * Returns the index of the most significant different digit (MSDD) in a given base.
   *
   * @param nid another node id to compare with.
   * @param base the base (as a power of two) to compare in.
   * @return the index of the msdd (0 is the least significant) / will return negative if they do
   *      not differ.
   */
  public int indexOfMSDD(Id nid, int base) {
    int ind = indexOfMSDB(nid);

    // ignore trailing LSBs if (IdBitLength % base) > 0
    ind -= IdBitLength % base;

    if (ind < 0) {
      return ind;
    }
    return ind / base;
  }


  /**
   * Returns a string representation of the Id in base 16. The string is a byte string from most to
   * least significant.
   * 
   * This was updated by Jeff because it was becomming a performance bottleneck.
   *
   * @return A String representation of this Id, abbreviated
   */
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("<0x");
    
    int n = IdBitLength / 4;
    for (int i = n-1; i >= n-6; i--) 
      buffer.append(tran[getDigit(i, 4)]);
    
    buffer.append("..>");
    
    return buffer.toString();
  }

  /**
   * Similar to toString(), but not wrapped by <0x ..>
   * @return
   */
  public String toStringBare() {
    StringBuffer buffer = new StringBuffer();
    
    int n = IdBitLength / 4;
    for (int i = n-1; i >= n-6; i--) 
      buffer.append(tran[getDigit(i, 4)]);
    
    
    return buffer.toString();    
  }
  
  /**
   * Returns the complete represntation of this Id, in hex.
   *
   * @return The complete representation of this Id, in hexadecimal
   */
  public String toStringFull() {
    StringBuffer buffer = new StringBuffer();

    int n = IdBitLength / 4;
    for (int i = n - 1; i >= 0; i--) 
      buffer.append(tran[getDigit(i, 4)]);
    
    return buffer.toString();
  }

  /**
   * Checks to see if the Id nid is clockwise or counterclockwise from this, on the ring. An Id is
   * clockwise if it is within the half circle clockwise from this on the ring. An Id is considered
   * counter-clockwise from itself.
   *
   * @param nid DESCRIBE THE PARAMETER
   * @return true if clockwise, false otherwise.
   */
  public boolean clockwise(rice.p2p.commonapi.Id nid) {
    return clockwise((Id) nid);
  }

  /**
   * Returns an Id corresponding to this Id plus a given distance
   *
   * @param offset the distance to add
   * @return the new Id
   */
  public rice.p2p.commonapi.Id addToId(rice.p2p.commonapi.Id.Distance offset) {
    return add((Id.Distance) offset);
  }

  /**
   * Returns the shorter numerical distance on the ring between a pair of Ids.
   *
   * @param nid the other node id.
   * @return the distance between this and nid.
   */
  public rice.p2p.commonapi.Id.Distance distanceFromId(rice.p2p.commonapi.Id nid) {
    return distance((Id) nid);
  }

  /**
   * Returns the longer numerical distance on the ring between a pair of Ids.
   *
   * @param nid the other node id.
   * @return the distance between this and nid.
   */
  public rice.p2p.commonapi.Id.Distance longDistanceFromId(rice.p2p.commonapi.Id nid) {
    return longDistance((Id) nid);
  }

  /**
   * Returns the absolute numerical distance between a pair of Ids.
   *
   * @param nid the other node id.
   * @return an int[] containing the distance between this and nid.
   */
  private int[] absDistance(Id nid) {
    return absDistance(nid,new int[nlen]);
  }
  private int[] absDistance(Id nid, int dist[]) {
    long x;
    long y;
    long diff;
    int carry = 0;

    if (compareTo(nid) > 0) {
      for (int i = 0; i < nlen; i++) {
        x = Id[i] & 0x0ffffffffL;
        y = nid.Id[i] & 0x0ffffffffL;

        diff = x - y - carry;

        if (diff < 0) {
          carry = 1;
        } else {
          carry = 0;
        }

        dist[i] = (int) diff;
      }
    } else {
      for (int i = 0; i < nlen; i++) {
        x = Id[i] & 0x0ffffffffL;
        y = nid.Id[i] & 0x0ffffffffL;

        diff = y - x - carry;

        if (diff < 0) {
          carry = 1;
        } else {
          carry = 0;
        }

        dist[i] = (int) diff;
      }
    }
    return dist;
  }

  /**
   * inverts the distance value stored in an integer array (computes 0-value)
   *
   * @param dist the distance value
   */
  private void invert(int[] dist) {
    int carry = 0;
    long diff;
    for (int i = 0; i < nlen; i++) {
      diff = dist[i] & 0x0ffffffffL;
      diff = 0L - diff - carry;
      if (diff < 0) {
        carry = 1;
      }
      dist[i] = (int) diff;
    }
  }

  /**
   * A class for representing and manipulating the distance between two Ids on the circle.
   *
   * @version $Id: Id.java 4654 2009-01-08 16:33:07Z jeffh $
   * @author amislove
   */
  public static class Distance implements rice.p2p.commonapi.Id.Distance {

    private static final long serialVersionUID = 5464763824924998962L;
    
    private int difference[];

    /**
     * Constructor.
     */
    public Distance() {
      difference = new int[nlen];
    }

    /**
     * Constructor.
     *
     * @param diff The different, as a byte array
     */
    public Distance(byte[] diff) {
      difference = new int[nlen];

      for (int j = 0; (j < IdBitLength / 8) && (j < diff.length); j++) {
        int k = diff[j] & 0xff;
        difference[j / 4] |= k << ((j % 4) * 8);
      }
    }

    /**
     * Constructor.
     *
     * @param diff The difference, as an int array
     */
    public Distance(int[] diff) {
      difference = diff;
    }

    /**
     * Blits the distance into a target array.
     *
     * @param target an array of length at least IdBitLength/8 for the distance to be stored in.
     */
    public void blit(byte target[]) {
      blit(target, 0);
    }
    
    /**
     * Blits the distance into a target array, starting at the given offset.
     *
     * @param offset The offset to start at
     * @param target an array of length at least IdBitLength/8 for the distance to be stored in.
     */
    public void blit(byte target[], int offset) {
      for (int j = 0; j < IdBitLength / 8; j++) {
        int k = difference[j / 4] >> ((j % 4) * 8);
        target[offset+j] = (byte) (k & 0xff);
      }
    }

    /**
     * Copy the distance into a freshly generated array.
     *
     * @return a fresh copy of the distance material
     */
    public byte[] copy() {
      byte target[] = new byte[IdBitLength / 8];
      blit(target);
      return target;
    }

    /**
     * Comparison operator. The comparison that occurs is an absolute magnitude comparison.
     *
     * @param obj the Distance to compare with.
     * @return negative if this < obj, 0 if they are equal and positive if this > obj.
     */
    public int compareTo(rice.p2p.commonapi.Id.Distance obj) {
      Distance oth = (Distance) obj;

      for (int i = nlen - 1; i >= 0; i--) {
        if (difference[i] != oth.difference[i]) {
          long t = difference[i] & 0x0ffffffffL;
          long o = oth.difference[i] & 0x0ffffffffL;
          if (t < o) {
            return -1;
          } else {
            return 1;
          }
        }
      }

      return 0;
    }

    /**
     * Equality operator.
     *
     * @param obj another Distance.
     * @return true if they are the same, false otherwise.
     */
    public boolean equals(Object obj) {
      if (compareTo((Id.Distance)obj) == 0) {
        return true;
      } else {
        return false;
      }
    }

    /**
     * Shift operator. shift(-1,0) multiplies value of this by two, shift(1,0) divides by 2
     *
     * @param cnt the number of bits to shift, negative shifts left, positive shifts right
     * @param fill value of bit shifted in (0 if fill == 0, 1 otherwise)
     * @return this
     */
    public Distance shift(int cnt, int fill) {
      return shift(cnt, fill, false);
    }


    /**
     * Shift operator. shift(-1,0) multiplies value of this by two, shift(1,0) divides by 2
     *
     * @param cnt the number of bits to shift, negative shifts left, positive shifts right
     * @param fill value of bit shifted in (0 if fill == 0, 1 otherwise)
     * @param roundUp if true, round up the results after right shifting
     * @return this
     */
    public Distance shift(int cnt, int fill, boolean roundUp) {
      int carry;
      int bit = 0;
      int lsb = 0;

      if (cnt > 0) {
        for (int j = 0; j < cnt; j++) {
          // shift right one bit
          carry = (fill == 0) ? 0 : 0x80000000;
          for (int i = nlen - 1; i >= 0; i--) {
            bit = difference[i] & 1;
            difference[i] = (difference[i] >>> 1) | carry;
            carry = (bit == 0) ? 0 : 0x80000000;
          }

          if (j == 0) {
            lsb = bit;
          }
        }

        if (roundUp && lsb > 0) {
          inc();
        }
      } else {
        for (int j = 0; j < -cnt; j++) {
          // shift left one bit
          carry = (fill == 0) ? 0 : 1;
          for (int i = 0; i < nlen; i++) {
            bit = difference[i] & 0x80000000;
            difference[i] = (difference[i] << 1) | carry;
            carry = (bit == 0) ? 0 : 1;
          }
        }
      }

      return this;
    }

    /**
     * Hash codes.
     *
     * @return a hash code.
     */
    public int hashCode() {
      int h = 0;

      // Hash function is computed by XORing the bits of the Id.
      for (int i = 0; i < nlen; i++) {
        h ^= difference[i];
      }

      return h;
    }

    /**
     * Returns a string representation of the distance The string is a byte string from most to
     * least significant.
     *
     * @return The string representation of this distance
     */
    public String toString() {
      String s = "0x";

      String tran[] = {"0", "1", "2", "3", "4", "5", "6", "7",
        "8", "9", "A", "B", "C", "D", "E", "F"};

      for (int j = IdBitLength / 8 - 1; j >= 0; j--) {
        int k = difference[j / 4] >> ((j % 4) * 8);
        s = s + tran[(k >> 4) & 0x0f] + tran[k & 0x0f];
      }

      return "< Id.distance " + s + " >";
    }

    // common API Support

    /**
     * Shift operator. shift(-1,0) multiplies value of this by two, shift(1,0) divides by 2
     *
     * @param cnt the number of bits to shift, negative shifts left, positive shifts right
     * @param fill value of bit shifted in (0 if fill == 0, 1 otherwise)
     * @return this
     */
    public rice.p2p.commonapi.Id.Distance shiftDistance(int cnt, int fill) {
      return shift(cnt, fill);
    }

    /**
     * increment this distance
     */
    private void inc() {
      long x;
      long sum;
      int carry = 1;

      // add one
      for (int i = 0; i < nlen; i++) {
        x = difference[i] & 0x0ffffffffL;
        sum = x + carry;
        if (sum >= 0x100000000L) {
          carry = 1;
        } else {
          carry = 0;
        }

        difference[i] = (int) sum;
      }
    }
  }

  public short getType() {
    return TYPE;
  }
  
  public static void main(String[] args) {
    System.out.println(new Id(Half).getCCW().toStringFull());    
    System.out.println(new Id(Half).getCW().toStringFull());    
    System.out.println(new Id(Null).getCCW().toStringFull());    
    System.out.println(new Id(Null).getCW().toStringFull());    
    System.out.println(new Id(NegOne).getCCW().toStringFull());    
    System.out.println(new Id(NegOne).getCW().toStringFull());    
    System.out.println(new Id(One).getCCW().toStringFull());    
    System.out.println(new Id(One).getCW().toStringFull());    

//    System.out.println(build("ABCD").toStringFull());
//    
//    int[] material = new int[2];
//    material[0] = 9876;
//    material[1] = 1234;
//    System.out.println(new Id(material).toStringFull());
  }

}

