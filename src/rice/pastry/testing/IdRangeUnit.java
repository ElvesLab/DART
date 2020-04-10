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
package rice.pastry.testing;

import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.pastry.*;
import java.util.*;

/**
 * IdRangeUnit tests the IdRange class.
 * 
 * @version $Id: IdRangeUnit.java 3613 2007-02-15 14:45:14Z jstewart $
 * 
 * @author Peter Druschel
 */

public class IdRangeUnit {
  private RandomSource rng;

  public IdRange createRandomIdRange() {
    IdRange r;
    r = new IdRange(Id.makeRandomId(rng), Id.makeRandomId(rng));
    return r;
  }
  
  public IdRange createFullIdRange() {
    IdRange r;
    Id id = Id.makeRandomId(rng);
    r = new IdRange(id, id);
    return r;
  }

  public IdRange createEmptyIdRange() {
    IdRange r;
    r = new IdRange();
    return r;
  }

  public IdRange createIdRangeStartingWith(Id ccw) {
    IdRange r;
    r = new IdRange(ccw, Id.makeRandomId(rng));
    return r;
  }

  public IdRange createIdRangeEndingIn(Id cw) {
    IdRange r;
    r = new IdRange(Id.makeRandomId(rng), cw);
    return r;
  }

  public Id createIdWithPrefix(byte b) {
    byte[] data = new byte[Id.IdBitLength / 8];

    data[data.length - 1] = b;
    return Id.build(data);
  }
  
  public void halfTest() {
    IdRange r = createFullIdRange();
    IdRange ccw = r.ccwHalf();
    IdRange cw = r.cwHalf();
    
    if (!cw.getCW().distance(cw.getCCW()).equals(cw.getCW().longDistance(cw.getCCW()))) {
      System.out.println("ALERT: cw long distance not equal");
    }
    
    if (!ccw.getCW().distance(ccw.getCCW()).equals(ccw.getCW().longDistance(ccw.getCCW()))) {
      System.out.println("ALERT: ccw long distance not equal");
    }
  }

  public void equalityTest(IdRange r1, IdRange r2) {

    if (r1.equals(r2) && !r1.isEmpty() && !r1.isFull())
      System.out.println("ALERT: equality failed with high probability" + r1
          + r2);

    if (!r1.equals(r1))
      System.out.println("ALERT: equality is not reflexive." + r1);

    IdRange r3 = new IdRange(r2.getCCW(), r2.getCW());
    IdRange r31 = new IdRange(r3);

    if ((!r3.equals(r2) && !r2.isEmpty()) || !r3.equals(r31))
      System.out.println("ALERT: equality failed." + r2 + r3 + r31);

    //if (r2.hashCode() != r3.hashCode() && !r2.isEmpty())
    // System.out.println("ALERT: hashCode failed." + r2 + r3);

    IdRange r4 = new IdRange();
    IdRange r5 = new IdRange(r1.getCCW(), r1.getCCW());

    if (r4.equals(r5))
      System.out.println("ALERT: equality failed" + r4 + r4);
    if (!r4.isEmpty() || r5.isEmpty())
      System.out.println("ALERT: isEmpty failed" + r4 + r5);

  }

  public void mergeIntersectTest(IdRange r1, IdRange r2) {

    IdRange m1 = r1.merge(r2);
    IdRange m2 = r2.merge(r1);
    IdRange i1 = r1.intersect(r2);

    if (!m1.equals(m2) && (!i1.isEmpty() || r1.isAdjacent(r2)) && !r1.isEmpty())
      System.out.println("ALERT: merge is not symmetric 1" + r1 + r2 + m1 + m2);

    boolean intersect = !i1.isEmpty();
    boolean adjacent = r1.isAdjacent(r2);
    boolean intersectOrAdjacent = intersect || adjacent;

    IdRange i2 = r2.intersect(r1);

    if (i1.isEmpty() != i2.isEmpty())
      System.out.println("ALERT: intersect error 1." + i1 + i2);

    if (intersectOrAdjacent) {

      if (m1.equals(r1) && !i1.isEmpty() && !i1.equals(r2))
        System.out.println("ALERT: merge is not symmetric 2" + r1 + r2 + m1
            + i1);

      if (m1.equals(r2) && !i1.isEmpty() && !i1.equals(r1))
        System.out.println("ALERT: merge is not symmetric 3" + r1 + r2 + m1
            + i1);

      IdRange re1 = m1.intersect(r1);
      IdRange re2 = m1.intersect(r2);

      if (!r1.equals(re1) || !r2.equals(re2))
        System.out.println("ALERT: intersect error 2." + r1 + re1 + r2 + re2
            + m1);

    } else {
      if (!m1.equals(r1) && !r1.isEmpty())
        System.out.println("ALERT: merge error 0." + r1 + r2 + m1);
    }

    if ((r1.isEmpty() || r2.isEmpty()) && !i1.isEmpty())
      System.out.println("ALERT: intersect error 3." + r1 + r2 + i1);

    if (r1.isEmpty() && !m1.equals(r2))
      System.out.println("ALERT: merge error 1." + r1 + r2 + m1);
    if (r2.isEmpty() && !m1.equals(r1))
      System.out.println("ALERT: merge error 2." + r1 + r2 + m1);

    if ((r1.isFull() || r2.isFull()) && !m1.isFull())
      System.out.println("ALERT: merge error 3." + r1 + r2 + m1);

    if (r1.isFull() && !i1.equals(r2))
      System.out.println("ALERT: intersect error 4." + r1 + r2 + i1);
    if (r2.isFull() && !i1.equals(r1))
      System.out.println("ALERT: intersect error 5." + r1 + r2 + m1);

  }

  public void diffSubtractTest(IdRange r1, IdRange r2) {

    IdRange d1 = r1.diff(r2);
    IdRange d2 = r2.diff(r1);

    IdRange i1 = r1.intersect(r2);
    IdRange i2 = r2.intersect(r1);

    if (!d1.intersect(i1).isEmpty() || !d1.intersect(i2).isEmpty()
        || !d2.intersect(i1).isEmpty() || !d2.intersect(i2).isEmpty())
      System.out.println("ALERT: diff error 1." + r1 + r2 + d1 + d2 + i1 + i2);

    if (!i1.equals(i2)) {
      if (d1.equals(d2)) {
        System.out.println("ALERT: diff error 15." + r1 + r2 + d1 + d2 + i1
            + i2);
      }
    }

    // try to reconstitute
    IdRange re1 = d1;
    IdRange re2 = d2;
    for (int i = 0; i < 3; i++) {
      re1 = re1.merge(d1);
      re1 = re1.merge(i1);
      re1 = re1.merge(d2);
      re1 = re1.merge(i2);
      re2 = re2.merge(d1);
      re2 = re2.merge(i1);
      re2 = re2.merge(d2);
      re2 = re2.merge(i2);
    }
    IdRange r12 = r1.merge(r2);

    if ((!re1.equals(r1) && !re1.equals(r2) && !re1.equals(r12))
        || (!re2.equals(r1) && !re2.equals(r2) && !re2.equals(r12)))
      System.out.println("ALERT: diff error 2." + r1 + r2 + d1 + d2 + i1 + i2
          + re1 + re2);

    d1 = r1.subtract(r2, false);
    d2 = r1.subtract(r2, true);

    if (!d1.intersect(i1).isEmpty() || !d1.intersect(i2).isEmpty()
        || !d2.intersect(i1).isEmpty() || !d2.intersect(i2).isEmpty())
      System.out.println("ALERT: subtract error 1." + r1 + r2 + d1 + d2 + i1
          + i2);

    if ((!d1.isEmpty() && d1.intersect(r1).isEmpty())
        || (!d2.isEmpty() && d2.intersect(r1).isEmpty()))
      System.out.println("ALERT: subtract error 2." + r1 + r2 + d1 + d2);

    // try to reconstitute
    re1 = d1;
    re2 = d2;
    for (int i = 0; i < 3; i++) {
      re1 = re1.merge(d1);
      re1 = re1.merge(i1);
      re1 = re1.merge(d2);
      re1 = re1.merge(i2);
      re2 = re2.merge(d1);
      re2 = re2.merge(i1);
      re2 = re2.merge(d2);
      re2 = re2.merge(i2);
    }
    r12 = r1.merge(r2);

    if (!re1.equals(r1) && !re2.equals(r1))
      System.out.println("ALERT: subtract error 25." + r1 + r2 + d1 + d2 + i1
          + i2 + re1 + re2);

    d1 = r2.subtract(r1, false);
    d2 = r2.subtract(r1, true);

    if (!d1.intersect(i1).isEmpty() || !d1.intersect(i2).isEmpty()
        || !d2.intersect(i1).isEmpty() || !d2.intersect(i2).isEmpty())
      System.out.println("ALERT: subtract error 3." + r1 + r2 + d1 + d2 + i1
          + i2);

    if ((!d1.isEmpty() && d1.intersect(r2).isEmpty())
        || (!d2.isEmpty() && d2.intersect(r2).isEmpty()))
      System.out.println("ALERT: subtract error 4." + r1 + r2 + d1 + d2);

    // try to reconstitute
    re1 = d1;
    re2 = d2;
    for (int i = 0; i < 3; i++) {
      re1 = re1.merge(d1);
      re1 = re1.merge(i1);
      re1 = re1.merge(d2);
      re1 = re1.merge(i2);
      re2 = re2.merge(d1);
      re2 = re2.merge(i1);
      re2 = re2.merge(d2);
      re2 = re2.merge(i2);
    }
    r12 = r1.merge(r2);

    if (!re1.equals(r2) && !re2.equals(r2))
      System.out.println("ALERT: subtract error 5." + r1 + r2 + d1 + d2 + i1
          + i2 + re1 + re2);

  }

  public void mergeTest(IdRange r1, IdRange r2) {

    IdRange ccw = r1.ccwHalf();
    IdRange cw = r1.cwHalf();

    if (!ccw.intersect(cw).isEmpty())
      System.out.println("ALERT: ccw and cw halves intersect." + r1 + ccw + cw);
    if (!r1.equals(ccw.merge(cw)))
      System.out.println("ALERT: merge cw/ccw failure." + r1 + ccw + cw);

    ccw = r2.ccwHalf();
    cw = r2.cwHalf();

    if (!ccw.intersect(cw).isEmpty())
      System.out.println("ALERT: ccw and cw halves intersect." + r2 + ccw + cw);
    if (!r2.equals(ccw.merge(cw)))
      System.out.println("ALERT: merge cw/ccw failure." + r2 + ccw + cw);

  }

  public IdRangeUnit() {
    rng = new SimpleRandomSource(null);

    System.out.println("IdRangeUnit test starting...");

    halfTest();
    
    for (int i = 0; i < 1000; i++) {
      IdRange r1 = createEmptyIdRange();
      IdRange r2 = createEmptyIdRange();

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createFullIdRange();

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createRandomIdRange();

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      //
      r1 = createFullIdRange();

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createFullIdRange();

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createEmptyIdRange();

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      //
      r1 = createRandomIdRange();

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeStartingWith(r1.getCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeStartingWith(r1.getCCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeEndingIn(r1.getCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeEndingIn(r1.getCCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = r1.complement();

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      //
      r1 = createFullIdRange();

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeStartingWith(r1.getCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeStartingWith(r1.getCCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeEndingIn(r1.getCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeEndingIn(r1.getCCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      // 
      r1 = createEmptyIdRange();

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeStartingWith(r1.getCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeStartingWith(r1.getCCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeEndingIn(r1.getCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r2 = createIdRangeEndingIn(r1.getCCW());

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);

      r1 = new IdRange(createIdWithPrefix((byte) 0xC0),
          createIdWithPrefix((byte) 0x40));
      r2 = new IdRange(createIdWithPrefix((byte) 0xE0),
          createIdWithPrefix((byte) 0x20));

      equalityTest(r1, r2);
      mergeIntersectTest(r1, r2);
      diffSubtractTest(r1, r2);
      mergeTest(r1, r2);
    }

    System.out.println("IdRangeUnit test finished.");

  }

  public static void main(String args[]) {
    IdRangeUnit niu = new IdRangeUnit();
  }
}

