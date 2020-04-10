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

import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.pastry.*;

import java.io.IOException;
import java.util.*;

/**
 * NodeIdUnit tests the NodeId class.
 * 
 * @version $Id: NodeIdUnit.java 3613 2007-02-15 14:45:14Z jstewart $
 * 
 * @author Andrew Ladd
 */

public class NodeIdUnit {
  private Id nid;

  private RandomSource rng;

  public Id createNodeId() {
    byte raw[] = new byte[Id.IdBitLength >> 3];

    rng.nextBytes(raw);

    Id nodeId = Id.build(raw);

    System.out.println("created node " + nodeId);

    byte copy[] = new byte[raw.length];

    nodeId.blit(copy);

    for (int i = 0; i < raw.length; i++)
      if (copy[i] != raw[i])
        System.out.println("blit test failed!");

    copy = nodeId.copy();

    for (int i = 0; i < raw.length; i++)
      if (copy[i] != raw[i])
        System.out.println("copy test failed!");

    return nodeId;
  }

  public void equalityTest() {
    System.out.println("--------------------------");
    System.out.println("Creating oth");
    Id oth = createNodeId();

    if (nid.equals(oth) == false)
      System.out.println("not equal - as expected.");
    else
      System.out
          .println("ALERT: equal - warning this happens with very low probability");

    if (nid.equals(nid) == true)
      System.out.println("equality seems reflexive.");
    else
      System.out.println("ALERT: equality is not reflexive.");

    System.out.println("hash code of nid: " + nid.hashCode());
    System.out.println("hash code of oth: " + oth.hashCode());
    System.out.println("--------------------------");
  }

  public void distanceTest() {
    System.out.println("--------------------------");

    System.out.println("creating a and b respectively");

    Id a = createNodeId();
    Id b = createNodeId();

    for (int i = 0; i < 100; i++) {
      Id.Distance adist = nid.distance(a);
      Id.Distance adist2 = a.distance(nid);
      Id.Distance bdist = nid.distance(b);
      System.out.println("adist =" + adist + "\n bdist=" + bdist);

      if (adist.equals(adist2) == true)
        System.out.println("distance seems reflexive");
      else
        System.out.println("ALERT: distance is non-reflexive.");

      if (adist.equals(bdist) == true)
        System.out
            .println("ALERT: nodes seem at equal distance - very unlikely");
      else
        System.out.println("nodes have different distance as expected.");

      System.out.println("result of comparison with a and b "
          + adist.compareTo(bdist));
      System.out.println("result of comparison with a to itself "
          + adist.compareTo(adist2));

      if (a.clockwise(b))
        System.out.println("b is clockwise from a");
      else
        System.out.println("b is counter-clockwise from a");

      Id.Distance abs = a.distance(b);
      Id.Distance abl = a.longDistance(b);
      if (abs.compareTo(abl) != -1)
        System.out.println("ERROR: abs.compareTo(abl)=" + abs.compareTo(abl));

      System.out.println("abs=" + abs);
      abs.shift(-1, 1);
      System.out.println("abs.shift(-1)=" + abs);
      abs.shift(1, 0);
      System.out.println("abs.shift(1)=" + abs);
      if (!abs.equals(a.distance(b)))
        System.out.println("SHIFT ERROR!");

      a = createNodeId();
      b = createNodeId();
    }

    byte[] raw0 = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    byte[] raw80 = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -128 };
    byte[] raw7f = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, 127 };

    byte[] t1 = { 0x62, (byte) 0xac, 0x0a, 0x6d, 0x26, 0x3a, (byte) 0xeb,
        (byte) 0xb1, (byte) 0xe4, (byte) 0x8e, 0x25, (byte) 0xf2, (byte) 0xe5,
        0x0e, (byte) 0xa2, 0x13 };
    byte[] t2 = { 0x3a, 0x3f, (byte) 0xfa, (byte) 0x82, 0x00, (byte) 0x91,
        (byte) 0xfb, (byte) 0x82, (byte) 0x9d, (byte) 0xd2, (byte) 0xd8, 0x42,
        (byte) 0x86, 0x40, 0x5d, (byte) 0xd7 };

    a = Id.build(t1/* raw80 */);
    b = Id.build(t2/* raw7f */);
    Id n0 = Id.build(raw0);
    Id n7f = Id.build(raw7f);
    Id n80 = Id.build(raw80);
    Id c = n0;

    System.out.println("a=" + a + "b=" + b + "c=" + c);
    System.out.println("a.clockwise(b)=" + a.clockwise(b));
    System.out.println("a.clockwise(a)=" + a.clockwise(a));
    System.out.println("b.clockwise(b)=" + b.clockwise(b));

    if (a.clockwise(c))
      System.out.println("c is clockwise from a");
    else
      System.out.println("c is counter-clockwise from a");

    if (b.clockwise(c))
      System.out.println("c is clockwise from b");
    else
      System.out.println("c is counter-clockwise from b");

    System.out.println("a.distance(b)" + a.distance(b) + "b.distance(a)="
        + b.distance(a));
    System.out.println("a.longDistance(b)" + a.longDistance(b)
        + "b.longDistance(a)=" + b.longDistance(a));
    System.out.println("a.distance(a)" + a.distance(a) + "a.longDistance(a)="
        + a.longDistance(a));

    System.out.println("a.isBetween(a,n7f)=" + a.isBetween(a, n7f));
    System.out.println("a.isBetween(n0,a)=" + a.isBetween(n0, a));

    System.out.println("a.isBetween(n0,n7f)=" + a.isBetween(n0, n7f));
    System.out.println("b.isBetween(n0,n80)=" + b.isBetween(n0, n80));
    System.out.println("a.isBetween(a,n80)=" + a.isBetween(a, n80));
    System.out.println("b.isBetween(n0,b)=" + b.isBetween(n0, b));

    System.out.println("--------------------------");
  }

  public void baseFiddlingTest() {
    System.out.println("--------------------------");

    String bitRep = "";
    for (int i = 0; i < Id.IdBitLength; i++) {
      if (nid.checkBit(i) == true)
        bitRep = bitRep + "1";
      else
        bitRep = bitRep + "0";
    }

    System.out.println(bitRep);

    String digRep = "";
    for (int i = 0; i < Id.IdBitLength; i++) {
      digRep = digRep + nid.getDigit(i, 1);
    }

    System.out.println(digRep);

    if (bitRep.equals(digRep) == true)
      System.out.println("strings the same - as expected");
    else
      System.out.println("ALERT: strings differ - this is wrong.");

    System.out.println("--------------------------");
  }

  public void msdTest() {
    System.out.println("--------------------------");

    System.out.println("creating a and b respectively");

    Id a = createNodeId();
    Id b = createNodeId();

    Id.Distance adist = nid.distance(a);
    Id.Distance bdist = nid.distance(b);
    Id.Distance aldist = nid.longDistance(a);
    Id.Distance bldist = nid.longDistance(b);

    System.out.println("nid.dist(a)=" + adist);
    System.out.println("nid.longDist(a)=" + aldist);
    System.out.println("nid.dist(b)=" + bdist);
    System.out.println("nid.longDist(b)=" + bldist);

    System.out.println("adist.compareTo(bdist) " + adist.compareTo(bdist));
    System.out.println("aldist.compareTo(bldist) " + aldist.compareTo(bldist));

    System.out.println("msdb a and nid " + nid.indexOfMSDB(a));
    System.out.println("msdb b and nid " + nid.indexOfMSDB(b));

    if (nid.indexOfMSDB(a) == a.indexOfMSDB(nid))
      System.out.println("msdb is symmetric");
    else
      System.out.println("ALERT: msdb is not symmetric");

    for (int i = 2; i <= 6; i++) {
      int msdd;

      System.out.println("msdd a and nid (base " + (1 << i) + ") "
          + (msdd = nid.indexOfMSDD(a, i)) + " val=" + a.getDigit(msdd, i)
          + "," + nid.getDigit(msdd, i));
      System.out.println("msdd b and nid (base " + (1 << i) + ") "
          + (msdd = nid.indexOfMSDD(b, i)) + " val=" + b.getDigit(msdd, i)
          + "," + nid.getDigit(msdd, i));
    }

    System.out.println("--------------------------");
  }

  public void alternateTest() {
    System.out.println("--------------------------");

    System.out.println("nid=" + nid);

    for (int b = 2; b < 7; b++)
      for (int num = 2; num <= (1 << b); num *= 2)
        for (int i = 1; i < num; i++)
          System.out.println("alternate (b=" + b + ") " + i + ":"
              + nid.getAlternateId(num, b, i));

    System.out.println("--------------------------");
  }

  public void domainPrefixTest() {
    System.out.println("--------------------------");

    System.out.println("nid=" + nid);

    for (int b = 2; b < 7; b++)
      for (int row = nid.IdBitLength / b - 1; row >= 0; row--)
        for (int col = 0; col < (1 << b); col++) {
          Id domainFirst = nid.getDomainPrefix(row, col, 0, b);
          Id domainLast = nid.getDomainPrefix(row, col, -1, b);
          System.out.println("prefixes " + nid + domainFirst + domainLast);
          int cmp = domainFirst.compareTo(domainLast);
          boolean equal = domainFirst.equals(domainLast);
          if ((cmp == 0) != equal)
            System.out.println("ERROR, compareTo=" + cmp + " equal=" + equal);
          if (cmp == 1)
            System.out.println("ERROR, compareTo=" + cmp);
        }

    System.out.println("--------------------------");
  }

  public NodeIdUnit() {
    Environment env = new Environment();
    rng = env.getRandomSource();

    System.out.println("Creating nid");
    nid = createNodeId();

    equalityTest();
    distanceTest();
    baseFiddlingTest();
    msdTest();
    alternateTest();
    domainPrefixTest();
  }

  public static void main(String args[]) {
    NodeIdUnit niu = new NodeIdUnit();
  }
}
