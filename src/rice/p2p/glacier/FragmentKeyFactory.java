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
import java.util.*;
import java.util.StringTokenizer;

import rice.environment.random.RandomSource;
import rice.p2p.commonapi.*;
import rice.p2p.multiring.MultiringIdFactory;
import rice.p2p.multiring.RingId;
import rice.pastry.Id;
import java.util.SortedMap;

/**
 * DESCRIBE THE CLASS
 *
 * @version $Id: FragmentKeyFactory.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author ahae
 */
public class FragmentKeyFactory implements IdFactory {

  private MultiringIdFactory FACTORY;

  /**
   * Constructor for FragmentKeyFactory.
   *
   * @param factory DESCRIBE THE PARAMETER
   */
  public FragmentKeyFactory(MultiringIdFactory factory) {
    FACTORY = factory;
  }

  /**
   * DESCRIBE THE METHOD
   *
   * @param material DESCRIBE THE PARAMETER
   * @return DESCRIBE THE RETURN VALUE
   */
  public rice.p2p.commonapi.Id buildId(byte[] material) {
    throw new RuntimeException("FragmentKeyFactory.buildId(byte[]) is not supported!");
  }

  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public rice.p2p.commonapi.Id buildId(int[] material) {
    throw new RuntimeException("FragmentKeyFactory.buildId(byte[]) is not supported!");
  }

  /**
   * Builds a protocol-specific Id by using the hash of the given string as
   * source data.
   *
   * @param string The string to use as source data
   * @return The built Id.
   */
  public rice.p2p.commonapi.Id buildId(String string) {
    throw new RuntimeException("FragmentKeyFactory.buildId(String) is not supported!");
  }

  /**
   * Builds a random protocol-specific Id.
   *
   * @param rng A random number generator
   * @return The built Id.
   */
  public rice.p2p.commonapi.Id buildRandomId(Random rng) {
    return new FragmentKey(new VersionKey(FACTORY.buildRandomId(rng), rng.nextLong()), rng.nextInt());
  }

  public rice.p2p.commonapi.Id buildRandomId(RandomSource rng) {
    return new FragmentKey(new VersionKey(FACTORY.buildRandomId(rng), rng.nextLong()), rng.nextInt());
  }

  /**
   * DESCRIBE THE METHOD
   *
   * @param string DESCRIBE THE PARAMETER
   * @return DESCRIBE THE RETURN VALUE
   */
  public rice.p2p.commonapi.Id buildIdFromToString(String string) {
    StringTokenizer stok = new StringTokenizer(string, "(,) :v#");
    if (stok.countTokens() < 4) {
      return null;
    }

    String keyRingS = stok.nextToken();
    String keyNodeS = stok.nextToken();
    String versionS = stok.nextToken();
    String fragmentIdS = stok.nextToken();
    RingId key = FACTORY.buildRingId(rice.pastry.Id.build(keyRingS), rice.pastry.Id.build(keyNodeS));

    return new FragmentKey(new VersionKey(key, Long.valueOf(versionS).longValue()), Integer.valueOf(fragmentIdS).intValue());
  }

  public rice.p2p.commonapi.Id buildIdFromToString(char[] chars, int offset, int length) {
    return buildIdFromToString(new String(chars, offset, length));
  }

  /**
   * Builds a protocol-specific Id.Distance given the source data.
   *
   * @param material The material to use
   * @return The built Id.Distance.
   */
  public rice.p2p.commonapi.Id.Distance buildIdDistance(byte[] material) {
    throw new RuntimeException("FragmentKeyFactory.buildIdDistance() is not supported!");
  }

  /**
   * Creates an IdRange given the CW and CCW ids.
   *
   * @param cw The clockwise Id
   * @param ccw The counterclockwise Id
   * @return An IdRange with the appropriate delimiters.
   */
  public IdRange buildIdRange(rice.p2p.commonapi.Id cw, rice.p2p.commonapi.Id ccw) {
    throw new RuntimeException("FragmentKeyFactory.buildIdRange() is not supported!");
  }
  
  /**
   * Builds an IdRange based on a prefix.  Any id which has this prefix should
   * be inside this IdRange, and any id which does not share this prefix should
   * be outside it.
   *
   * @param string The toString() representation of an Id
   * @return The built Id.
   */
  public IdRange buildIdRangeFromPrefix(String string) {
    return new FragmentKeyRange(FACTORY.buildIdRangeFromPrefix(string));
  }

  /**
   * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  public IdSet buildIdSet() {
    return new FragmentKeySet();
  }
  
  /**
   * Creates an empty IdSet.
   *
   * @Param map The map which to take the keys from to create the IdSet's elements
   * @return an empty IdSet
   */
  public IdSet buildIdSet(SortedMap map) {
    return new FragmentKeySet(map);
  }

  /**
   * Creates an empty NodeHandleSet.
   *
   * @return an empty NodeHandleSet
   */
  public NodeHandleSet buildNodeHandleSet() {
    throw new RuntimeException("FragmentKeyFactory.buildNodeHandleSet() is not supported!");
  }
  
  public int getIdToStringLength() {
    return FACTORY.getIdToStringLength() + 4;
  }
}
