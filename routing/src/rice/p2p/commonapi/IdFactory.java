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

package rice.p2p.commonapi;

import java.util.*;

import rice.environment.random.RandomSource;

/**
 * @(#) IdFactory.java This interface provides applications with a way of generating Ids in a
 * protocol-specific manner.
 *
 * @version $Id: IdFactory.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author Alan Mislove
 * @author Peter Druschel
 */
public interface IdFactory {

  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildId(byte[] material);

  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildId(int[] material);

  /**
   * Builds a protocol-specific Id by using the hash of the given string as source data.
   *
   * @param string The string to use as source data
   * @return The built Id.
   */
  public Id buildId(String string);

  /**
   * Builds a random protocol-specific Id.
   *
   * @param rng A random number generator
   * @return The built Id.
   */
  public Id buildRandomId(Random rng);
  public Id buildRandomId(RandomSource rng);
  
  /**
   * Builds an Id by converting the given toString() output back to an Id.  Should
   * not normally be used.
   *
   * @param string The toString() representation of an Id
   * @return The built Id.
   */
  public Id buildIdFromToString(String string);
  
  /**
   * Builds an Id by converting the given toString() output back to an Id.  Should
   * not normally be used.
   *
   * @param chars The character array
   * @param offset The offset to start reading at
   * @param length The length to read
   * @return The built Id.
   */
  public Id buildIdFromToString(char[] chars, int offset, int length);
  
  /**
   * Builds an IdRange based on a prefix.  Any id which has this prefix should
   * be inside this IdRange, and any id which does not share this prefix should
   * be outside it.
   *
   * @param string The toString() representation of an Id
   * @return The built Id.
   */
  public IdRange buildIdRangeFromPrefix(String string);
  
  /**
   * Returns the length a Id.toString should be.
   *
   * @return The correct length;
   */
  public int getIdToStringLength();

  /**
   * Builds a protocol-specific Id.Distance given the source data.
   *
   * @param material The material to use
   * @return The built Id.Distance.
   */
  public Id.Distance buildIdDistance(byte[] material);

  /**
   * Creates an IdRange given the CW and CCW ids.
   *
   * @param cw The clockwise Id
   * @param ccw The counterclockwise Id
   * @return An IdRange with the appropriate delimiters.
   */
  public IdRange buildIdRange(Id cw, Id ccw);
  
  /**
   * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  public IdSet buildIdSet();
  
  /**
   * Creates an empty IdSet.
   *
   * @Param map The map which to take the keys from to create the IdSet's elements
   * @return an empty IdSet
   */
  public IdSet buildIdSet(SortedMap map);
  
  /**
   * Creates an empty NodeHandleSet.
   *
   * @return an empty NodeHandleSet
   */
  public NodeHandleSet buildNodeHandleSet();
}

