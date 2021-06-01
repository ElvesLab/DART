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

package rice.pastry.commonapi;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.random.RandomSource;
import rice.p2p.commonapi.*;

import java.util.*;
import java.security.*;

/**
 * This class provides applications with a way of genertating
 * pastry Ids.
 *
 * @version $Id: PastryIdFactory.java 4232 2008-07-03 12:28:01Z jstewart $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public class PastryIdFactory implements IdFactory {

  private MessageDigest md;

  /**
   * Constructor
   */
  public PastryIdFactory(Environment env) {
    try {
      md = MessageDigest.getInstance("SHA");
    } catch ( NoSuchAlgorithmException e ) {
      Logger logger = env.getLogManager().getLogger(getClass(), null);
      if (logger.level <= Logger.SEVERE) logger.log(
        "No SHA support!" );
    }
  }
      
  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildId(byte[] material) {
    return rice.pastry.Id.build(material);
  }

  /**
   * Builds a protocol-specific Id given the source data.
   *
   * @param material The material to use
   * @return The built Id.
   */
  public Id buildId(int[] material) {
    return rice.pastry.Id.build(material);
  }

  /**
   * Builds a protocol-specific Id by using the hash of the given string as source data.
   *
   * @param string The string to use as source data
   * @return The built Id.
   */
  public Id buildId(String string) {
    synchronized (md) {
      md.update(string.getBytes());
      return buildId(md.digest());
    }
  }
  
  /**
   * Builds a random protocol-specific Id.
   *
   * @param rng A random number generator
   * @return The built Id.
   */
  public rice.p2p.commonapi.Id buildRandomId(Random rng) {
    return rice.pastry.Id.makeRandomId(rng);
  }

  public rice.p2p.commonapi.Id buildRandomId(RandomSource rng) {
    return rice.pastry.Id.makeRandomId(rng);
  }

  /**
   * Builds an Id by converting the given toString() output back to an Id.  Should
   * not normally be used.
   *
   * @param string The toString() representation of an Id
   * @return The built Id.
   */
  public Id buildIdFromToString(String string) {
    return rice.pastry.Id.build(string);
  }
  
  /**
   * Builds an Id by converting the given toString() output back to an Id.  Should
   * not normally be used.
   *
   * @param chars The character array
   * @param offset The offset to start reading at
   * @param length The length to read
   * @return The built Id.
   */
  public Id buildIdFromToString(char[] chars, int offset, int length) {
    return rice.pastry.Id.build(chars, offset, length);
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
    rice.pastry.Id start = rice.pastry.Id.build(string);
    
    rice.pastry.Id end = rice.pastry.Id.build(string + "ffffffffffffffffffffffffffffffffffffffff");
        
    end = end.getCW();
    
    return new rice.pastry.IdRange(start, end);
  }
  
  /**
   * Returns the length a Id.toString should be.
   *
   * @return The correct length;
   */
  public int getIdToStringLength() {
    return rice.pastry.Id.IdBitLength/4;
  }
  
  /**
   * Builds a protocol-specific Id.Distance given the source data.
   *
   * @param material The material to use
   * @return The built Id.Distance.
   */
  public Id.Distance buildIdDistance(byte[] material) {
    return new rice.pastry.Id.Distance(material);
  }

  /**
   * Creates an IdRange given the CW and CCW ids.
   *
   * @param cw The clockwise Id
   * @param ccw The counterclockwise Id
   * @return An IdRange with the appropriate delimiters.
   */
  public IdRange buildIdRange(Id cw, Id ccw) {
    return new rice.pastry.IdRange((rice.pastry.Id) cw, (rice.pastry.Id) ccw);
  }

  /**
   * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  public IdSet buildIdSet() {
    return new rice.pastry.IdSet();
  }
  
  /**
    * Creates an empty IdSet.
   *
   * @return an empty IdSet
   */
  public IdSet buildIdSet(SortedMap map) {
    return new rice.pastry.IdSet(map);
  }
  
  /**
   * Creates an empty NodeHandleSet.
   *
   * @return an empty NodeHandleSet
   */
  public NodeHandleSet buildNodeHandleSet() {
    return new rice.pastry.NodeSet();
  }
}

