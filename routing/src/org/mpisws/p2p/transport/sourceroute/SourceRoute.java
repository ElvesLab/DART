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

package org.mpisws.p2p.transport.sourceroute;

import java.io.*;
import java.util.*;

import rice.p2p.commonapi.rawserialization.*;

/**
 * Class which represets a source route to a remote IP address.
 *
 * @version $Id: SourceRoute.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author Alan Mislove
 */
public abstract class SourceRoute<Identifier> {
  
  // Support for coalesced Ids - ensures only one copy of each Id is in memory
//  private static WeakHashMap SOURCE_ROUTE_MAP = new WeakHashMap();
  
  // the default distance, which is used before a ping
  protected List<Identifier> path;

  /**
   * Constructor
   *
   * @param nodeId This node handle's node Id.
   * @param address DESCRIBE THE PARAMETER
   */
  protected SourceRoute(List<Identifier> path) {
    this.path = new ArrayList<Identifier>(path);
    
    // assertion
    for (Identifier i : this.path) {
      if (i == null) throw new IllegalArgumentException("path["+i+"] is null"); 
    }
  }  

  protected SourceRoute(Identifier address) {
    this.path = new ArrayList<Identifier>(1);
    path.add(address);
  }  
  
  protected SourceRoute(Identifier local, Identifier remote) {
    this.path = new ArrayList<Identifier>(2);
    path.add(local);
    path.add(remote);
  }  
  
  /**
   * Method which performs the coalescing and interaction with the weak hash map
   *
   * @param id The Id to coalesce
   * @return The Id to use
   */
//  protected static SourceRoute resolve(WeakHashMap map, SourceRoute route) {
//    synchronized (map) {
//      WeakReference ref = (WeakReference) map.get(route);
//      SourceRoute result = null;
//      
//      if ((ref != null) && ((result = (SourceRoute) ref.get()) != null)) {
//        return result;
//      } else {
//        map.put(route, new WeakReference(route));
//        return route;
//      }
//    }
//  }
  
  /**
   * Constructor.
   *
   * @param path The path of the route
   */
//  public static SourceRoute build(Identifier[] path) {
//    return resolve(SOURCE_ROUTE_MAP, new SourceRoute(path));
//  }
  
  /**
   * Constructor.
   *
   * @param path The path of the route
   */
//  public static SourceRoute build(Identifier address) {
//    return resolve(SOURCE_ROUTE_MAP, new SourceRoute(new Identifier[] {address}));
//  }
  
  /**
   * Define readResolve, which will replace the deserialized object with the canootical
   * one (if one exists) to ensure Id coalescing.
   *
   * @return The real Id
   */
//  private Object readResolve() throws ObjectStreamException {
//    return resolve(SOURCE_ROUTE_MAP, this);
//  }

  /**
   * Returns the hashCode of this source route
   *
   * @return The hashCode
   */
  public int hashCode() {
    int result = 399388937;
    
    for (Identifier i : path)
      result ^= i.hashCode();
    
    return result;
  }
  
  /**
   * Checks equality on source routes
   *
   * @param o The source route to compare to
   * @return The equality
   */
  public boolean equals(Object o) {
    if (o == null)
      return false;
    
    if (!(o instanceof SourceRoute))
      return false;
    
    SourceRoute that = (SourceRoute)o;
    return this.path.equals(that.path);
  }
  
  /**
   * Internal method for computing the toString of an array of InetSocketAddresses
   *
   * @param path The path
   * @return THe string
   */
  public String toString() {
    StringBuffer result = new StringBuffer();
    result.append("{");
    
    for (int i=0; i<path.size(); i++) {
      Identifier thePath = path.get(i);
//      for (int ctr = 0; ctr < thePath.address.length; ctr++) {
//        InetSocketAddress theAddr = thePath.address[ctr];
//        InetAddress theAddr2 = theAddr.getAddress();
//        if (theAddr2 == null) {
//          result.append(theAddr.toString());
//        } else {
//          String ha = theAddr2.getHostAddress();
//          result.append(ha + ":" + theAddr.getPort());
//        }
//        if (ctr < thePath.address.length - 1) result.append(";");
//      } // ctr
      result.append(thePath.toString());
      if (i < path.size() - 1) result.append(" -> ");
    }
    
    result.append("}");
    
    return result.toString();
  }
  
  /**
   * Internal method for computing the toString of an array of InetSocketAddresses
   *
   * @param path The path
   * @return THe string
   */
//  public String toStringFull() {
//    StringBuffer result = new StringBuffer();
//    result.append("{");
//    
//    for (int i=0; i<path.length; i++) {
//      result.append(path[i].toString());
//      if (i < path.length - 1) result.append(" -> ");
//    }
//    
//    result.append("}");
//    
//    return result.toString();
//  }
  
  /**
   * Method which revereses path and cliams the corresponding address
   *
   * @param path The path to reverse
   * @param address The address to claim
   */
//  public SourceRoute reverse(Identifier localAddress) {
//    Identifier[] result = new Identifier[path.length];
//    
//    for (int i=0; i<path.length-1; i++)
//      result[i] = path[path.length-2-i];
//    
//    result[result.length-1] = localAddress;
//    
//    return SourceRoute.build(result);
//  }
  
  /**
   * Method which revereses path
   *
   */
//  public SourceRoute<Identifier> reverse() {
//    ArrayList<Identifier> result = new ArrayList<Identifier>(path);
//    
//    Collections.reverse(result);
//    
//    return new SourceRoute<Identifier>(result);
//  }
  
  /**
   * Method which returns the first "hop" of this source route
   *
   * @return The first hop of this source route
   */
  public Identifier getFirstHop() {
    return path.get(0);
  }
  
  /**
   * Method which returns the first "hop" of this source route
   *
   * @return The first hop of this source route
   */
  public Identifier getLastHop() {
    return path.get(path.size()-1);
  }
  
  /**
   * Returns the number of hops in this source route
   *
   * @return The number of hops 
   */
  public int getNumHops() {
    return path.size();
  }
  
  /**
   * Returns the hop at the given index
   *
   * @param i The hop index
   * @return The hop
   */
  public Identifier getHop(int i) {
    return path.get(i);
  }
  
  /**
   * Returns whether or not this route is direct
   *
   * @return whether or not this route is direct
   */
  public boolean isDirect() {
    return (path.size() <= 2);
  }
  
  /**
   * Returns whether or not this route goes through the given address
   *
   * @return whether or not this route goes through the given address
   */
  public boolean goesThrough(Identifier address) {
    return path.contains(address);
  }
  
  /**
   * Internal method which returns an array representing the source
   * route
   *
   * @return An array represetning the route
   */
//  public InetSocketAddress[] toArray() {
//    InetSocketAddress[] result = new InetSocketAddress[path.length];
//    
//    for (int i=0; i<result.length; i++)
//      result[i] = path[i].getAddress();
//    
//    return result;
//  }
  
  /**
   * Method which creates a new source route by removing the last hop of this one
   */
//  public SourceRoute removeLastHop() {
//    Identifier[] result = new Identifier[path.length-1];
//    System.arraycopy(path, 0, result, 0, result.length);
//    
//    return SourceRoute.build(result);
//  }
  
  /**
   * Method which creates a new source route by appending the given address
   * to the end of this one
   *
   * @param address The address to append
   */
//  public SourceRoute append(Identifier address) {
//    Identifier[] result = new Identifier[path.length+1];
//    System.arraycopy(path, 0, result, 0, path.length);
//    result[result.length-1] = address;
//    
//    return SourceRoute.build(result);
//  }
  
  /**
   * Method which creates a new source route by appending the given address
   * to the end of this one
   *
   * @param address The address to append
   */
//  public SourceRoute prepend(Identifier address) {
//    Identifier[] result = new Identifier[path.length+1];
//    System.arraycopy(path, 0, result, 1, path.length);
//    result[0] = address;
//    
//    return SourceRoute.build(result);
//  }

  /**
   * Returns which hop in the path the identifier is.  
   * @param identifier the hop
   * @return -1 if not in the path
   */
  public int getHop(Identifier identifier) {
    for(int i = 0; i < path.size(); i++) {
      Identifier id = path.get(i);
      if (id.equals(identifier)) return i;
    }
    return -1;
  }

  
  /***************** Raw Serialization ***************************************/  
  abstract public void serialize(OutputBuffer buf) throws IOException;
  abstract public int getSerializedLength();
  
  
//  public void serialize(OutputBuffer buf) throws IOException {
//    buf.writeByte((byte)path.size());
//    for (Identifier i : path) {
//      i.serialize(buf);
//    }
//  }
  
//  public static SourceRoute build(InputBuffer buf) throws IOException {
////    byte version = buf.readByte();
////    switch(version) {
////      case 0:
//        byte numInPath = buf.readByte();
//        Identifier[] path = new Identifier[numInPath];
//        for (int i = 0; i < numInPath; i++) {
//          path[i] = Identifier.build(buf);
//        }    
//        return new SourceRoute(path);
////      default:
////        throw new IOException("Unknown Version: "+version);
////    }     
//  }

  
//  public int getSerializedLength() {
//    int ret = 5; // version+numhops
//    
//    // the size of all the EISAs
//    for (Identifier i : path) {
//      ret += i.getSerializedLength();
//    }    
//    return ret;
//  }
}


