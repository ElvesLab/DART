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
/**
 * 
 */
package org.mpisws.p2p.transport.sourceroute.manager.simple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.manager.SourceRouteStrategy;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.util.TimerWeakHashMap;

/**
 * This is a simple implementation of the SourceRouteStrategy.  It caches 
 * routes to destinations but relies on a NextHopeStrategy to provide 
 * new nodes that can be used to source route through.  If we already 
 * source-route to get to them, we simply prepend the route with that one.
 * 
 * @author Jeff Hoye
 *
 */
public class SimpleSourceRouteStrategy<Identifier> implements SourceRouteStrategy<Identifier> {
  /**
   * Destination -> route
   * 
   * The order of the list is from here to the end.  The last hop is always the destination.
   * The local node is implied and is not included.  Direct routes are also stored here.
   */
  TimerWeakHashMap<Identifier, SourceRoute>routes;
  NextHopStrategy<Identifier> strategy;
  Environment environment;
  Logger logger;
  LivenessProvider<SourceRoute> livenessManager;
  SourceRouteFactory<Identifier> srFactory;
  Identifier localAddress;
  
  public SimpleSourceRouteStrategy(
      Identifier localAddress,
      SourceRouteFactory<Identifier> srFactory, 
      NextHopStrategy<Identifier> strategy, 
      Environment env) {
    this.localAddress = localAddress;
    this.srFactory = srFactory;
    this.strategy = strategy;
    this.environment = env;
    this.logger = environment.getLogManager().getLogger(SimpleSourceRouteStrategy.class, null);
    routes = new TimerWeakHashMap<Identifier, SourceRoute>(environment.getSelectorManager(),300000);
  }

  /**
   * Note, this implementation only allows 1 - hop routes, need to check the liveness, of a route
   * to determine longer routes.  In most cases a 1-hop route should be sufficient.
   */
  public Collection<SourceRoute<Identifier>> getSourceRoutes(Identifier destination) {
    Collection<Identifier> nextHops = strategy.getNextHops(destination);
    List<SourceRoute<Identifier>> ret = new ArrayList<SourceRoute<Identifier>>(nextHops.size());
    for (Identifier intermediate : nextHops) {
      if (!intermediate.equals(destination)) {
        List<Identifier> hopList = new ArrayList<Identifier>(3);
        
        hopList.add(localAddress);
        hopList.add(intermediate);
        hopList.add(destination);
        SourceRoute<Identifier> route = srFactory.getSourceRoute(hopList);
        ret.add(route);
      }
    }
    return ret;
  }
  
  
  
  /**
   * Produces a route to the destination.  A direct route if there is not 
   * a cached multi-hop route.
   * 
   * @param dest
   */
  private SourceRoute<Identifier> getRoute(Identifier intermediate, Identifier dest) {
    SourceRoute route = routes.get(dest);
    if (route == null) {
      route = srFactory.getSourceRoute(localAddress,dest);
      routes.put(dest, route);
    }    
    return route;
  }
  
}
