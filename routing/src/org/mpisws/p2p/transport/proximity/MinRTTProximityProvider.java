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
package org.mpisws.p2p.transport.proximity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.mpisws.p2p.transport.liveness.PingListener;
import org.mpisws.p2p.transport.liveness.Pinger;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.time.TimeSource;

public class MinRTTProximityProvider<Identifier> implements ProximityProvider<Identifier>, PingListener<Identifier> {
  /**
   * millis for the timeout
   * 
   * The idea is that we don't want this parameter to change too fast, 
   * so this is the timeout for it to increase, you could set this to infinity, 
   * but that may be bad because it doesn't account for intermediate link failures
   */
   public int PROX_TIMEOUT;// = 60*60*1000;

   /**
    * Holds only pending DeadCheckers
    */
   Map<Identifier, EntityManager> managers;

   Pinger<Identifier> tl;
   
   Logger logger;
   
   TimeSource time;
   
   int pingThrottle = 5000; // TODO: Make configurable
   
  public MinRTTProximityProvider(Pinger<Identifier> tl, Environment env) {
    this.tl = tl;
    this.logger = env.getLogManager().getLogger(MinRTTProximityProvider.class, null);
    this.time = env.getTimeSource();
    tl.addPingListener(this);
    this.managers = new HashMap<Identifier, EntityManager>();
  }
  
  public int proximity(Identifier i, Map<String, Object> options) {
    EntityManager manager = getManager(i);
    int ret = manager.proximity;
    if (ret == DEFAULT_PROXIMITY) {
      manager.ping(options);
    }
    return ret;
  }
  
  public void pingResponse(Identifier i, int rtt, Map<String, Object> options) {
    getManager(i).markProximity(rtt, options);
  }

  public void pingReceived(Identifier i, Map<String, Object> options) {

  }
  
  public void clearState(Identifier i) {
    synchronized(managers) {
      managers.remove(i);
    }    
  }
  
  public EntityManager getManager(Identifier i) {
    synchronized(managers) {
      EntityManager manager = managers.get(i);
      if (manager == null) {
        manager = new EntityManager(i);
        if (logger.level <= Logger.FINER) logger.log("Creating EM for "+i);
        managers.put(i,manager);
      }
      return manager;
    }
  }

  /**
   * Internal class which is charges with managing the remote connection via
   * a specific route
   * 
   */
  public class EntityManager {
    
    // the remote route of this manager
    protected Identifier identifier;
    
    // the current best-known proximity of this route
    protected int proximity;
    
    protected long lastPingTime = Integer.MIN_VALUE; // we don't want underflow, but we don't want this to be zero either
    /**
     * Constructor - builds a route manager given the route
     *
     * @param route The route
     */
    public EntityManager(Identifier route) {
      if (route == null) throw new IllegalArgumentException("route is null");
      this.identifier = route;
      proximity = DEFAULT_PROXIMITY;
    }
    
    public void ping(Map<String, Object> options) {
      long now = time.currentTimeMillis();
      if ((now - lastPingTime) < pingThrottle) {
        if  (logger.level <= Logger.FINE) logger.log("Dropping ping because pingThrottle."+(pingThrottle - (now - lastPingTime)));
        return;
      }          
      lastPingTime = now;
      tl.ping(identifier, options);
    }

    /**
     * Method which returns the last cached proximity value for the given address.
     * If there is no cached value, then DEFAULT_PROXIMITY is returned.
     *
     * @param address The address to return the value for
     * @return The ping value to the remote address
     */
    public int proximity() {
      return proximity;
    }
    
    /**
     * This method should be called when this route has its proximity updated
     *
     * @param proximity The proximity
     */
    protected void markProximity(int proximity, Map<String, Object> options) {
      if (proximity < 0) throw new IllegalArgumentException("proximity must be >= 0, was:"+proximity);
      if (logger.level <= Logger.FINER) logger.log(this+".markProximity("+proximity+")");
      if (this.proximity > proximity) {
        if (logger.level <= Logger.FINE) logger.log(this+" updating proximity to "+proximity);
        this.proximity = proximity;
        notifyProximityListeners(identifier, proximity, options);
      }
    }

    public String toString() {
      return identifier.toString();
    }
  }

  Collection<ProximityListener<Identifier>> listeners = new ArrayList<ProximityListener<Identifier>>();
  public void addProximityListener(ProximityListener<Identifier> listener) {
    synchronized(listeners) {
      listeners.add(listener);
    }
  }

  public boolean removeProximityListener(ProximityListener<Identifier> listener) {
    synchronized(listeners) {
      return listeners.remove(listener);
    }
  }
  
  public void notifyProximityListeners(Identifier i, int prox, Map<String, Object> options) {
    Collection<ProximityListener<Identifier>> temp;
    synchronized(listeners) {
      temp = new ArrayList<ProximityListener<Identifier>>(listeners);
    }
    for (ProximityListener<Identifier> p : temp) {
      p.proximityChanged(i, prox, options);
    }
  }
}

