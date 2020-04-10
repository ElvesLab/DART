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
package org.mpisws.p2p.transport.rendezvous;

import java.util.HashMap;
import java.util.Map;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.time.TimeSource;
import rice.p2p.util.tuples.MutableTuple;

public class EphemeralDBImpl<Identifier, HighIdentifier> implements EphemeralDB<Identifier, HighIdentifier> {
  /**
   * Time where a NAT will have reset the port to a new forwarding
   * 
   * in millis
   * 
   * Default 2 hours
   */
  protected long STALE_PORT_TIME = 2*60*60*1000;
  
  TimeSource time;
  Logger logger;
  
  protected long nextTag = NO_TAG+1;
  protected Map<HighIdentifier, Long> highToTag = new HashMap<HighIdentifier, Long>();
  protected Map<Identifier, Long> ephemeralToTag = new HashMap<Identifier, Long>();
  /**
   * maps tag to ephemeral and timestamp
   */
  protected Map<Long, MutableTuple<Identifier, Long>> tagToEphemeral = new HashMap<Long, MutableTuple<Identifier,Long>>();

  /**
   * 
   * @param env
   * @param stalePortTime how long until we should forget about the port binding
   */
  public EphemeralDBImpl(Environment env, long stalePortTime) {
    time = env.getTimeSource();
    this.logger = env.getLogManager().getLogger(EphemeralDBImpl.class, null);
    this.STALE_PORT_TIME = stalePortTime;
  }
  
  
  /**
   * Tell the DB that the high identifier points to this tag
   * 
   * @param high
   * @param tag
   */
  public void mapHighToTag(HighIdentifier high, long tag) {
    if (logger.level <= Logger.FINE) logger.log("mapHighToTag("+high+","+tag+")");
    highToTag.put(high, tag);
  }

  public Identifier getEphemeral(HighIdentifier high) {
    Long tag = highToTag.get(high);
    if (tag == null) {
      if (logger.level <= Logger.FINE) logger.log("getEphemeral("+high+"):null");
      return null;
    }
    Identifier ret = getEphemeral(tag, null);
    if (ret == null) {
      highToTag.remove(high);
    }
    if (logger.level <= Logger.FINE) logger.log("getEphemeral("+high+"):"+ret);
    return ret;
  }
  
  public Identifier getEphemeral(long tag, Identifier i) {
    MutableTuple<Identifier, Long> ret = tagToEphemeral.get(tag);
    if (ret == null) {
      if (logger.level <= Logger.FINE) logger.log("getEphemeral("+tag+","+i+"):"+i);
      return i;
    }
    if (ret.b() < time.currentTimeMillis()-STALE_PORT_TIME) {
      clear(tag);
      if (logger.level <= Logger.FINE) logger.log("getEphemeral("+tag+","+i+"):"+i);
      return i;
    }
    if (logger.level <= Logger.FINE) logger.log("getEphemeral("+tag+","+i+"):"+ret.a());
    return ret.a();
  }

  public long getTagForEphemeral(Identifier addr) {
    long now = time.currentTimeMillis();
    Long tag = ephemeralToTag.get(addr);
    if (tag == null) {
      // it doesn't exist
    } else {      
      MutableTuple<Identifier, Long> ret = tagToEphemeral.get(tag);      
      if (ret.b() < now-STALE_PORT_TIME) {
        // it's stale
        clear(tag);
      } else {
        // update timestamp
        ret.setB(now);
        if (logger.level <= Logger.FINE) logger.log("getTagForEphemeral("+addr+"):"+tag);
        return tag;
      }
    }

    // if we're here, we need to create a new one
    tag = nextTag++;
    MutableTuple<Identifier, Long> ret = new MutableTuple<Identifier, Long>(addr, now);
    ephemeralToTag.put(addr, tag);
    tagToEphemeral.put(tag,ret);
    if (logger.level <= Logger.FINE) logger.log("getTagForEphemeral("+addr+"):"+tag);
    return tag;
  }

  protected void clear(long tag) {
    if (logger.level <= Logger.FINE) logger.log("clear("+tag+")");
    MutableTuple<Identifier, Long> ret = tagToEphemeral.remove(tag);
    ephemeralToTag.remove(ret.a());
  }
  
}
