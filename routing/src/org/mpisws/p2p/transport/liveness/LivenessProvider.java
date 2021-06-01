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
package org.mpisws.p2p.transport.liveness;

import java.util.Map;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;

/**
 * Expands the Transport Layer to include pings and liveness checks.
 * 
 * @author Jeff Hoye
 *
 */
public interface LivenessProvider<Identifier> extends LivenessTypes {
  
  public int getLiveness(Identifier i, Map<String, Object> options);
  
  /**
   * Returns whether a new notification will occur.
   * 
   * Will return false if a liveness check has recently completed.
   * 
   * Will return true if a new liveness check starts, or an existing one is in progress.
   * 
   * @param i the node to check
   * @return true if there will be an update (either a ping, or a change in liveness)
   * false if there won't be an update due to bandwidth concerns
   */
  public boolean checkLiveness(Identifier i, Map<String, Object> options);
  
  public void addLivenessListener(LivenessListener<Identifier> name);
  public boolean removeLivenessListener(LivenessListener<Identifier> name);
  
  /**
   * Force layer to clear the existing state related to the Identifier.  Usually 
   * if there is reason to believe a node has returned.
   * 
   * @param i
   */
  public void clearState(Identifier i);
}
