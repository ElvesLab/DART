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
package rice.pastry.socket.nat.rendezvous;

import org.mpisws.p2p.transport.rendezvous.OutgoingPilotListener;
import org.mpisws.p2p.transport.rendezvous.PilotManager;
import org.mpisws.p2p.transport.rendezvous.RendezvousContact;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.pastry.NodeHandle;
import rice.pastry.NodeSetEventSource;
import rice.pastry.NodeSetListener;
import rice.pastry.leafset.LeafSet;

/**
 * Notifies the pilot strategy of leafset changes involving non-natted nodes.
 * 
 * Only instantiate this on NATted nodes.
 * 
 * @author Jeff Hoye
 *
 */
public class LeafSetPilotStrategy<Identifier extends RendezvousContact> implements NodeSetListener, OutgoingPilotListener<Identifier> {
  LeafSet leafSet;
  PilotManager<Identifier> manager;
  Logger logger;
  
  public LeafSetPilotStrategy(LeafSet leafSet, PilotManager<Identifier> manager, Environment env) {
    this.leafSet = leafSet;
    this.manager = manager;
    this.manager.addOutgoingPilotListener(this);
    this.logger = env.getLogManager().getLogger(LeafSetPilotStrategy.class, null);
    
    leafSet.addNodeSetListener(this);
  }

  public void nodeSetUpdate(NodeSetEventSource nodeSetEventSource, NodeHandle handle, boolean added) {
    if (logger.level <= Logger.FINER) logger.log("nodeSetUpdate("+handle+")");
//    if (logger.level <= Logger.FINE) logger.log("nodeSetUpdate("+handle+")");
    Identifier nh = (Identifier)handle;
    if (nh.canContactDirect()) {
      if (added) {
        manager.openPilot(nh, null);
      } else {
        manager.closePilot(nh);        
      }
    }
  }

  public void pilotOpening(Identifier i) {
    // TODO Auto-generated method stub
    
  }

  public void pilotClosed(Identifier i) {
    if (leafSet.contains((NodeHandle)i)) {
      manager.openPilot(i, null);
    }
  }
  
}
