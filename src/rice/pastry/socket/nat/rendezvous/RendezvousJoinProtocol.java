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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.rendezvous.ContactDirectStrategy;
import org.mpisws.p2p.transport.rendezvous.PilotManager;
import org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl;
import org.mpisws.p2p.transport.util.OptionsFactory;

import rice.Continuation;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.ReadyStrategy;
import rice.pastry.join.JoinRequest;
import rice.pastry.leafset.LeafSet;
import rice.pastry.messaging.Message;
import rice.pastry.routing.RoutingTable;
import rice.pastry.standard.ConsistentJoinProtocol;

/**
 * The purpose of this class is to allow a NATted node to boot.  
 * 
 * Without this class, when the JoinRequest reaches the nearest neighbor of the joiner, 
 * that node can't deliver the Request back to the joiner (because he is NATted).
 * 
 * The class opens a pilot to the bootstrap, then includes this node in the RendezvousJoinRequest.
 * 
 * Note that this class uses both JoinRequests and RendezvousJoinRequests.  The latter are only used for 
 * a NATted Joiner.
 * 
 * Overview:
 * Extend CJPSerializer to also use RendezvousJoinRequest (include the bootstrap, and possibly additional credentials)
 *   pass in constructor
 *   
 * Override handleInitiateJoin():
 *  If local node is NATted:
 *    open a pilot to the bootstrap (make sure to complete this before continuing)
 *    send the RendezvousJoinRequest
 *  else 
 *    super.handleInitiateJoin()
 *    
 * TODO: 
 * Override respondToJoiner():
 *   If joiner is NATted:
 *    use the pilot on the bootstrap:
 *      rendezvousLayer.requestSocket(joiner, bootstrap)
 *    
 * Override completeJoin() to close the pilot to the bootstrap before calling super.completeJoin() because that will cause pilots to open.
 *    may need a way to verify that it is closed, or else don't close it if it's in the leafset, b/c it may become busted
 *    
 * @author Jeff Hoye
 *
 */
public class RendezvousJoinProtocol extends ConsistentJoinProtocol {

  PilotManager<RendezvousSocketNodeHandle> pilotManager;
  
  public RendezvousJoinProtocol(PastryNode ln, NodeHandle lh, RoutingTable rt,
      LeafSet ls, ReadyStrategy nextReadyStrategy, PilotManager<RendezvousSocketNodeHandle> pilotManager) {
    super(ln, lh, rt, ls, nextReadyStrategy, new RCJPDeserializer(ln));
    this.pilotManager = pilotManager;
  }

  /**
   * Use RendezvousJoinRequest if local node is NATted
   */
  @Override
  protected void getJoinRequest(NodeHandle b, final Continuation<JoinRequest, Exception> deliverJRToMe) {
    final RendezvousSocketNodeHandle bootstrap = (RendezvousSocketNodeHandle)b;
    
    
    // if I can be contacted directly by anyone, or I can contact the bootstrap despite the fact that he's firewalled, then call super
    ContactDirectStrategy<RendezvousSocketNodeHandle> contactStrat = 
      (ContactDirectStrategy<RendezvousSocketNodeHandle>)
      thePastryNode.getVars().get(RendezvousSocketPastryNodeFactory.RENDEZVOUS_CONTACT_DIRECT_STRATEGY);
    if ((((RendezvousSocketNodeHandle)thePastryNode.getLocalHandle()).canContactDirect()) || // I can be contacted directly
         (!bootstrap.canContactDirect() && contactStrat.canContactDirect(bootstrap))) { // I can contact the bootstrap even though he's firewalled
      super.getJoinRequest(bootstrap, deliverJRToMe);
      return;
    }
    
    // TODO: Throw exception if can't directly contact the bootstrap
    
    // open the pilot before sending the JoinRequest.
    if (logger.level <= Logger.FINE) logger.log("opening pilot to "+bootstrap);
    pilotManager.openPilot((RendezvousSocketNodeHandle)bootstrap, 
        new Continuation<SocketRequestHandle<RendezvousSocketNodeHandle>, Exception>(){

      public void receiveException(Exception exception) {
        deliverJRToMe.receiveException(exception);
      }

      public void receiveResult(
          SocketRequestHandle<RendezvousSocketNodeHandle> result) {
        RendezvousJoinRequest jr = new RendezvousJoinRequest(localHandle, thePastryNode
            .getRoutingTable().baseBitLength(), thePastryNode.getEnvironment().getTimeSource().currentTimeMillis(), bootstrap);                
        deliverJRToMe.receiveResult(jr);
      }
    });
  }

  /**
   * This is called from respondToJoiner() and other places, we need to set the OPTION_USE_PILOT
   * to the intermediate node, so that will queue the RendezvousTL to use the pilot.
   * 
   */
  @Override
  protected Map<String, Object> getOptions(JoinRequest jr, Map<String, Object> existing) {
    if (jr.accepted()) {
      if (jr instanceof RendezvousJoinRequest) {
        RendezvousJoinRequest rjr = (RendezvousJoinRequest)jr;
        return OptionsFactory.addOption(existing, RendezvousTransportLayerImpl.OPTION_USE_PILOT, rjr.getPilot());
      }
    }
    return existing;
  }
  
  static class RCJPDeserializer extends CJPDeserializer {
    public RCJPDeserializer(PastryNode pn) {
      super(pn);
    }

    @Override
    public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
      switch(type) {
        case RendezvousJoinRequest.TYPE:
          return new RendezvousJoinRequest(buf,pn, (NodeHandle)sender, pn);
      }      
      return super.deserialize(buf, type, priority, sender);
    }
  }
}
