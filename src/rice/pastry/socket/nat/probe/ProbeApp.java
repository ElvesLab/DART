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
package rice.pastry.socket.nat.probe;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.mpisws.p2p.transport.multiaddress.AddressStrategy;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.networkinfo.ProbeStrategy;
import org.mpisws.p2p.transport.networkinfo.Prober;

import rice.Continuation;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.client.PastryAppl;
import rice.pastry.messaging.Message;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.nat.rendezvous.RendezvousSocketNodeHandle;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;

public class ProbeApp extends PastryAppl implements ProbeStrategy {
  Prober prober;
  AddressStrategy addressStrategy;
  
  public ProbeApp(PastryNode pn, Prober prober, AddressStrategy addressStrategy) {
    super(pn, null, 0, null);
    this.prober = prober;
    this.addressStrategy = addressStrategy;
    
    setDeserializer(new MessageDeserializer() {
      public rice.p2p.commonapi.Message deserialize(InputBuffer buf, short type,
          int priority, rice.p2p.commonapi.NodeHandle sender) throws IOException {
        switch(type) {
        case ProbeRequestMessage.TYPE:
          return ProbeRequestMessage.build(buf, getAddress());
        default:
          throw new IllegalArgumentException("Unknown type: "+type);    
        }
      }    
    });
  }

  @Override
  public void messageForAppl(Message msg) {
    ProbeRequestMessage prm = (ProbeRequestMessage)msg;
    handleProbeRequestMessage(prm);
  }
  
  public void handleProbeRequestMessage(ProbeRequestMessage prm) {
    if (logger.level <= Logger.FINE) logger.log("handleProbeRequestMessage("+prm+")");
    prober.probe(
        addressStrategy.getAddress(((SocketNodeHandle)thePastryNode.getLocalHandle()).getAddress(), 
            prm.getProbeRequester()), 
            prm.getUID(), null, null);
  }

  /**
   * Send a ProbeRequestMessage to a node in the leafset.  
   * 
   * The node must not have the same external address as addr.  
   * If no such candidate can be found, use someone who does.
   * If there are no candidates at all, send the message to self (or call handleProbeRequest()
   */
  public Cancellable requestProbe(MultiInetSocketAddress addr, long uid, final Continuation<Boolean, Exception> deliverResultToMe) {
    if (logger.level <= Logger.FINE) logger.log("requestProbe("+addr+","+uid+","+deliverResultToMe+")");
    // Step 1: find valid helpers
    
    // make a list of valid candidates
    ArrayList<NodeHandle> valid = new ArrayList<NodeHandle>();
    
    Iterator<NodeHandle> candidates = thePastryNode.getLeafSet().iterator();
    while(candidates.hasNext()) {
      SocketNodeHandle nh = (SocketNodeHandle)candidates.next();
      // don't pick self
      if (!nh.equals(thePastryNode.getLocalHandle())) {
        // if nh will send to addr's outermost address
        if (addressStrategy.getAddress(nh.getAddress(), addr).equals(addr.getOutermostAddress())) {
          valid.add(nh);
        }
      }
    }
    
    // if there are no valid nodes, use the other nodes
    if (valid.isEmpty()) {
      deliverResultToMe.receiveResult(false);
      return null;
//      if (logger.level <= Logger.WARNING) logger.log("requestProbe("+addr+","+uid+") found nobody to help verify connectivity, doing it by my self");
//      valid.add(thePastryNode.getLocalHandle());
    }
    
    // Step 2: choose one randomly
    NodeHandle handle = valid.get(thePastryNode.getEnvironment().getRandomSource().nextInt(valid.size()));

    // Step 3: send the probeRequest
    ProbeRequestMessage prm = new ProbeRequestMessage(addr, uid, getAddress());
    return thePastryNode.send(handle, prm, new PMessageNotification() {    
      public void sent(PMessageReceipt msg) {
        deliverResultToMe.receiveResult(true);
      }    
      public void sendFailed(PMessageReceipt msg, Exception reason) {
        deliverResultToMe.receiveResult(false);
      }    
    }, null);
  }

  public Collection<InetSocketAddress> getExternalAddresses() {
    ArrayList<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();
    Iterator<NodeHandle> i = thePastryNode.getLeafSet().iterator();
    while(i.hasNext()) {
      NodeHandle nh = i.next();      
      RendezvousSocketNodeHandle rsnh = (RendezvousSocketNodeHandle)nh;
      if (rsnh.canContactDirect()) {
        ret.add(rsnh.getInetSocketAddress());
      }      
    }

    return ret;
  }
}
