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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.manager.simple.NextHopStrategy;

import rice.pastry.NodeHandle;
import rice.pastry.leafset.LeafSet;
import rice.pastry.socket.SocketNodeHandle;

public class RendezvousLeafSetNHStrategy implements NextHopStrategy<MultiInetSocketAddress>{
  LeafSet ls;
  
  public RendezvousLeafSetNHStrategy(LeafSet leafSet) {
    this.ls = leafSet;
  }

//  public void setLeafSet(LeafSet ls) {
//    this.ls = ls;
//  }
  
  public Collection<MultiInetSocketAddress> getNextHops(MultiInetSocketAddress destination) {
    if (ls == null) return null;
    
    Collection<MultiInetSocketAddress> ret = walkLeafSet(destination, 8);
    
    // don't include the direct route
//    ret.remove(destination);
    return ret;
  }
  
  private Collection<MultiInetSocketAddress> walkLeafSet(MultiInetSocketAddress destination, int numRequested) {
    Collection<MultiInetSocketAddress> result = new HashSet<MultiInetSocketAddress>();
    LeafSet leafset = ls;
    for (int i = 1; i < leafset.maxSize()/2; i++) {      
      RendezvousSocketNodeHandle snh = (RendezvousSocketNodeHandle)leafset.get(-i);
      if (snh != null && !snh.eaddress.equals(destination) && snh.canContactDirect()) {
        result.add(snh.eaddress);
        if (result.size() >= numRequested) return result;
      }
      snh = (RendezvousSocketNodeHandle)leafset.get(i);
      if (snh != null && !snh.eaddress.equals(destination) && snh.canContactDirect()) {
        result.add(snh.eaddress);
        if (result.size() >= numRequested) return result;
      }
    }
    return result;
  }
}
