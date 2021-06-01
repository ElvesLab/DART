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

import org.mpisws.p2p.transport.multiaddress.AddressStrategy;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.rendezvous.ContactDirectStrategy;

import rice.environment.Environment;
import rice.environment.logging.Logger;

public class RendezvousContactDirectStrategy implements ContactDirectStrategy<RendezvousSocketNodeHandle> {
  MultiInetSocketAddress localAddr; 
  Environment environment;
  AddressStrategy addressStrategy;
  Logger logger;
  
  public RendezvousContactDirectStrategy(
      RendezvousSocketNodeHandle localNodeHandle, 
      AddressStrategy addressStrategy, 
      Environment environment) {
    this.localAddr = localNodeHandle.getAddress();
    this.environment = environment;
    this.addressStrategy = addressStrategy;
    this.logger = environment.getLogManager().getLogger(RendezvousContactDirectStrategy.class, null);
  }

  /**
   * Return true if they're behind the same firewall
   * 
   * If the address I should use to contact the node is the same as his internal address
   * 
   */
  public boolean canContactDirect(RendezvousSocketNodeHandle remoteNode) {    
    if (remoteNode.canContactDirect()) return true;
    
    MultiInetSocketAddress a = remoteNode.getAddress();
    if (a.getNumAddresses() == 1) {
      // they're on the same physical node
      return a.getInnermostAddress().getAddress().equals(localAddr.getInnermostAddress().getAddress());
    } else {
      boolean ret = addressStrategy.getAddress(localAddr, a).equals(a.getInnermostAddress());
      if (ret && logger.level <= Logger.FINE) logger.log("rendezvous contacting direct:"+remoteNode); 
      return ret;
    }
  }
}
