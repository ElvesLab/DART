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
package org.mpisws.p2p.transport.sourceroute.factory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteFactory;

import rice.p2p.commonapi.rawserialization.InputBuffer;

/**
 * TODO: may be a good idea to special case the single hop, as is done in multi-inet.
 * @author Jeff Hoye
 *
 */
public class WireSourceRouteFactory implements SourceRouteFactory<InetSocketAddress> {

  public SourceRoute<InetSocketAddress> build(InputBuffer buf, InetSocketAddress localAddr, InetSocketAddress lastHop) throws IOException {
    byte numInPath = buf.readByte();
    ArrayList<InetSocketAddress> path = new ArrayList<InetSocketAddress>(numInPath);
    for (int i = 0; i < numInPath; i++) {
      byte[] addrBytes = new byte[4];
      buf.read(addrBytes);
      InetAddress addr = InetAddress.getByAddress(addrBytes);
      short port = buf.readShort();

      path.add(new InetSocketAddress(addr, 0xFFFF & port));
    }    
    return new WireSourceRoute(path);
  }

  public SourceRoute<InetSocketAddress> getSourceRoute(List<InetSocketAddress> route) {
    return new WireSourceRoute(route);
  }

  public SourceRoute<InetSocketAddress> reverse(SourceRoute<InetSocketAddress> route) {
    WireSourceRoute temp = (WireSourceRoute)route;
    ArrayList<InetSocketAddress> result = new ArrayList<InetSocketAddress>(temp.getPath());
    
    Collections.reverse(result);
    
    return new WireSourceRoute(result);
  }

  public SourceRoute<InetSocketAddress> getSourceRoute(
      InetSocketAddress local, InetSocketAddress dest) {
    return new WireSourceRoute(local, dest);
  }

  public SourceRoute<InetSocketAddress> getSourceRoute(InetSocketAddress local) {
    return new WireSourceRoute(local);
  }
}
