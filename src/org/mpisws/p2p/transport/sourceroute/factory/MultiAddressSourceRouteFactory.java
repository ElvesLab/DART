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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteFactory;

import rice.p2p.commonapi.rawserialization.InputBuffer;

public class MultiAddressSourceRouteFactory implements SourceRouteFactory<MultiInetSocketAddress> {

  /**
   * 2 in the path is a special case, and we can just generate it from the local and last hops
   */
  public SourceRoute<MultiInetSocketAddress> build(InputBuffer buf, MultiInetSocketAddress local, MultiInetSocketAddress lastHop) throws IOException {
    byte numInPath = buf.readByte();
    if (numInPath == 2) {
      return new MultiAddressSourceRoute(lastHop, local);
    }
    
    ArrayList<MultiInetSocketAddress> path = new ArrayList<MultiInetSocketAddress>(numInPath);
    for (int i = 0; i < numInPath; i++) {
      path.add(MultiInetSocketAddress.build(buf));
    }    
    return new MultiAddressSourceRoute(path);
  }

  public SourceRoute<MultiInetSocketAddress> getSourceRoute(List<MultiInetSocketAddress> route) {
    return new MultiAddressSourceRoute(route);
  }

  public SourceRoute<MultiInetSocketAddress> reverse(SourceRoute<MultiInetSocketAddress> route) {
    MultiAddressSourceRoute temp = (MultiAddressSourceRoute)route;
    ArrayList<MultiInetSocketAddress> result = new ArrayList<MultiInetSocketAddress>(temp.getPath());
    
    Collections.reverse(result);
    
    return new MultiAddressSourceRoute(result);
  }

  public SourceRoute<MultiInetSocketAddress> getSourceRoute(
      MultiInetSocketAddress local, MultiInetSocketAddress dest) {
    return new MultiAddressSourceRoute(local, dest);
  }

  public SourceRoute<MultiInetSocketAddress> getSourceRoute(MultiInetSocketAddress local) {
    return new MultiAddressSourceRoute(local);
  }
}
