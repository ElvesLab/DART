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
package rice.pastry.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;


import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.messaging.Message;
import rice.pastry.socket.TransportLayerNodeHandle;

public class BogusNodeHandle extends TransportLayerNodeHandle<Collection<InetSocketAddress>> {
  public Collection<InetSocketAddress> addresses;
  
  public BogusNodeHandle(InetSocketAddress address) {
    addresses = Collections.singletonList(address);
  }

  public BogusNodeHandle(InetSocketAddress[] bootstraps) {
    addresses = Arrays.asList(bootstraps);
  }

  @Override
  public boolean equals(Object obj) {
    throw new IllegalStateException("This NodeHandle is Bogus, don't use it.");
  }

  @Override
  public int getLiveness() {
    throw new IllegalStateException("This NodeHandle is Bogus, don't use it.");
  }

  @Override
  public Id getNodeId() {
    throw new IllegalStateException("This NodeHandle is Bogus, don't use it.");
  }

  @Override
  public String toString() {
    return "BogusNodeHandle "+addresses;
  }

  
  @Override
  public int hashCode() {
    throw new IllegalStateException("This NodeHandle is Bogus, don't use it.");
  }

  @Override
  public boolean ping() {
    throw new IllegalStateException("This NodeHandle is Bogus, don't use it.");
  }

  @Override
  public int proximity() {
    throw new IllegalStateException("This NodeHandle is Bogus, don't use it.");
  }

  @Override
  public void receiveMessage(Message msg) {
    throw new IllegalStateException("This NodeHandle is Bogus, don't use it.");
  }

  @Override
  public void serialize(OutputBuffer buf) throws IOException {
    throw new IllegalStateException("This NodeHandle is Bogus, don't use it.");
  }

  @Override
  public Collection<InetSocketAddress> getAddress() {
    return addresses;
  }

  @Override
  public long getEpoch() {
    return 0;
  }

}
