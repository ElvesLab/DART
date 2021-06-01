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
package rice.pastry.peerreview;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import rice.environment.Environment;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.transport.NodeHandleAdapter;
import rice.pastry.transport.TLDeserializer;

public class CallbackFactory extends SocketPastryNodeFactory {
  
  public Map<PastryNode, NodeHandle> localHandleTable;
  
  public CallbackFactory(Environment env)
      throws IOException {
    super(null,0, env);
    localHandleTable = new HashMap<PastryNode, NodeHandle>();
  }

  
  @Override
  public NodeHandleAdapter getNodeHandleAdapter(
      PastryNode pn, 
      NodeHandleFactory handleFactory2, 
      TLDeserializer deserializer) throws IOException {
    return null;
  }
  
  @Override
  public NodeHandle getLocalHandle(PastryNode pn, NodeHandleFactory nhf) {
    NodeHandle ret = localHandleTable.remove(pn);
    if (ret == null) throw new IllegalStateException("Couldn't find the handle in localHandleTable:"+pn);
    return ret;
  }

}
