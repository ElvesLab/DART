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
package rice.pastry.socket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.util.Serializer;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.NodeHandleFactoryListener;
import rice.pastry.PastryNode;

public class SocketNodeHandleFactory implements NodeHandleFactory<SocketNodeHandle>, Serializer<SocketNodeHandle> {
  protected PastryNode pn;
  protected Map<SocketNodeHandle, SocketNodeHandle> handleSet;
  protected Collection<NodeHandleFactoryListener<SocketNodeHandle>> listeners = new ArrayList<NodeHandleFactoryListener<SocketNodeHandle>>();
  Logger logger;
  
  public SocketNodeHandleFactory(PastryNode pn) {
    this.pn = pn;
    this.logger = pn.getEnvironment().getLogManager().getLogger(SocketNodeHandleFactory.class, null);
    
    handleSet = new HashMap<SocketNodeHandle, SocketNodeHandle>();
  }
  
  
  /**
   * This is kind of weird, may need to rethink this.  This is called to build one w/o 
   * deserializing anything.  (Either the local node, or one with a bogus id).
   * 
   * @param i
   * @param id
   * @return
   */
  public SocketNodeHandle getNodeHandle(MultiInetSocketAddress i, long epoch, Id id) {
    SocketNodeHandle handle = new SocketNodeHandle(i, epoch, id, pn);
    
    return (SocketNodeHandle)coalesce(handle);
  }

  public SocketNodeHandle readNodeHandle(InputBuffer buf) throws IOException {
    return coalesce(SocketNodeHandle.build(buf, pn));
  }
  
  public SocketNodeHandle coalesce(SocketNodeHandle h) {
    SocketNodeHandle handle = (SocketNodeHandle)h;
    if (handleSet.containsKey(handle)) {
      return handleSet.get(handle);
    }
    
    handle.setLocalNode(pn);
    
    handleSet.put(handle, handle);
    notifyListeners(handle);
    return handle;
  }
  
  /**
   * Notify the listeners that this new handle has come along.
   */
  protected void notifyListeners(SocketNodeHandle nh) {
    Collection<NodeHandleFactoryListener<SocketNodeHandle>> temp = listeners;
    synchronized (listeners) {      
      temp = new ArrayList<NodeHandleFactoryListener<SocketNodeHandle>>(listeners);      
    }
    for (NodeHandleFactoryListener<SocketNodeHandle> foo:temp) {
      foo.nodeHandleFound(nh);
    }
  }


  public void addNodeHandleFactoryListener(
      NodeHandleFactoryListener<SocketNodeHandle> listener) {
    synchronized(listeners) {
      listeners.add(listener);
    }
  }


  public void removeNodeHandleFactoryListener(
      NodeHandleFactoryListener<SocketNodeHandle> listener) {
    synchronized(listeners) {
      listeners.remove(listener);
    }
  }


  public SocketNodeHandle deserialize(InputBuffer buf) throws IOException {
    return readNodeHandle(buf);
  }


  public void serialize(SocketNodeHandle i, OutputBuffer buf)
      throws IOException {
    i.serialize(buf);
  }
}
