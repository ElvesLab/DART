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
package org.mpisws.p2p.transport.liveness;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessTransportLayerImpl.EntityManager;
import org.mpisws.p2p.transport.liveness.LivenessTransportLayerImpl.LSocket;

import rice.environment.Environment;

/**
 * Cancels liveness check if you read/write to TCP or read UDP from the node.
 * 
 * @author Jeff Hoye
 *
 * @param <Identifier>
 */
public class AggressiveLivenessTransportLayerImpl<Identifier> extends
    LivenessTransportLayerImpl<Identifier> {

  public AggressiveLivenessTransportLayerImpl(
      TransportLayer<Identifier, ByteBuffer> tl, Environment env,
      ErrorHandler<Identifier> errorHandler, int checkDeadThrottle) {
    super(tl, env, errorHandler, checkDeadThrottle);
  }

  public P2PSocket<Identifier> getLSocket(P2PSocket<Identifier> s, EntityManager manager) {
    ALSocket sock = new ALSocket(manager, s, manager.identifier.get());
    synchronized(manager.sockets) {
      manager.sockets.add(sock);
    }
    return sock;
  }
  
  class ALSocket extends LSocket {

    public ALSocket(EntityManager manager, P2PSocket<Identifier> socket,
        Identifier hardRef) {
      super(manager, socket, hardRef);
    }
    
    public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
      super.receiveSelectResult(socket, canRead, canWrite);
      cancelLivenessCheck(manager, socket.getOptions());
    }
  }
  
  @Override
  public void messageReceived(Identifier i, ByteBuffer m,
      Map<String, Object> options) throws IOException {
    super.messageReceived(i, m, options);
    cancelLivenessCheck(i, options);
  }
}
