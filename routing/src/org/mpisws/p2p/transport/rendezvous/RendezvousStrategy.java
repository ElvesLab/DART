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
package org.mpisws.p2p.transport.rendezvous;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;

import rice.Continuation;
import rice.p2p.commonapi.Cancellable;
import rice.pastry.socket.nat.rendezvous.RendezvousSocketNodeHandle;

/**
 * Uses a 3rd party channel to request a node to connect to a dest.
 * 
 * @author Jeff Hoye
 *
 */
public interface RendezvousStrategy<Identifier> {
  public static int SUCCESS = 1;

  /**
   * Calls ChannelOpener.openChannel(dest, credentials) on target
   * 
   * Possible exceptions to deliverResultToMe:
   *   NodeIsFaultyException if target is faulty
   *   UnableToConnectException if dest is faulty
   * 
   * Called by:
   *   1) Rendezvous if the target and source are NATted
   *   2) Source if target is NATted, but source isn't
   *   
   *   
   * Not called if the pilotFinder found a pilot for the target (in FreePastry this means that this will not be called
   * if the target is in the leafSet).
   *   
   * @param target call ChannelOpener.openChannel() on this Identifier
   * @param rendezvous pass this to ChannelOpener.openChannel(), it's who the ChannelOpener will connect to
   * @param credentials this is also passed to ChannelOpener.openChannel()
   * @param deliverResultToMe notify me when success/failure
   * @return a way to cancel the request
   */
  public Cancellable openChannel(Identifier target, Identifier rendezvous, Identifier source, int uid, Continuation<Integer, Exception> deliverResultToMe, Map<String, Object> options);
  
  /**
   * Sends the message via an out-of-band channel.  Usually routing.
   * 
   * @param i
   * @param m
   * @param deliverAckToMe
   * @param options
   * @return
   */
  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(Identifier i, ByteBuffer m, MessageCallback<Identifier, ByteBuffer> deliverAckToMe, Map<String, Object> options);

  public void setTransportLayer(RendezvousTransportLayer<Identifier> ret);
}
