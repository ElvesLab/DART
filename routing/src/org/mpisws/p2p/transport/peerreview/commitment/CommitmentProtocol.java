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
package org.mpisws.p2p.transport.peerreview.commitment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.peerreview.infostore.StatusChangeListener;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;

import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.tuples.Tuple;

/**
 * This protocol attaches signatures to outgoing messages and acknowledges
 * incoming messages. It also has transmit and receive queues where messages can
 * be held while acknowledgments are pending, and it can retransmit messages a
 * few times when an acknowledgment is not received.
 */
public interface CommitmentProtocol<Handle extends RawSerializable, Identifier extends RawSerializable> extends StatusChangeListener<Identifier> {
//  int lookupPeer(Identifier handle);
//  PacketInfo *enqueueTail(struct packetInfo *queue, unsigned char *message, int msglen);
//  void makeProgress(int idx);
//  int findRecvEntry(Identifier *id, long long seq);
//  long long findAckEntry(Identifier *id, long long seq);
//  void initReceiveCache();
//  void addToReceiveCache(Identifier *id, long long senderSeq, int indexInLocalHistory);
  public MessageRequestHandle<Handle, ByteBuffer> handleOutgoingMessage(
      Handle target, ByteBuffer message, 
      MessageCallback<Handle, ByteBuffer> deliverAckToMe, 
      Map<String, Object> options);
  public void handleIncomingAck(Handle source, AckMessage<Identifier> ackMessage, Map<String, Object> options) throws IOException;
  public void handleIncomingMessage(Handle source, UserDataMessage<Handle> msg, Map<String, Object> options) throws IOException;
  public void notifyCertificateAvailable(Identifier id);
  public Tuple<AckMessage<Identifier>,Boolean> logMessageIfNew(UserDataMessage<Handle> udm);
  public void setTimeToleranceMillis(long timeToleranceMicros);

}
