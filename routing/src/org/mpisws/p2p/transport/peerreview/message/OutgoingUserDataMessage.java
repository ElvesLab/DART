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
package org.mpisws.p2p.transport.peerreview.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.peerreview.commitment.PeerInfo;

import rice.p2p.commonapi.rawserialization.RawSerializable;

public class OutgoingUserDataMessage<Handle extends RawSerializable> extends UserDataMessage<Handle> implements MessageRequestHandle<Handle, ByteBuffer> {
  PeerInfo<Handle> pi;
  MessageCallback<Handle, ByteBuffer> deliverAckToMe;
  Map<String, Object> options;
  
  public OutgoingUserDataMessage(long topSeq, Handle senderHandle,
      byte[] topMinusOne, byte[] sig, ByteBuffer message, int relevantlen,
      Map<String, Object> options, PeerInfo<Handle> pi, MessageCallback<Handle, ByteBuffer> deliverAckToMe) {
    super(topSeq, senderHandle, topMinusOne, sig, message, relevantlen);
    this.options = options;
    this.pi = pi;
    this.deliverAckToMe = deliverAckToMe;
  }

  public Handle getIdentifier() {
    return pi.getHandle();
  }

  public ByteBuffer getMessage() {
    return getPayload();
  }

  public Map<String, Object> getOptions() {
    return options;
  }

  public boolean cancel() {
    // implementing this is going to be a bit tricky
    // make a cancelled bit here, and then check it before sending, 
    // and when receive the ack, don't call notify
    return false; //pi.getXmitQueue().remove(this);
  }

  public void sendComplete() {
    if (deliverAckToMe != null) deliverAckToMe.ack(this);
  }

  public void sendFailed(IOException ioe) {
    if (deliverAckToMe != null) deliverAckToMe.sendFailed(this, ioe);
  }
}
