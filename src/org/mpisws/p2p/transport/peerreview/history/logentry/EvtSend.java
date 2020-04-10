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
package org.mpisws.p2p.transport.peerreview.history.logentry;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;


/**
 * 
  EVT_SEND
  nodeID receiverID
  bool hashed

  data payload   - or -  relevantPayload, hash

 * @author Jeff Hoye
 *
 * @param <Identifier>
 */
public class EvtSend<Identifier extends RawSerializable> extends HistoryEvent implements PeerReviewConstants {
  public Identifier receiverId;
  public ByteBuffer payload;
  public byte[] hash;
  
  public EvtSend(Identifier receiverId, ByteBuffer payload, int relevantPayload, HashProvider hasher) {
    this.receiverId = receiverId;
    this.payload = ByteBuffer.wrap(payload.array(), payload.position(), relevantPayload);
    hash = hasher.hash(this.payload);
  }
  
  public EvtSend(Identifier receiverId, ByteBuffer payload) {
    this.receiverId = receiverId;
    this.payload = ByteBuffer.wrap(payload.array(), payload.position(), payload.remaining());
  }
  
  public EvtSend(InputBuffer buf, Serializer<Identifier> idSerializer, int hashSize) throws IOException {
    receiverId = idSerializer.deserialize(buf);
    boolean hasHash = buf.readBoolean();    
    byte[] payload_bytes;
    if (hasHash) {
      payload_bytes = new byte[buf.bytesRemaining()-hashSize];
    } else {
      payload_bytes = new byte[hashSize];      
    }
    buf.read(payload_bytes);
    payload = ByteBuffer.wrap(payload_bytes);
    if (hasHash) {
      hash = new byte[hashSize];
      buf.read(hash);   
    }
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    receiverId.serialize(buf);
    buf.writeBoolean(hash != null);
    buf.write(payload.array(), payload.position(), payload.remaining());
    if (hash != null) {
      buf.write(hash, 0, hash.length);      
    }
  }
  
  public short getType() {
    return EVT_SEND;
  }
}
