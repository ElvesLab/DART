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

import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;

/**
  MSG_ACK
  byte type = MSG_ACK
  nodeID recipientID
  long long sendEntrySeq
  long long recvEntrySeq
  hash hashTopMinusOne
  signature sig
 * 
 * @author Jeff Hoye
 *
 * @param <Identifier>
 */
public class AckMessage<Identifier extends RawSerializable> implements PeerReviewMessage, Evidence {

  Identifier nodeId;
  long sendEntrySeq;
  long recvEntrySeq;
  byte[] hashTopMinusOne;
  byte[] signature;
  
  public AckMessage(Identifier nodeId, long sendEntrySeq, long recvEntrySeq,
      byte[] hashTopMinusOne, byte[] signature) {
    this.nodeId = nodeId;
    this.sendEntrySeq = sendEntrySeq;
    this.recvEntrySeq = recvEntrySeq;
    this.hashTopMinusOne = hashTopMinusOne;
    this.signature = signature;
  }
  
  public short getType() {
    return MSG_ACK;
  }
  
  public short getEvidenceType() {
    return RESP_SEND;
  }

  public void serialize(OutputBuffer buf) throws IOException {
    nodeId.serialize(buf);
    buf.writeLong(sendEntrySeq);
    buf.writeLong(recvEntrySeq);
    buf.write(hashTopMinusOne,0,hashTopMinusOne.length);
    buf.write(signature,0,signature.length);
  }
  
  public static <Identifier extends RawSerializable> AckMessage<Identifier> build(InputBuffer sib, Serializer<Identifier> serializer, int hashSizeInBytes, int signatureSizeInBytes) throws IOException {
    Identifier remoteId = serializer.deserialize(sib);
    long ackedSeq = sib.readLong();
    long hisSeq = sib.readLong();    
    byte[] hTopMinusOne = new byte[hashSizeInBytes];
    sib.read(hTopMinusOne);
    byte[] signature = new byte[signatureSizeInBytes];
    sib.read(signature);
    return new AckMessage<Identifier>(remoteId, ackedSeq, hisSeq, hTopMinusOne, signature);
  }

  public Identifier getNodeId() {
    return nodeId;
  }

  public long getSendEntrySeq() {
    return sendEntrySeq;
  }

  public long getRecvEntrySeq() {
    return recvEntrySeq;
  }

  public byte[] getHashTopMinusOne() {
    return hashTopMinusOne;
  }

  public byte[] getSignature() {
    return signature;
  }
}
