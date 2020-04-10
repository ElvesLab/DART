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

import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;

/**
  EVT_ACK
  nodeID remoteID
  long long ackedSeq
  long long hisSeq
  hash hTopMinusOne
  signature sig
 * @author Jeff Hoye
 *
 * @param <Identifier>
 */
public class EvtAck<Identifier extends RawSerializable> extends HistoryEvent {
  Identifier remoteId;
  long ackedSeq;
  long hisSeq;
  byte[] hTopMinusOne;
  byte[] signature;

  public EvtAck(Identifier remoteId, long ackedSeq, long hisSeq,
      byte[] topMinusOne, byte[] signature) {
    super();
    this.remoteId = remoteId;
    this.ackedSeq = ackedSeq;
    this.hisSeq = hisSeq;
    hTopMinusOne = topMinusOne;
    this.signature = signature;
  }
  
  public EvtAck(InputBuffer buf, Serializer<Identifier> idSerializer, int hashSize, int signatureSize) throws IOException {
    remoteId = idSerializer.deserialize(buf);
    ackedSeq = buf.readLong();
    hisSeq = buf.readLong();
    hTopMinusOne = new byte[hashSize];
    buf.read(hTopMinusOne);
    signature = new byte[signatureSize];
    buf.read(signature);
  }

  public short getType() {
    return EVT_SENDSIGN;
  }

  public void serialize(OutputBuffer buf) throws IOException {
    remoteId.serialize(buf);
    buf.writeLong(ackedSeq);
    buf.writeLong(hisSeq);
    buf.write(hTopMinusOne,0,hTopMinusOne.length);
    buf.write(signature,0,signature.length);
  }

  public Identifier getRemoteId() {
    return remoteId;
  }

  public long getAckedSeq() {
    return ackedSeq;
  }

  public long getHisSeq() {
    return hisSeq;
  }

  public byte[] getHTopMinusOne() {
    return hTopMinusOne;
  }

  public byte[] getSignature() {
    return signature;
  }
}
