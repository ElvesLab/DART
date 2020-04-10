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
import java.util.Map;

import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.EvidenceSerializer;
import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;

/**
 * MSG_CHALLENGE
  byte type = MSG_CHALLENGE
  nodeID originator
  long long evidenceSeq
  byte chalType = {CHAL_AUDIT|CHAL_SEND}
  [challenge payload follows]

 * @author Jeff Hoye
 *
 */
public class ChallengeMessage<Identifier extends RawSerializable> implements PeerReviewMessage {
  public static short TYPE = MSG_CHALLENGE;
    
  public Identifier originator;
  public long evidenceSeq;
  public Evidence challenge;
  
  public ChallengeMessage(Identifier originator, long evidenceSeq, Evidence challenge) {
    this.originator = originator;
    this.evidenceSeq = evidenceSeq;
    this.challenge = challenge;
  }

  public short getType() {
    return TYPE;
  }
  
  public short getChallengeType() {
    return challenge.getEvidenceType();
  }

  public ChallengeMessage(InputBuffer buf, Serializer<Identifier> idSerializer, EvidenceSerializer evidenceSerializer) throws IOException {
    originator = idSerializer.deserialize(buf);
    evidenceSeq = buf.readLong();
    byte chalType = buf.readByte();
    challenge = evidenceSerializer.deserialize(buf,chalType,false);
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    originator.serialize(buf);
    buf.writeLong(evidenceSeq);
    buf.writeByte((byte)challenge.getEvidenceType());
    challenge.serialize(buf);
  }

  public Evidence getChallenge() {
    return challenge;
  }

}
