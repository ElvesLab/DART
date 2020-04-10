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
package org.mpisws.p2p.transport.peerreview.evidence;

import java.io.IOException;

import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializerImpl;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.EvidenceSerializer;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;

public class EvidenceSerializerImpl<Handle extends RawSerializable, Identifier extends RawSerializable> implements EvidenceSerializer, PeerReviewConstants {
  protected int hashSize;
  protected int signatureSize;
  protected Serializer<Handle> handleSerializer;
  protected Serializer<Identifier> idSerializer;
  protected AuthenticatorSerializerImpl authSerializer;
  
  public EvidenceSerializerImpl(Serializer<Handle> handleSerializer, Serializer<Identifier> idSerializer, int hashSize, int signatureSize) {
    super();
    this.handleSerializer = handleSerializer;
    this.idSerializer = idSerializer;
      this.hashSize = hashSize;
    this.signatureSize = signatureSize;
    this.authSerializer = new AuthenticatorSerializerImpl(hashSize,signatureSize);
  }
  
  public Evidence deserialize(InputBuffer buf, byte type, boolean response) throws IOException {
    switch(type) {
    case CHAL_AUDIT:
      if (response) {
        return new AuditResponse<Handle>(buf,handleSerializer,hashSize);
      } else {
        return new ChallengeAudit(buf, hashSize, signatureSize);
      }
    case CHAL_SEND:
    case (byte)MSG_USERDATA:
      if (response) {
        return AckMessage.build(buf, idSerializer, hashSize, signatureSize);
      } else {
        return UserDataMessage.build(buf, handleSerializer, hashSize, signatureSize);
      }
    case PROOF_INCONSISTENT:
      return new ProofInconsistent(buf, authSerializer, hashSize);
    case PROOF_NONCONFORMANT:
      return new ProofNonconformant<Handle>(buf,handleSerializer,hashSize,signatureSize);
    }      
    throw new IllegalArgumentException("Unknown type:"+type);
  }


}
