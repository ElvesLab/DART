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
package org.mpisws.p2p.transport.peerreview.statement;

import java.io.IOException;

import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.EvidenceSerializer;
import org.mpisws.p2p.transport.peerreview.message.PeerReviewMessage;
import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;

public abstract class Statement<Identifier extends RawSerializable> implements PeerReviewMessage {
  public Identifier originator;
  public Identifier subject;
  public long evidenceSeq;
  public Evidence evidence;

  public Statement(Identifier originator, Identifier subject,
      long evidenceSeq, Evidence evidence) {
    this.originator = originator;
    this.subject = subject;
    this.evidenceSeq = evidenceSeq;
    this.evidence = evidence;
  }

  public Statement(InputBuffer buf, Serializer<Identifier> idSerializer, EvidenceSerializer evSerializer) throws IOException {
    originator = idSerializer.deserialize(buf);
    subject = idSerializer.deserialize(buf);
    evidenceSeq = buf.readLong();
    evidence = evSerializer.deserialize(buf, buf.readByte(),isResponse());
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    originator.serialize(buf);
    subject.serialize(buf);
    buf.writeLong(evidenceSeq);
    buf.writeByte((byte)evidence.getEvidenceType());
    evidence.serialize(buf);
  }
  
  protected abstract boolean isResponse();
}
