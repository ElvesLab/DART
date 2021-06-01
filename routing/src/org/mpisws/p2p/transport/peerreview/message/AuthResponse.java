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

import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;

/**
 *MSG_AUTHRESP
  byte type = MSG_AUTHRESP
  nodeID subject
  authenticator a1           // newest authenticator before timestamp in AUTHREQ
  authenticator a2           // most recent authenticator
 
 * @author Jeff Hoye
 *
 * @param <Identifier>
 */
public class AuthResponse<Identifier extends RawSerializable> implements PeerReviewMessage {
  public Identifier subject;
  public Authenticator authFrom;
  public Authenticator authTo;
  
  public AuthResponse(Identifier subject, Authenticator authFrom, Authenticator authTo) {
    this.subject = subject;
    this.authFrom = authFrom;
    this.authTo = authTo;
  }
  
  public short getType() {
    return MSG_AUTHREQ;
  }

  public AuthResponse(InputBuffer buf, Serializer<Identifier> serializer, int hashSize, int signatureSize) throws IOException {
    subject = serializer.deserialize(buf);
    authFrom = new Authenticator(buf,hashSize,signatureSize);
    authTo = new Authenticator(buf,hashSize,signatureSize);
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    subject.serialize(buf);
    authFrom.serialize(buf);
    authTo.serialize(buf);
  }
}
