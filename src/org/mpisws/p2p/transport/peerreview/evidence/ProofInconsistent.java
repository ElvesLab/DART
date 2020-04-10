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
import org.mpisws.p2p.transport.peerreview.audit.LogSnippet;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * PROOF_INCONSISTENT
 * byte type = PROOF_INCONSISTENT
 * authenticator auth1
 * char whichInconsistency   // 0=another auth, 1=a log snippet
 * authenticator auth2       // if whichInconsistency==0
 * long long firstSeq         // these fields exist only if whichInconsistency==1
 * hash baseHash
 * [entries]
 * 
 * @author Jeff Hoye
 */
public class ProofInconsistent implements PeerReviewConstants, Evidence {
  public static final byte NO_SNIPPET = 0;
  public static final byte LOG_SNIPPET = 1;
  
  
  public Authenticator auth1;
  
  public Authenticator auth2;

  public LogSnippet snippet;

  public ProofInconsistent(Authenticator auth1, Authenticator auth2) {
    this.auth1 = auth1;
    this.auth2 = auth2;
  }
  
  public ProofInconsistent(Authenticator auth1, Authenticator auth2, LogSnippet snippet) {
    this.auth1 = auth1;
    this.auth2 = auth2;
    this.snippet = snippet;
  }
  
  public short getEvidenceType() {
    return PROOF_INCONSISTENT;
  }
  
  public ProofInconsistent(InputBuffer buf, AuthenticatorSerializer serializer, int hashSize) throws IOException {
    auth1 = serializer.deserialize(buf);
    byte type = buf.readByte();
    auth2 = serializer.deserialize(buf);
    switch(type) {
    case NO_SNIPPET:
      break;
    case LOG_SNIPPET:
      snippet = new LogSnippet(buf, hashSize);
      break;
    default:
      throw new IOException("unknown type:"+type);
    }
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    auth1.serialize(buf);
    if (snippet == null) {
      buf.writeByte(NO_SNIPPET);
    } else {
      buf.writeByte(LOG_SNIPPET);
    }
    auth2.serialize(buf);
    if (snippet != null) {
      snippet.serialize(buf);
    }
  }
  
  
}
