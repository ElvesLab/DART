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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Builds the message to a target size.
 * 
 * @author Jeff Hoye
 */
public class AuthPushMessage<Identifier extends RawSerializable> implements PeerReviewMessage {
    
  public Map<Identifier, List<Authenticator>> authenticators;
  
  public AuthPushMessage(Map<Identifier, List<Authenticator>> authenticators) {
    this.authenticators = authenticators;
  }

  public short getType() {
    return MSG_AUTHPUSH;
  }

  public static <I extends RawSerializable> AuthPushMessage<I> build(InputBuffer buf, Serializer<I> idSerializer, AuthenticatorSerializer authSerializer) throws IOException {
    Map<I, List<Authenticator>> authenticators = new HashMap<I, List<Authenticator>>();
    int numIds = buf.readShort();
    for (int i = 0; i < numIds; i++) {
      I id = idSerializer.deserialize(buf);
      int numAuths = buf.readShort();
      List<Authenticator> l = new ArrayList<Authenticator>();
      authenticators.put(id, l);
      for (int a = 0; a < numAuths; a++) {
        l.add(authSerializer.deserialize(buf));
      }
    }
    return new AuthPushMessage<I>(authenticators);
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeShort((short)authenticators.size());
    for (Entry<Identifier, List<Authenticator>> e : authenticators.entrySet()) {
      e.getKey().serialize(buf);
      buf.writeShort((short)e.getValue().size());
      for (Authenticator a : e.getValue()) {
        a.serialize(buf);
      }
    }
  }  
}
