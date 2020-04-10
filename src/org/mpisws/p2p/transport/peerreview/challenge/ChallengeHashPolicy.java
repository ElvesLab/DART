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
package org.mpisws.p2p.transport.peerreview.challenge;

import java.io.IOException;

import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.history.HashPolicy;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSend;
import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.util.rawserialization.SimpleInputBuffer;

public class ChallengeHashPolicy<Identifier> implements HashPolicy, PeerReviewConstants {
  byte flags;
  Identifier originator;
  boolean includeNextCheckpoint;
  boolean includeNextSendSign;
  private Serializer<Identifier> idSerializer;

  public ChallengeHashPolicy(byte flags, Identifier originator,
      Serializer<Identifier> idSerializer) {
    this.includeNextCheckpoint = ((flags & FLAG_INCLUDE_CHECKPOINT) == FLAG_INCLUDE_CHECKPOINT);
    this.flags = flags;
    this.idSerializer = idSerializer;
    
    /* If FLAG_FULL_MESSAGES_SENDER is set, we must not hash SEND entries, and we must include
    SENDSIGN entries if they follow a SEND to the specified node */
 
    if ((flags & FLAG_FULL_MESSAGES_SENDER) == FLAG_FULL_MESSAGES_SENDER) {
      assert(originator != null);
      this.originator = originator;
    } else {
      this.originator = null;
    }
  }

  public boolean hashEntry(short type, byte[] content) {
    switch (type) { 
    case EVT_CHECKPOINT : /* We include at most one checkpoint, and only if FLAG_INCLUDE_CHECKPOINT is set */
      if (includeNextCheckpoint) {
        includeNextCheckpoint = false;
        return false;
      }
      return true;
    case EVT_SEND : /* We include SEND entries only if FLAG_FULL_MESSAGES_{SENDER|ALL} is set */
      if ((flags & FLAG_FULL_MESSAGES_ALL) == FLAG_FULL_MESSAGES_ALL)
        return false;
      
      
      if (originator != null) {
        try {
          Identifier otherOriginator = idSerializer.deserialize(new SimpleInputBuffer(content));  
          if (otherOriginator.equals(originator)) { 
            includeNextSendSign = true;
          }
        } catch (IOException ioe) {
          // it's fine, it just didn't deserialize properly
          ioe.printStackTrace();
        }
        return false;
      }
      return true;
    case EVT_SENDSIGN : /* We only include SENDSIGN entries if they go to the specified target node */
      if ((flags & FLAG_FULL_MESSAGES_ALL) == FLAG_FULL_MESSAGES_ALL)
        return false;

      if (includeNextSendSign) {
        includeNextSendSign = false;
        return false;
      }
      return true;
    default :
      break;
    }  
    return false;
  }

}
