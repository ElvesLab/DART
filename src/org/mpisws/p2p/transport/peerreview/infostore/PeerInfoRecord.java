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
package org.mpisws.p2p.transport.peerreview.infostore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.util.FileOutputBuffer;

/**
 * This is just an index to the real evidence which is on disk
 * 
 * @author Jeff Hoye
 *
 * @param <Handle>
 * @param <Identifier>
 */
public class PeerInfoRecord<Handle, Identifier> implements PeerReviewConstants {
  private Identifier subject;
  private Authenticator lastCheckedAuth;
  private int status;
  private PeerInfoStore<Handle, Identifier> store;
  
  /**
   * answeredEvidence contains challenges that have been responded to
   * unansweredEvidence contains proofs and unAnswered evidence
   * 
   * Originator -> TimeStamp -> Record
   */
  Map<Identifier, Map<Long, EvidenceRecordImpl>> answeredEvidence;
  Map<Identifier, Map<Long, EvidenceRecordImpl>> unansweredEvidence;

  public PeerInfoRecord(Identifier id, PeerInfoStore<Handle, Identifier> store) {
    this.subject = id;
    this.store = store;

    unansweredEvidence = new HashMap<Identifier, Map<Long, EvidenceRecordImpl>>();
    answeredEvidence = new HashMap<Identifier, Map<Long, EvidenceRecordImpl>>();
    status = STATUS_TRUSTED;
  }
  
  /* Locates evidence, or creates a new entry if 'create' is set to true */

  public EvidenceRecordImpl findEvidence(Identifier originator, long timestamp, boolean create) {    
    // first check answeredEvidence, then check unansweredEvidence, and add it there if necessary    
    Map<Long,EvidenceRecordImpl> foo = answeredEvidence.get(originator);
    if (foo != null) {
      EvidenceRecordImpl bar = foo.get(timestamp);
      if (bar != null) {
        return bar;
      }
    }
    
    foo = unansweredEvidence.get(originator);
    if (foo == null) {
      if (create) {
        foo = new HashMap<Long, EvidenceRecordImpl>();
        unansweredEvidence.put(originator,foo);
        EvidenceRecordImpl bar = new EvidenceRecordImpl(originator,timestamp);
        foo.put(timestamp, bar);
        return bar;
      }
      return null;
    } else {
      EvidenceRecordImpl bar = foo.get(timestamp);
      if (bar == null && create) {
        bar = new EvidenceRecordImpl(originator,timestamp);
      }
      return bar;
    }    
  }

  
  public class EvidenceRecordImpl implements EvidenceRecord<Handle, Identifier> {
    public Identifier originator;
    public long timestamp;
    Handle interestedParty;
    boolean isProof;

    public EvidenceRecordImpl(Identifier originator, long timestamp) {
      this(originator,timestamp, false, null);
    }

    public EvidenceRecordImpl(Identifier originator, long timestamp, boolean isProof, Handle interestedParty) {
      this.originator = originator;
      this.timestamp = timestamp;
      this.isProof = isProof;
      this.interestedParty = interestedParty;
    }
    
    public boolean hasResponse() {      
      Map<Long, EvidenceRecordImpl> foo = answeredEvidence.get(originator);
      if (foo == null) return false;
      return foo.containsKey(timestamp);
    }

    public void setIsProof(boolean isProof) {
      this.isProof = isProof;
      /* This may cause the node to become SUSPECTED or EXPOSED */ 
      if (isProof && (status != STATUS_EXPOSED)) {
        status = STATUS_EXPOSED;
        store.notifyStatusChanged(subject, STATUS_EXPOSED);
      } else if (!isProof && (status == STATUS_TRUSTED)) {
        status = STATUS_SUSPECTED;
        store.notifyStatusChanged(subject, STATUS_SUSPECTED);
      }
    }

    public void setInterestedParty(Handle interestedParty) {
      this.interestedParty = interestedParty;
    }
    
    public void setHasResponse() {
      assert(!isProof());
      
      // pull from unanswered (if it's there)
      Map<Long, EvidenceRecordImpl> foo = unansweredEvidence.get(originator);
      if (foo != null) {
        // remove this from 
        foo.remove(timestamp);
        if (foo.isEmpty()) {
          unansweredEvidence.remove(originator);
        }
      }
      
      // put into answered
      foo = answeredEvidence.get(originator);
      if (foo == null) {
        foo = new HashMap<Long, EvidenceRecordImpl>();
        answeredEvidence.put(originator, foo);
      }
      foo.put(timestamp, this);
      
      /* If this was the last unanswered challenge to a SUSPECTED node, it goes back to TRUSTED */      
      if ((status == STATUS_SUSPECTED) && unansweredEvidence.isEmpty()) {
        status = STATUS_TRUSTED;
        store.notifyStatusChanged(subject, STATUS_TRUSTED);
      }    
    }

    public boolean isProof() {
      return isProof;
    }
    
    public long getTimeStamp() {
      return timestamp;
    }
    
    public Identifier getOriginator() {
      return originator;
    }

    public Handle getInterestedParty() {
      return interestedParty;
    }
  }


  public int getStatus() {
    return status;
  }

  public Authenticator getLastCheckedAuth() {
    return lastCheckedAuth;
  }

  public void setLastCheckedAuth(Authenticator auth, File dir, IdStrTranslator<Identifier> translator) throws IOException {
    FileOutputBuffer buf = new FileOutputBuffer(new File(dir,translator.toString(subject)+".info"));
    auth.serialize(buf);
    buf.close();
    lastCheckedAuth = auth;    
  }

  public EvidenceRecord<Handle, Identifier> getFirstUnansweredChallenge() {
    return getFirstUnansweredChallenge(false);
  }
  
  /**
   * 
   * @param proof only return proofs
   * @return
   */
  protected EvidenceRecord<Handle, Identifier> getFirstUnansweredChallenge(boolean proof) {
    for (Map<Long, EvidenceRecordImpl> foo : unansweredEvidence.values()) {
      for (EvidenceRecordImpl bar : foo.values()) {
        if (proof) {
          if (bar.isProof) return bar;
        } else {
          return bar;
        }
      }
    }
    return null;
  }

  public EvidenceRecord<Handle, Identifier> getFirstProof() {
    return getFirstUnansweredChallenge(true);
  }
}
