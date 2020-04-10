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
package org.mpisws.p2p.transport.peerreview.audit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStore;
import org.mpisws.p2p.transport.peerreview.evidence.AuditResponse;
import org.mpisws.p2p.transport.peerreview.evidence.ChallengeAudit;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceTransferProtocol;
import org.mpisws.p2p.transport.peerreview.evidence.ProofInconsistent;
import org.mpisws.p2p.transport.peerreview.evidence.ProofNonconformant;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.AuthRequest;
import org.mpisws.p2p.transport.peerreview.message.AuthResponse;
import org.mpisws.p2p.transport.peerreview.message.ChallengeMessage;
import org.mpisws.p2p.transport.peerreview.message.PeerReviewMessage;
import org.mpisws.p2p.transport.peerreview.replay.Verifier;
import org.mpisws.p2p.transport.peerreview.replay.playback.ReplaySM;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.MathUtils;
import rice.selector.TimerTask;
import static org.mpisws.p2p.transport.peerreview.Basics.renderStatus;

public class AuditProtocolImpl<Handle extends RawSerializable, Identifier extends RawSerializable> implements AuditProtocol<Handle, Identifier> {

  PeerReview<Handle, Identifier> peerreview;
  SecureHistory history; 
  PeerInfoStore<Handle, Identifier> infoStore;
  AuthenticatorStore<Identifier> authInStore;
  IdentityTransport<Handle, Identifier> transport;
  AuthenticatorStore<Identifier> authOutStore;
  EvidenceTransferProtocol<Handle, Identifier> evidenceTransferProtocol;
  AuthenticatorStore<Identifier> authCacheStore;
  
  /**
   * Here we remember calls to investigate() that have not been resolved yet 
   */
  Map<Identifier,ActiveInvestigationInfo<Handle>> activeInvestigation = new HashMap<Identifier, ActiveInvestigationInfo<Handle>>();

  /**
   * Here we remember calls to investigate() that have not been resolved yet 
   */
  Map<Identifier,ActiveAuditInfo<Handle, Identifier>> activeAudit = new HashMap<Identifier, ActiveAuditInfo<Handle,Identifier>>();

  int logDownloadTimeout;
  boolean replayEnabled;
  long lastAuditStarted;
  long auditIntervalMillis;
  
  /**
   * Null if there is no current timer.
   */
  protected TimerTask auditTimer;
  protected TimerTask progressTimer;

  Logger logger;
  
  public AuditProtocolImpl(PeerReview<Handle, Identifier> peerreview,
      SecureHistory history, PeerInfoStore<Handle, Identifier> infoStore,
      AuthenticatorStore<Identifier> authInStore,
      IdentityTransport<Handle, Identifier> transport,
      AuthenticatorStore<Identifier> authOutStore,
      EvidenceTransferProtocol<Handle, Identifier> evidenceTransferProtocol,
      AuthenticatorStore<Identifier> authCacheStore) {
    this.logger = peerreview.getEnvironment().getLogManager().getLogger(AuditProtocolImpl.class, null);
    this.peerreview = peerreview;
    this.history = history;
    this.infoStore = infoStore;
    this.authInStore = authInStore;
    this.transport = transport;
    this.authOutStore = authOutStore;
    this.evidenceTransferProtocol = evidenceTransferProtocol;
    this.authCacheStore = authCacheStore;
    
    this.progressTimer = null;
    this.logDownloadTimeout = DEFAULT_LOG_DOWNLOAD_TIMEOUT;
    this.replayEnabled = true;
    this.lastAuditStarted = peerreview.getTime();
    this.auditIntervalMillis = DEFAULT_AUDIT_INTERVAL_MILLIS;
    
    auditTimer = peerreview.getEnvironment().getSelectorManager().schedule(new TimerTask() {
      @Override
      public void run() {
        auditsTimerExpired();
      }},auditIntervalMillis);
  }

  
  public void setLogDownloadTimeout(int timeoutMicros) { 
    this.logDownloadTimeout = timeoutMicros; 
  }
  
  public void disableReplay() { 
    this.replayEnabled = false; 
  }

  /**
   *  Starts to audit a node 
   */
  void beginAudit(Handle target, Authenticator authFrom, Authenticator authTo, byte needPrevCheckpoint, boolean replayAnswer) {
    long evidenceSeq = peerreview.getEvidenceSeq();

    /* Put together an AUDIT challenge */
    ChallengeAudit audit = new ChallengeAudit(needPrevCheckpoint,authFrom,authTo);    
    ChallengeMessage<Identifier> auditRequest = new ChallengeMessage<Identifier>(peerreview.getLocalId(),evidenceSeq,audit);

    /* Create an entry in our audit buffer */

    ActiveAuditInfo<Handle, Identifier> aai = new ActiveAuditInfo<Handle, Identifier>(
        target,replayAnswer,false,peerreview.getTime()+logDownloadTimeout,auditRequest,evidenceSeq,null);
    activeAudit.put(peerreview.getIdentifierExtractor().extractIdentifier(target),aai);
    
    /* Send the AUDIT challenge to the target node */

    if (logger.level <= Logger.FINE) logger.log("Sending AUDIT request to "+aai.target+" (range="+authFrom.getSeq()+"-"+authTo.getSeq()+",eseq="+evidenceSeq+")");
    peerreview.transmit(target, auditRequest, null, null);
  }

  public void startAudits() {
    /* Find out which nodes we are currently witnessing */

//    Collection<Handle> buffer = new ArrayList<Handle>;
    Collection<Handle> buffer = peerreview.getApp().getMyWitnessedNodes(); //app->getMyWitnessedNodes(buffer, MAX_WITNESSED_NODES);
    
    /* For each of these nodes ... */
    
    for (Handle h : buffer) {
      Identifier i = peerreview.getIdentifierExtractor().extractIdentifier(h);
      /* If the node is not trusted, we don't audit */
    
      int status = infoStore.getStatus(i);
      if (status != STATUS_TRUSTED) {
        if (logger.level <= Logger.FINE) logger.log("Node "+h+" is "+renderStatus(status)+"; skipping audit");
        continue;
      }
    
      /* If a previous audit of this node is still in progress, we skip this round */
          
      if (!activeAudit.containsKey(h)) {
          
        /* Retrieve the node's newest and last-checked authenticators. Note that we need
           at least two distinct authenticators to be able to audit a node. */
          
        Authenticator authFrom;
        Authenticator authTo;
        boolean haveEnoughAuthenticators = true;
        byte needPrevCheckpoint = 0;

        if (logger.level <= Logger.INFO) logger.log("Starting to audit "+h);

        if ((authFrom = infoStore.getLastCheckedAuth(i)) == null) {
          if ((authFrom = authInStore.getOldestAuthenticator(i)) != null) {
            if (logger.level <= Logger.FINE) logger.log("We haven't audited this node before; using oldest authenticator");
            needPrevCheckpoint = 1;
          } else {
            if (logger.level <= Logger.FINE) logger.log("We don't have any authenticators for this node; skipping this audit");
            haveEnoughAuthenticators = false;
          }
        }

        if ((authTo = authInStore.getMostRecentAuthenticator(i)) == null) {
          if (logger.level <= Logger.FINE) logger.log("No recent authenticator; skipping this audit");
          haveEnoughAuthenticators = false;
        }
          
        if (haveEnoughAuthenticators && authFrom.getSeq()>authTo.getSeq()) {
          if (logger.level <= Logger.FINE) logger.log("authFrom>authTo; skipping this audit");
          haveEnoughAuthenticators = false; 
        }
          
        /* Add an entry to our table of ongoing audits. This entry will periodically
           be checked by cleanupAudits() */
          
        if (haveEnoughAuthenticators) {
          beginAudit(h, authFrom, authTo, needPrevCheckpoint, true);
        }
      } else {
        if (logger.level <= Logger.WARNING) logger.log("Node "+h+" is already being audited; skipping");
      }
    }
    
    /* Make sure that makeProgressOnAudits() is called regularly */
    scheduleProgressTimer();
  }

  protected void scheduleProgressTimer() {
    if (progressTimer == null) {
      progressTimer = peerreview.getEnvironment().getSelectorManager().schedule(new TimerTask() {

        @Override
        public void run() {
          makeProgressTimerExpired();
        }}, PROGRESS_INTERVAL_MILLIS);
    }        
  }
  
  /**
   * Called periodically to check if all audits have finished. An audit may not
   * finish if either (a) the target does not respond, or (b) the target's
   * response is discarded because it is malformed. In either case, we report
   * the failed AUDIT challenge to the witnesses.
   */
  void cleanupAudits() {
    long now = peerreview.getTime();
   
    for (ActiveAuditInfo<Handle, Identifier> foo : activeAudit.values()) { 
      if ((now >= foo.currentTimeout) && !foo.isReplaying) {
        Identifier i = peerreview.getIdentifierExtractor().extractIdentifier(foo.target);
//        int headerLen = 1 + peerreview->getIdentifierSizeBytes() + sizeof(long long);
           
        if (logger.level <= Logger.WARNING) logger.log("No response to AUDIT request; filing as evidence "+foo.evidenceSeq);
        try {
          infoStore.addEvidence(peerreview.getLocalId(), i, foo.evidenceSeq, 
              foo.request.challenge);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        peerreview.sendEvidenceToWitnesses(i, foo.evidenceSeq, foo.request.challenge);
  
        terminateAudit(foo.target);
      }          
    }
  }

  void terminateAudit(Handle h) {
    activeAudit.remove(h);
  }
  
  void terminateInvestigation(Handle h) {
    activeInvestigation.remove(h);
  }

  void makeProgressOnInvestigations() {
    long now = peerreview.getTime();
    
    for (ActiveInvestigationInfo<Handle> aii : activeInvestigation.values()) {
      if (aii.currentTimeout < now) {
        long authFromSeq = aii.authFrom!=null ? aii.authFrom.getSeq() : -1;
        long authToSeq = aii.authTo!=null ? aii.authTo.getSeq() : -1;
        if ((0<=authFromSeq) && (authFromSeq <= aii.since) && (aii.since < authToSeq)) {
          if (logger.level <= Logger.FINE) logger.log("Investigation of "+aii.target+" (since "+aii.since+") is proceeding with an audit");
          
//  #if 0
//          unsigned char lastCheckedAuth[authenticatorSizeBytes];
//          if (infoStore->getLastCheckedAuth(aii.target->getIdentifier(), lastCheckedAuth)) {
//            long long lastCheckedSeq = *(long long*)&lastCheckedAuth;
//            if (lastCheckedSeq > authFromSeq) {
//              if (logger.level <= Logger.FINER) logger.log("We already have the log up to %lld; starting investigation from there", lastCheckedSeq);
//              memcpy(aii.authFrom, lastCheckedAuth, authenticatorSizeBytes);
//              authFromSeq = lastCheckedSeq;
//            }
//          }
//  #endif
          
          if (authToSeq > authFromSeq) {
            if (logger.level <= Logger.FINER) logger.log("Authenticators: "+authFromSeq+"-"+authToSeq);
            beginAudit(aii.target, aii.authFrom, aii.authTo, FLAG_FULL_MESSAGES_SENDER, false);
            terminateInvestigation(aii.target);
          } else {
            if (logger.level <= Logger.WARNING) logger.log("Cannot start investigation; authTo<authFrom ?!? (since="+aii.since+", authFrom="+authFromSeq+", authTo="+authToSeq+")");
            terminateInvestigation(aii.target);
          }
        } else {
          if (logger.level <= Logger.FINE) logger.log("Retransmitting investigation requests for "+aii.target+" at "+aii.since);
          sendInvestigation(aii.target);
          aii.currentTimeout += INVESTIGATION_INTERVAL_MILLIS;
        }
      }
    }
  }

  private void sendInvestigation(Handle target) {
    // TODO Auto-generated method stub
    
  }


  void setAuditInterval(long newIntervalMillis) {
    auditIntervalMillis = newIntervalMillis;
    auditTimer.cancel();
    startAuditTimer();
  }


  /**
   * A periodic timer to audit all nodes for which we are a witness 
   */
  protected void auditsTimerExpired() {
    startAudits();
    lastAuditStarted = peerreview.getTime();
    startAuditTimer();
  }
  
  protected void startAuditTimer() {
    long now = peerreview.getTime();
    long nextTimeout = lastAuditStarted + (long)((500+(peerreview.getRandomSource().nextInt(1000)))*0.001*auditIntervalMillis);
    if (nextTimeout <= now) {
      nextTimeout = now + 1;
    }
    
    auditTimer = peerreview.getEnvironment().getSelectorManager().schedule(new TimerTask() {
    
      @Override
      public void run() {
        auditsTimerExpired();
      }
    
    },nextTimeout-now);    
  }
  
  /**
   * While some audits haven't finished, we must call makeProgress() regularly
   */
  protected void makeProgressTimerExpired() {
    progressTimer.cancel();
    cleanupAudits();
    makeProgressOnInvestigations();
    if (progressTimer == null && ((activeAudit.size() > 0) || (activeInvestigation.size() > 0))) {
      progressTimer = peerreview.getEnvironment().getSelectorManager().schedule(new TimerTask() {
        @Override
        public void run() {
          makeProgressTimerExpired();
        }        
      }, PROGRESS_INTERVAL_MILLIS);
    }
  }
  
  /**
   * Called by the challenge/response protocol if we have received a response to
   * an AUDIT challenge. At this point, we already know that we have all the
   * necessary certificates (because of the statement protocol).
   */
  public void processAuditResponse(Identifier subject, long timestamp, AuditResponse<Handle> response) throws IOException {
//    try {
    LogSnippet snippet = response.getLogSnippet();
    ActiveAuditInfo<Handle, Identifier> aai = findOngoingAudit(subject, timestamp);
    
    /* Read the header of the log snippet */
    
//    int reqHdrSize = 1+peerreview->getIdentifierSizeBytes()+sizeof(long long);
    ChallengeAudit challengeAudit = (ChallengeAudit)aai.request.challenge;
    long fromSeq = challengeAudit.from.getSeq();
    Authenticator toAuthenticator = challengeAudit.to; 
    long toSeq = toAuthenticator.getSeq();
    byte[] currentNodeHash = snippet.baseHash;
    byte[] initialNodeHash = snippet.baseHash;
//    int readptr = 0;
    Handle subjectHandle = response.getLogOwner(); //peerreview->readNodeHandle(snippet, (unsigned int*)&readptr, snippetLen);
    long currentSeq = snippet.getFirstSeq();
//    int extInfoLen = snippet[readptr++];
//    unsigned char *extInfo = &snippet[readptr];
//    readptr += extInfoLen;
//    
    long initialCurrentSeq = currentSeq;
//    memcpy(currentNodeHash, &snippet[readptr], hashSizeBytes);
//    memcpy(initialNodeHash, &snippet[readptr], hashSizeBytes);
//    readptr += hashSizeBytes;
//    int initialReadPtr = readptr;
//    char buf1[200];
//    
    
    /* Retrieve all the authenticators we have for this node */
    
    List<Authenticator> auths = authInStore.getAuthenticators(subject, fromSeq, toSeq);

    /* We have to keep a small fraction of the authenticators around so we can later
       answer AUTHREQ messages from other nodes. */

    if (logger.level <= Logger.FINE) logger.log("Checking of AUDIT response against "+auths.size()+" authenticators ["+fromSeq+"-"+toSeq+"]");

//    unsigned char mrauth[authenticatorSizeBytes];
    long mostRecentAuthInCache = -1;
    Authenticator mrauth = authCacheStore.getMostRecentAuthenticator(subject);
    if (mrauth != null) { 
      mostRecentAuthInCache = mrauth.getSeq();
    }
      
    for (int i=auths.size()-1; i>=0; i--) {
      Authenticator thisAuth = auths.get(i);
      long thisSeq = thisAuth.getSeq();
      if (thisSeq > (mostRecentAuthInCache + (AUTH_CACHE_INTERVAL*1000L))) {
        if (logger.level <= Logger.FINER) logger.log( "Caching auth "+thisSeq+" for "+subject);
        authCacheStore.addAuthenticator(subject, thisAuth);
        mostRecentAuthInCache = thisSeq;
      }
    }

    /* We read the entries one by one, calculating the node hashes as we go, and we compare
       the node hashes to our (sorted) list of authenticators. If there is any divergence,
       we have proof that the node is misbehaving. */

    int authPtr = auths.size() - 1;
    Authenticator nextAuth = (authPtr<0) ? null : auths.get(authPtr);
    long nextAuthSeq = (authPtr<0) ? -1 : nextAuth.getSeq();
    if (logger.level <= Logger.FINER) logger.log("  NA #"+authPtr+" "+nextAuthSeq);

    for (SnippetEntry sEntry : snippet.entries) {
      currentSeq = sEntry.seq;
//    while (readptr < snippetLen) {
//      unsigned char entryType = snippet[readptr++];
//      unsigned char sizeCode = snippet[readptr++];
//      unsigned int entrySize = sizeCode;
//      bool entryIsHashed = (sizeCode == 0);
//
//      if (sizeCode == 0xFF) {
//        entrySize = *(unsigned short*)&snippet[readptr];
//        readptr += 2;
//      } else if (sizeCode == 0xFE) {
//        entrySize = *(unsigned int*)&snippet[readptr];
//        readptr += 4;
//      } else if (sizeCode == 0) {
//        entrySize = hashSizeBytes;
//      }
//
      if (logger.level <= Logger.FINER) logger.log("[2] Entry type "+sEntry.type+", size="+sEntry.content.length+
          ", seq="+currentSeq+(sEntry.isHash ? " (hashed)" : ""));

      byte[] entry = sEntry.content;

      /* Calculate the node hash from the entry */

      byte[] contentHash;
      if (sEntry.isHash) {
        contentHash = sEntry.content;
        assert(contentHash.length == peerreview.getHashSizeInBytes());
      } else {
        contentHash = transport.hash(ByteBuffer.wrap(entry));
      }
      
      currentNodeHash = transport.hash(currentSeq, sEntry.type, currentNodeHash, contentHash);
      if (logger.level <= Logger.FINEST) logger.log("NH ["+MathUtils.toBase64(currentNodeHash)+"]");

      /* If we have an authenticator for this entry (matched by sequence number), the hash in the
         authenticator must match the node hash. If not, we have a proof of misbehavior. */

      if (authPtr >= 0) {
        boolean foundMisbehavior = false;
        
        if (currentSeq == nextAuthSeq) {          
          if (!Arrays.equals(currentNodeHash, nextAuth.getHash())) {
            if (logger.level <= Logger.WARNING) logger.log("Found a divergence for node <"+subject+">'s authenticator #"+currentSeq);
            foundMisbehavior = true;
          } else {  
            if (logger.level <= Logger.FINEST) logger.log("Authenticator verified OK");
  
            authPtr--;
            nextAuth = (authPtr<0) ? null : auths.get(authPtr);
            nextAuthSeq = (authPtr<0) ? -1 : nextAuth.getSeq();            
            if (logger.level <= Logger.FINEST) logger.log( "NA #"+authPtr+" "+ nextAuthSeq);
          }
        } else if (currentSeq > nextAuthSeq) {
          if (logger.level <= Logger.WARNING) logger.log("Node "+subject+" is trying to hide authenticator #"+nextAuthSeq);
          foundMisbehavior = true;
        }
        
        if (foundMisbehavior) {
          if (logger.level <= Logger.FINE) logger.log("Extracting proof of misbehavior from audit response");
          ProofInconsistent proof = new ProofInconsistent(toAuthenticator,nextAuth,snippet);
//          unsigned char proof[1+authenticatorSizeBytes+1+authenticatorSizeBytes+(snippetLen-posAfterNodeHandle)];
//          unsigned int pos = 0;
//          writeByte(proof, &pos, PROOF_INCONSISTENT);
//          writeBytes(proof, &pos, toAuthenticator, authenticatorSizeBytes);
//          writeByte(proof, &pos, 1);
//          writeBytes(proof, &pos, nextAuth, authenticatorSizeBytes);
//          writeBytes(proof, &pos, &snippet[posAfterNodeHandle], snippetLen-posAfterNodeHandle);
//          assert(pos <= sizeof(proof));

          long evidenceSeq = peerreview.getEvidenceSeq();
          if (logger.level <= Logger.FINE) logger.log("Filing proof against "+subject+" under evidence sequence number #"+evidenceSeq);
          infoStore.addEvidence(peerreview.getLocalId(), subject, evidenceSeq, proof);
          peerreview.sendEvidenceToWitnesses(subject, evidenceSeq, proof);

          terminateAudit(aai.target);
//          terminateAudit(peerreview.getIdentifierExtractor().extractIdentifier(subject));
          return;

        }
      }
    }

    /* All these authenticators for this segment checked out okay. We don't need them any more,
       so we can remove them from our local store. */
//
    if (logger.level <= Logger.FINE) logger.log( "All authenticators in range ["+fromSeq+","+toSeq+"] check out OK; flushing");
//  #warning must check the old auths; flush from fromSeq only!
    authInStore.flushAuthenticatorsFor(subject, Long.MIN_VALUE, toSeq);

    /* Find out if the log snipped is 'useful', i.e. if we can append it to our local history */

    String namebuf = infoStore.getHistoryName(subject);
    
    if (logger.level <= Logger.FINE) logger.log("opening history for "+namebuf);
    SecureHistory subjectHistory = peerreview.getHistoryFactory().open(namebuf, "w");
    
    boolean logCanBeAppended = false;
    long topSeq = 0;
    if (subjectHistory != null) {
      topSeq = subjectHistory.getTopLevelEntry().getSeq();
      if (topSeq >= fromSeq) 
        logCanBeAppended = true;
    } else {
      logCanBeAppended = true;
    }

    /* If it should not be replayed (e.g. because it was retrieved during an investigation), 
       we stop here */
    
    if (!aai.shouldBeReplayed/* && !logCanBeAppended*/) {
      if (logger.level <= Logger.FINE) logger.log( "This audit response does not need to be replayed; discarding");
      terminateAudit(aai.target);
      return;
    }

    /* Add entries to our local copy of the subject's history */
    
    if (logger.level <= Logger.FINE) logger.log( "Adding entries in snippet to log '"+namebuf+"'");
    if (!logCanBeAppended)
      throw new RuntimeException("Cannot append snippet to local copy of node's history; there appears to be a gap ("+topSeq+"-"+fromSeq+")!");
    
    if (subjectHistory == null) {
      subjectHistory = peerreview.getHistoryFactory().create(namebuf, initialCurrentSeq-1, initialNodeHash);
      if (subjectHistory == null)
        throw new RuntimeException("Cannot create subject history: '"+namebuf+"'");
    }
    
    long firstNewSeq = currentSeq - (currentSeq%1000000) + 1000000;
    subjectHistory.appendSnippetToHistory(snippet); //, , peerreview, initialCurrentSeq, firstNewSeq);
//  #warning need to verify older authenticators against history

    if (replayEnabled) {

      /* We need to replay the log segment, so let's find the last checkpoint */

      short[] markerTypes = new short[]{ EVT_CHECKPOINT };
      long lastCheckpointIdx = subjectHistory.findLastEntry(markerTypes, fromSeq);

      if (logger.level <= Logger.FINEST) logger.log( "LastCheckpointIdx="+lastCheckpointIdx+" (up to "+fromSeq+")");

      if (lastCheckpointIdx < 0) {
        if (logger.level <= Logger.WARNING) logger.log("Cannot find last checkpoint in subject history "+namebuf);
    //    if (subjectHistory->getNumEntries() >= MAX_ENTRIES_BETWEEN_CHECKPOINTS)
    //      throw new RuntimeException("TODO: Must generate proof due to lack of checkpoints");

        terminateAudit(subjectHandle);
        return;
      }

      /* Create a Verifier instance and get a Replay instance from the application */

      Verifier verifier = peerreview.getVerifierFactory().getVerifier(subjectHistory, subjectHandle, lastCheckpointIdx, fromSeq/1000000, snippet.getExtInfo());
      PeerReviewCallback<Handle, Identifier> replayApp = peerreview.getApp().getReplayInstance(verifier);
      if (replayApp == null) throw new RuntimeException("Application returned NULL when getReplayInstance() was called");

      verifier.setApplication(replayApp);

      aai.verifier = verifier;
      aai.isReplaying = true;

      if (logger.level <= Logger.INFO) logger.log( "REPLAY ============================================");
      if (logger.level <= Logger.FINE) logger.log( "Node being replayed: "+subjectHandle);
      if (logger.level <= Logger.FINE) logger.log( "Range in log       : "+fromSeq+"-"+toSeq);

      /* Do the replay */

//      while (verifieverifier.makeProgress());
//      if (true) throw new RuntimeException("delme");
      ReplaySM sm = ((ReplaySM)verifier.getEnvironment().getSelectorManager());
      while(sm.makeProgress());
      
      boolean verifiedOK = verifier.verifiedOK(); 
      if (logger.level <= Logger.INFO) logger.log( "END OF REPLAY: "+(verifiedOK ? "VERIFIED OK" : "VERIFICATION FAILED")+" =================");

      /* If there was a divergence, we have a proof of misbehavior */

      if (!verifiedOK) {
//        FILE *outfile = tmpfile();
        snippet = subjectHistory.serializeRange(lastCheckpointIdx, subjectHistory.getNumEntries()-1, null);
        if (snippet == null) throw new RuntimeException("Cannot serialize range for PROOF "+subject);

//        int snippet2size = ftell(outfile);
        long lastCheckpointSeq;
        IndexEntry foo = subjectHistory.statEntry(lastCheckpointIdx);
        if (foo == null) throw new RuntimeException("Cannot stat checkpoint entry");
        lastCheckpointSeq = foo.getSeq();
        
        if (logger.level <= Logger.WARNING) logger.log("Audit revealed a protocol violation; filing evidence (snippet from "+lastCheckpointSeq+")");

        ProofNonconformant<Handle> proof = new ProofNonconformant<Handle>(toAuthenticator,subjectHandle,snippet);
//        unsigned int maxProofLen = 1+authenticatorSizeBytes+MAX_HANDLE_SIZE+sizeof(long long)+snippet2size;
//        unsigned char *proof = (unsigned char*)malloc(maxProofLen);
//        unsigned int proofLen = 0;
//        writeByte(proof, &proofLen, PROOF_NONCONFORMANT);
//        writeBytes(proof, &proofLen, toAuthenticator, authenticatorSizeBytes);
//        subjectHandle->write(proof, &proofLen, maxProofLen);
//        writeLongLong(proof, &proofLen, lastCheckpointSeq);
//        fseek(outfile, 0, SEEK_SET);
//        fread(&proof[proofLen], snippet2size, 1, outfile);
//        proofLen += snippet2size;
//        assert(proofLen <= maxProofLen);
//
        long evidenceSeq = peerreview.getEvidenceSeq();
        infoStore.addEvidence(peerreview.getLocalId(), subject, evidenceSeq, proof);
        peerreview.sendEvidenceToWitnesses(subject, evidenceSeq, proof);
      } 
    }

    /* Terminate the audit, and remember the last authenticator for further reference */

    if (logger.level <= Logger.FINE) logger.log( "Audit completed; terminating");  
    infoStore.setLastCheckedAuth(peerreview.getIdentifierExtractor().extractIdentifier(aai.target), ((ChallengeAudit)aai.request.challenge).to);
    terminateAudit(aai.target);
//    } catch (IOException ioe) {
//      throw new RuntimeException(ioe);
//    }
  }

  public ActiveAuditInfo<Handle, Identifier> findOngoingAudit(Identifier subject, long evidenceSeq) {
    ActiveAuditInfo<Handle, Identifier> ret = activeAudit.get(subject);
    if (ret == null) return null;
    if (!ret.isReplaying && ret.evidenceSeq == evidenceSeq) {
        return ret;
    }
    return null;
  }

  public Evidence statOngoingAudit(Identifier subject, long evidenceSeq) {    
    ActiveAuditInfo<Handle, Identifier> aii = findOngoingAudit(subject, evidenceSeq);
    if (aii == null) return null;
    return aii.request.challenge;
  }
  
  /* Handle an incoming datagram, which could be either a request for autheticators, or a response to such a request */

  public void handleIncomingDatagram(Handle handle, PeerReviewMessage message) {
    
    switch (message.getType()) {
      case MSG_AUTHREQ: { /* Request for authenticators */
        AuthRequest<Identifier> request = (AuthRequest<Identifier>)message;
        
//        if (msglen == (int)(1+sizeof(long long)+peerreview->getIdentifierSizeBytes())) {

          /* The request contains a timestamp T; we're being asked to return two authenticators,
             one has to be older than T, and the other must be as recent as possible */
          
//          unsigned int pos = 1;
//          long long since = readLongLong(message, &pos);
//          Identifier *id = peerreview->readIdentifier(message, &pos, msglen);
          if (logger.level <= Logger.INFO) logger.log("Received authenticator request for "+request.subject+" (since "+request.timestamp+")");
          
//          unsigned char response[1+peerreview->getIdentifierSizeBytes()+2*authenticatorSizeBytes];
//          boolean canRespond = true;
//          pos = 0;
//          writeByte(response, &pos, MSG_AUTHRESP);
//          id->write(response, &pos, sizeof(response));
          
          /* There are several places we might find 'old' authenticators. Check all of them,
             and pick the authenticator that is closest to T */
          
//          unsigned char a1[authenticatorSizeBytes];
          Authenticator a1 = authInStore.getLastAuthenticatorBefore(request.subject, request.timestamp);
//          long long seq1 = *(long long*)&a1;
//          unsigned char a2[authenticatorSizeBytes];
          Authenticator a2 = authCacheStore.getLastAuthenticatorBefore(request.subject, request.timestamp);
//          long long seq2 = *(long long*)&a2;
//          unsigned char a3[authenticatorSizeBytes];
          Authenticator a3 = authInStore.getOldestAuthenticator(request.subject);
//          long long seq3 = *(long long*)&a3;
          
          Authenticator best = null;
//          boolean haveBest = false;
//          long seqBest = -1;
          
          if (a1 != null && (best == null || (!(best.getSeq()<request.timestamp) && (a1.getSeq()<request.timestamp)) || ((best.getSeq()<request.timestamp) && (a1.getSeq()<request.timestamp) && (best.getSeq()<a1.getSeq())))) {
            best = a1;
          }

          if (a2 != null && (best == null || (!(best.getSeq()<request.timestamp) && (a2.getSeq()<request.timestamp)) || ((best.getSeq()<request.timestamp) && (a2.getSeq()<request.timestamp) && (best.getSeq()<a2.getSeq())))) {
            best = a2;
          }

          if (a3 != null && (best == null || (!(best.getSeq()<request.timestamp) && (a3.getSeq()<request.timestamp)) || ((best.getSeq()<request.timestamp) && (a3.getSeq()<request.timestamp) && (best.getSeq()<a3.getSeq())))) {
            best = a3;
          }
          
//          if (best != null) {
////            memcpy(&response[pos], best, authenticatorSizeBytes);
//          } else {
//            canRespond = false;
//          }
          
          /* Recent authenticators can be found in two places; check both */
          
          Authenticator foo = authInStore.getMostRecentAuthenticator(request.subject);
          if (foo == null) {
            foo = authCacheStore.getMostRecentAuthenticator(request.subject);
          }
          
//          pos += 2*authenticatorSizeBytes;
//          assert(pos == sizeof(response));
          
          if (best != null && foo != null) {
            AuthResponse<Identifier> response = new AuthResponse<Identifier>(request.subject,best,foo);
            peerreview.transmit(handle, response, null, null);
          } else {
            if (logger.level <= Logger.WARNING) logger.log("Cannot respond to this request; we don't have any authenticators for "+request.subject);
          }
        }
        
        break;
      case MSG_AUTHRESP: { /* Response to a request for authenticators */
        AuthResponse<Identifier> response = (AuthResponse<Identifier>)message;
//        if (msglen == (int)(1+peerreview->getIdentifierSizeBytes()+2*authenticatorSizeBytes)) {
//          unsigned int pos = 1;
//          Identifier *id = peerreview->readIdentifier(message, &pos, msglen);
//          unsigned char *authFrom = &message[pos];
//          unsigned char *authTo = &message[pos+authenticatorSizeBytes];
          if (logger.level <= Logger.FINE) logger.log("Received AUTHRESP(<"+response.subject+">, "+response.authFrom+".."+response.authTo+") from "+handle);

//  #warning must check signatures here        
          int idx = -1;
          ActiveInvestigationInfo<Handle> investigation = activeInvestigation.get(response.subject);
          if (idx >= 0) {
            if (investigation.authFrom == null || (response.authFrom.getSeq()<investigation.authFrom.getSeq())) {
              investigation.authFrom =  response.authFrom;
            }
            
            if (investigation.authTo == null || (response.authTo.getSeq()>investigation.authTo.getSeq())) {
              investigation.authTo = response.authTo;
            }
          } else {
            if (logger.level <= Logger.WARNING) logger.log("AUTH response does not match any ongoing investigations; ignoring");
          }
        }
        
        break;
      default:
        throw new RuntimeException("AuditProtocol cannot handle incoming datagram type #"+message.getType());
    }
  }

  /**
   * Starts an investigation by sending authenticator requests to all the
   * witnesses. Most of them are (hopefully) going to respond; we're going to
   * pick the authenticators that work best for us.
   */
  protected void sendInvestigation(ActiveInvestigationInfo<Handle> investigation) {
    Identifier id = peerreview.getIdentifierExtractor().extractIdentifier(investigation.target);
    AuthRequest<Identifier> request = new AuthRequest<Identifier>(investigation.since,id);
   
    evidenceTransferProtocol.sendMessageToWitnesses(id, request, null, null);
  }

  /**
   * Start an investigation 
   * since is in millis
   */
  public void investigate(Handle target, long since) {
    Identifier id = peerreview.getIdentifierExtractor().extractIdentifier(
        target);
    ActiveInvestigationInfo<Handle> investigation = activeInvestigation.get(id);
    if (investigation != null) {
      if (since * 1000000 > investigation.since) {
        if (logger.level <= Logger.FINE) logger.log("Skipping investigation request for " + target + " at "
              + since + ", since an investigation at " + investigation.since + " is already ongoing");
        return;
      }

      if (logger.level <= Logger.FINE)
        logger.log("Extending existing investigation from "+ investigation.since + " to " + since * 1000000);
      investigation.since = since * 1000000; /* log granularity */
    } else {
      investigation = new ActiveInvestigationInfo<Handle>(target, since * 1000000,
          peerreview.getTime() + INVESTIGATION_INTERVAL_MILLIS, null, null);
      activeInvestigation.put(id, investigation);
    }
    sendInvestigation(investigation);
    scheduleProgressTimer();
  }  
}
