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
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.audit.LogSnippet;
import org.mpisws.p2p.transport.peerreview.challenge.ChallengeResponseProtocol;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.evidence.AuditResponse;
import org.mpisws.p2p.transport.peerreview.evidence.ChallengeAudit;
import org.mpisws.p2p.transport.peerreview.evidence.ProofInconsistent;
import org.mpisws.p2p.transport.peerreview.evidence.ProofNonconformant;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.PeerReviewMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.peerreview.replay.Verifier;

import rice.Continuation;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.MathUtils;
import rice.p2p.util.tuples.Tuple;

public class StatementProtocolImpl<Handle extends RawSerializable, Identifier extends RawSerializable> implements StatementProtocol<Handle, Identifier> {

  protected Logger logger;
  
  protected Map<Identifier, LinkedList<IncompleteStatementInfo<Handle, Identifier>>> incompleteStatement = 
    new HashMap<Identifier, LinkedList<IncompleteStatementInfo<Handle, Identifier>>>();
  
  protected ChallengeResponseProtocol<Handle, Identifier> challengeProtocol;
  protected IdentityTransport<Handle, Identifier> transport;
  protected PeerReview<Handle, Identifier> peerreview;
  protected PeerInfoStore<Handle, Identifier> infoStore;
  protected boolean progressTimerActive;
  
  public StatementProtocolImpl(
      PeerReview<Handle, Identifier> peerreview,
      ChallengeResponseProtocol<Handle, Identifier> challengeProtocol,
      PeerInfoStore<Handle, Identifier> infoStore,
      IdentityTransport<Handle, Identifier> transport) {
    this.peerreview = peerreview;
    this.challengeProtocol = challengeProtocol;
    this.infoStore = infoStore;
    this.transport = transport;
    
    this.logger = peerreview.getEnvironment().getLogManager().getLogger(StatementProtocolImpl.class, null);
  }

  /**
   * Called if we have received a certificate for a new nodeID. If any messages
   * in our queue were waiting for this certificate, we may be able to forward
   * them now.
   */
  public void notifyCertificateAvailable(Identifier id) {
   Collection<IncompleteStatementInfo<Handle, Identifier>> foo = incompleteStatement.get(id);
   if (foo != null) {
     for (IncompleteStatementInfo<Handle, Identifier> is : foo) {
       if (!is.isFinished() && is.isMissingCertificate()) {
         makeProgressOnStatement(is);
       }
     }
   }
  }
  
  /**
   *  If a message hangs around in our queue for too long, we discard it 
   */
  public void cleanupIncompleteStatements() {    
    long now = peerreview.getTime();
    Iterator<LinkedList<IncompleteStatementInfo<Handle, Identifier>>> fooIter = incompleteStatement.values().iterator();
    while(fooIter.hasNext()) {
      LinkedList<IncompleteStatementInfo<Handle, Identifier>> foo = fooIter.next();
      Iterator<IncompleteStatementInfo<Handle, Identifier>> barIter = foo.iterator();
      while(barIter.hasNext()) {
        IncompleteStatementInfo<Handle, Identifier> bar = barIter.next();
        if (bar.isFinished() || (bar.getCurrentTimeout() < now)) {
          if (!bar.isFinished()) {
            if (logger.level <= Logger.WARNING) logger.log("Statement by "+bar.sender+" is incomplete after the timeout; discarding "+bar);
          }
          barIter.remove();            
        }
      }
      if (foo.isEmpty()) {
        fooIter.remove();
      }
    }
  }

  /**
   * We use a periodic timer to throw out stale messages 
   */
  void timerExpired(int timerID) {
    throw new RuntimeException("todo: implement");
  }
  
  /**
   * Incoming ACCUSTION and RESPONSE messages come here first. We check whether
   * we have all the necessary nodeID certificates; if not, we request them from
   * the sender.
   */
  public void handleIncomingStatement(Handle source, Statement<Identifier> statement, Map<String, Object> options) {
    assert((statement.getType() == MSG_ACCUSATION) || (statement.getType() == MSG_RESPONSE));
    
    if (logger.level <= Logger.INFO) logger.log("Incoming "+((statement.getType() == MSG_ACCUSATION) ? "accusation" : "response")+" from "+source);
    
    /* Find an empty spot in our message buffer and store the message there */
    IncompleteStatementInfo<Handle, Identifier> idx = new IncompleteStatementInfo<Handle, Identifier>(false,source,peerreview.getTime() + STATEMENT_COMPLETION_TIMEOUT_MILLIS,statement,false,null,options);
    LinkedList<IncompleteStatementInfo<Handle, Identifier>> foo = incompleteStatement.get(statement.subject);
    if (foo == null) {
      foo = new LinkedList<IncompleteStatementInfo<Handle,Identifier>>();
      incompleteStatement.put(statement.subject, foo);
    }
    foo.add(idx);
    
    /* Then call our central do-something-useful function */
     
    makeProgressOnStatement(idx);
    cleanupIncompleteStatements();
  }
  
  /**
   * This reads a log snippet entry by entry, looks up all the nodeIDs that are
   * mentioned, and requests the first nodeID certificate we don't have locally.
   * Also, it checks if the snipped is malformed in any way. That way, the other
   * protocols won't have to do all these sanity checks later.
   */
  int checkSnippetAndRequestCertificates(LogSnippet snippet, IncompleteStatementInfo<Handle, Identifier> idx) {
    Tuple<Integer,Identifier> ret = peerreview.getEvidenceTool().checkSnippet(snippet);
    int code = ret.a();
    final Identifier missingCertID = ret.b();
   
    if (code == CERT_MISSING) {
      assert(missingCertID != null);
  
      idx.isMissingCertificate = true;
      idx.missingCertificateID = missingCertID;
      if (logger.level <= Logger.FINE) logger.log("AUDIT RESPONSE requires certificate for "+missingCertID+"; requesting");
      peerreview.requestCertificate(idx.sender, missingCertID);  
      
    }
  
    return code;
  }

  /* Tries to make progress on some waiting message. We try to either (a) request a certificate
  we need but don't have, (b) forward the message to another protocol, or (c) discard
  the message because it's malformed. */

  @SuppressWarnings("unchecked")
  public void makeProgressOnStatement(IncompleteStatementInfo<Handle, Identifier> idx) {
    assert(!idx.finished);

    /* Fetch the statement and reset all the 'missing' fields */

    Statement<Identifier> statement = idx.statement;
    idx.isMissingCertificate = false;
 
    Identifier originator = statement.originator;
    Identifier subject = statement.subject;
    long timestamp = statement.evidenceSeq;
    Evidence payload = statement.evidence;
 
    /* Retrieve the subject's public key if we don't have it already */

    if (!transport.hasCertificate(subject)) {
      if (logger.level <= Logger.FINE) logger.log("Need subject's certificate to verify statement; asking source for it");
      idx.isMissingCertificate = true;
      idx.missingCertificateID = subject;
      peerreview.requestCertificate(idx.sender, subject);
      return;
    }
 
  // unsigned char subjectAsBytes[identifierSizeBytes];
  // unsigned int subjectAsBytesLen = 0;
  // subject->write(subjectAsBytes, &subjectAsBytesLen, sizeof(subjectAsBytes));
   
   /* Further checking depends on the type of the statement. At this point, we may still request
      additional material from the sender, if it becomes necessary. */
   
   if (statement.getType() == MSG_ACCUSATION) {
   
     /* === CHECK ACCUSATIONS === */
   
     switch (payload.getEvidenceType()) {
       case CHAL_AUDIT: {
         ChallengeAudit auditEvidence = (ChallengeAudit)payload;
  
         if (!peerreview.verify(subject, auditEvidence.from)) {
           if (logger.level <= Logger.WARNING) logger.log("AUDIT challenge's first authenticator has an invalid signature");
           idx.finished = true;
           return;
         }
  
         if (!peerreview.verify(subject, auditEvidence.to)) {
           if (logger.level <= Logger.WARNING) logger.log("AUDIT challenge's second authenticator has an invalid signature");
           idx.finished = true;
           return;
         }
  
         break;
       }
       case CHAL_SEND: {
         UserDataMessage<Handle> udm = (UserDataMessage<Handle>)payload;
  
         byte[] innerHash = udm.getInnerHash(subject, transport);
     
         // NOTE: This call puts the authenticator in our authInStore. This is intentional! Otherwise the bad guys
         // could fork their logs and send forked messages only as CHAL_SENDs.
         Authenticator authenticator = peerreview.extractAuthenticator(peerreview.getIdentifierExtractor().extractIdentifier(udm.getSenderHandle()), udm.getTopSeq(), EVT_SEND, innerHash, udm.getHTopMinusOne(), udm.getSignature());
         if (authenticator == null) {
           if (logger.level <= Logger.WARNING) logger.log("Message in SEND challenge is not properly signed; discarding");
           idx.finished = true;
           return;
         }       
         break;
       }
       case PROOF_INCONSISTENT: {
         ProofInconsistent pi = (ProofInconsistent)payload;
//         if (pi.snippet != null)
//           throw new RuntimeException("Inconsistency II (log) not implemented "+pi);
  
         if (!peerreview.verify(subject, pi.auth1)) {
           if (logger.level <= Logger.WARNING) logger.log("INCONSISTENT proof's first authenticator has an invalid signature");
           idx.finished = true;
           return;
         }
  
         if (!peerreview.verify(subject, pi.auth2)) {
           if (logger.level <= Logger.WARNING) logger.log("INCONSISTENT proof's second authenticator has an invalid signature");
           idx.finished = true;
           return;
         }

         long seq1 = pi.auth1.getSeq();
         long seq2 = pi.auth2.getSeq();
         byte[] hash1 = pi.auth1.getHash();
         byte[] hash2 = pi.auth2.getHash();

         if(pi.snippet == null) {           
           if (seq1 != seq2) {
             if (logger.level <= Logger.WARNING) logger.log("INCONSISTENT-0 proof's authenticators don't have matching sequence numbers (seq1="+seq1+", seq2="+seq2+") -- discarding");
             idx.finished = true;
             return;
           }
           
           if (Arrays.equals(hash1, hash2)) {
             if (logger.level <= Logger.WARNING) logger.log("INCONSISTENT-0 proof's authenticators have the same hash (seq1="+seq1+", seq2="+seq2+") hash: "+MathUtils.toBase64(hash1)+" -- discarding");
             idx.finished = true;
             return;
           }           
         } else {
//           unsigned int xpos = 2+2*authenticatorSizeBytes;
//           unsigned char extInfoLen = ((xpos+sizeof(long long))<payloadLen) ? payload[xpos+sizeof(long long)] : 0;
//
//           if (payloadLen < (xpos + sizeof(long long) + 1 + extInfoLen + hashSizeBytes)) {
//             if (logger.level <= Logger.WARNING) logger.log("INCONSISTENT-1 proof is too short (case #2); discarding");
//             idx.finished = true;
//             return;
//           }
           
           long firstSeq = pi.snippet.getFirstSeq();
//           unsigned char *baseHash = &payload[xpos+sizeof(long long)+1+extInfoLen];
//           unsigned char *snippet = &payload[xpos+sizeof(long long)+1+extInfoLen+hashSizeBytes];
//           int snippetLen = payloadLen - (xpos+sizeof(long long)+1+extInfoLen+hashSizeBytes);
           
//           assert(snippetLen >= 0);
           
           if (!((firstSeq<seq2) && (seq2<seq1))) {
             if (logger.level <= Logger.WARNING) logger.log("INCONSISTENT-1 proof's sequence numbers do not make sense (first="+firstSeq+", seq2="+seq2+", seq1="+seq1+") -- discarding");
             idx.finished = true;
             return;
           }
           
           if (!pi.snippet.checkHashChainContains(hash1, seq1, transport, logger)) {
             if (logger.level <= Logger.WARNING) logger.log("Snippet in INCONSISTENT-1 proof cannot be authenticated using first authenticator (#"+seq1+") -- discarding");
             idx.finished = true;
             return;
           }
           
           if (pi.snippet.checkHashChainContains(hash2, seq2, transport, logger)) {
             if (logger.level <= Logger.WARNING) logger.log("INCONSISTENT-1 proof claims that authenticator 2 (#"+seq2+") is not in the snippet, but it is -- discarding");
             idx.finished = true;
             return;
           }
         }       

         break;
       }
       case PROOF_NONCONFORMANT: {
         ProofNonconformant<Handle> pn = (ProofNonconformant<Handle>)payload;
         
  //       unsigned int pos = 1;
  //       unsigned char *auth = &payload[pos];
  //       pos += authenticatorSizeBytes;
  //       NodeHandle *subjectHandle = peerreview->readNodeHandle(payload, &pos, payloadLen);
  //       long long firstSeq = readLongLong(payload, &pos);
  //       unsigned char *baseHash = &payload[pos];
  //       pos += hashSizeBytes;
  //
         /* Is the authenticator properly signed? */
  
  //       unsigned char signedHash[hashSizeBytes];       
         if (!peerreview.verify(subject, pn.to)) {
           if (logger.level <= Logger.WARNING) logger.log("NONCONFORMANT proof's authenticator has an invalid signature");
           idx.finished = true;
           return;
         }
  
         /* Is the snippet well-formed, and do we have all the certificates? */
  
         switch (checkSnippetAndRequestCertificates(pn.snippet, idx)) {
           case INVALID:
             if (logger.level <= Logger.WARNING) logger.log("PROOF NONCONFORMANT is not well-formed; discarding");
             idx.finished = true;
             return;
           case CERT_MISSING:
             return;
           default:
             break;
         }
  
         /* Are the signatures in the snippet okay, and does int contain the authenticated node? */
         
         if (!peerreview.getEvidenceTool().checkSnippetSignatures(
             pn.snippet,pn.myHandle,null, FLAG_INCLUDE_CHECKPOINT,null,pn.to.getHash(),pn.to.getSeq())) {
           if (logger.level <= Logger.WARNING) logger.log("PROOF NONCONFORMANT cannot be validated (signatures or authenticator)");
           return;
         }
         
         /* Now we are convinced that 
              - the authenticator is valid
              - the snippet is well-formed, and we have all the certificates
              - the snippet starts with a checkpoint and contains the authenticated node 
            We must now replay the log; if the proof is valid, it won't check out. */
         try {
           SecureHistory subjectHistory = peerreview.getHistoryFactory().createTemp(pn.snippet.getFirstSeq()-1, pn.snippet.getBaseHash());
           subjectHistory.appendSnippetToHistory(pn.snippet);
  //         peerreview.getEvidenceTool().appendSnippetToHistory(pn.snippet, subjectHistory, -1);
    
    //#warning ext info missing
           Verifier<Handle> verifier = peerreview.getVerifierFactory().getVerifier(subjectHistory, pn.myHandle, 1, pn.snippet.getFirstSeq()/1000000, null);
           PeerReviewCallback<Handle, Identifier> replayApp = peerreview.getApp().getReplayInstance(verifier);
           verifier.setApplication(replayApp);
    
           if (logger.level <= Logger.INFO) logger.log("REPLAY ============================================");
           if (logger.level <= Logger.FINE) logger.log("Node being replayed: "+pn.myHandle);
           if (logger.level <= Logger.FINE) logger.log("Range in log       : "+pn.snippet.getFirstSeq()+"-?");
    
           while (verifier.makeProgress());
           boolean verifiedOK = verifier.verifiedOK();
           
           if (verifiedOK) {
             if (logger.level <= Logger.WARNING) logger.log("PROOF NONCONFORMANT contains a log snippet that actually is conformant; discarding");
             return;
           }
         } catch (IOException ioe) {
           if (logger.level <= Logger.WARNING) logger.logException("Couldn't replay!!! "+pn, ioe);
         }
         break;        
  //       throw new RuntimeException("todo: implement");
       }
       default: {
         if (logger.level <= Logger.WARNING) logger.log("Unknown payload type #"+payload.getEvidenceType()+" in accusation; discarding");
         idx.finished = true;
         return;
       }
     }
   } else {
   
     /* === CHECK RESPONSES === */
   
     switch (payload.getEvidenceType()) {
       
       /* To check an AUDIT RESPONSE, we need to verify that:
             - it is well-formed
             - we have certificates for all senders occurring in RECV entries 
          We do NOT check signatures, sequence numbers, or whether the content makes any
          sense at all. We also do NOT check whether this is a valid response to some
          specific challenge.
       */
     
       case CHAL_AUDIT: {
  
         if (logger.level <= Logger.FINE) logger.log("Checking AUDIT RESPONSE statement");
         
         AuditResponse<Handle> auditResponse = (AuditResponse<Handle>)payload;
         
  //       int readptr = 0;
  //       readByte(payload, (unsigned int*)&readptr); /* RESP_AUDIT */
  //       NodeHandle *subjectHandle = peerreview->readNodeHandle(payload, (unsigned int*)&readptr, payloadLen);
  //       readptr += sizeof(long long);
  //       readptr += 1 + payload[readptr];
  //       readptr += hashSizeBytes;
  //       delete subjectHandle;
  
         switch (checkSnippetAndRequestCertificates(auditResponse.getLogSnippet(), idx)) {
           case INVALID:
             if (logger.level <= Logger.WARNING) logger.log("AUDIT RESPONSE is not well-formed; discarding");
             idx.finished = true;
             return;
           case CERT_MISSING:
             return;
           default:
             break;
         }
  
         break;
       }
       
       case CHAL_SEND:
       {
         AckMessage<Identifier> ackMessage = (AckMessage<Identifier>)payload;
         // this will be handled in challengeProtocol.handleStatement()
         break;
       }
       
       default :
       {
         if (logger.level <= Logger.WARNING) logger.log("Unknown payload type #"+payload.getEvidenceType()+" in response; discarding");
         idx.finished = true;
         return;
       }
     }
   }
   
   /* At this point, we are convinced that the statement is valid, and we have all the 
      necessary supplemental material to be able to check it and any responses to it */
  
    try {
      challengeProtocol.handleStatement(idx.sender, statement, idx.options);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    idx.finished = true;
  }

}
