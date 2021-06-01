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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.audit.AuditProtocol;
import org.mpisws.p2p.transport.peerreview.audit.LogSnippet;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStore;
import org.mpisws.p2p.transport.peerreview.commitment.CommitmentProtocol;
import org.mpisws.p2p.transport.peerreview.evidence.AuditResponse;
import org.mpisws.p2p.transport.peerreview.evidence.ChallengeAudit;
import org.mpisws.p2p.transport.peerreview.history.HashPolicy;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.EvidenceRecord;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStoreImpl;
import org.mpisws.p2p.transport.peerreview.message.AccusationMessage;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.ChallengeMessage;
import org.mpisws.p2p.transport.peerreview.message.PeerReviewMessage;
import org.mpisws.p2p.transport.peerreview.message.ResponseMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.p2p.util.tuples.Tuple;

public class ChallengeResponseProtocolImpl<Handle extends RawSerializable, Identifier extends RawSerializable> 
    implements PeerReviewConstants, ChallengeResponseProtocol<Handle, Identifier> {
  PeerReviewImpl<Handle, Identifier> peerreview;
  IdentityTransport<Handle, Identifier> transport;
  PeerInfoStore<Handle, Identifier> infoStore;
  SecureHistory history;
  AuthenticatorStore<Identifier> authOutStore; 
  AuditProtocol<Handle, Identifier> auditProtocol;
  CommitmentProtocol<Handle, Identifier> commitmentProtocol;
  protected Logger logger;
  Map<Handle, LinkedList<PacketInfo<Handle, Identifier>>> queue = new HashMap<Handle,LinkedList<PacketInfo<Handle, Identifier>>>();
  
  public ChallengeResponseProtocolImpl(
      PeerReviewImpl<Handle, Identifier> peerReviewImpl,
      IdentityTransport<Handle, Identifier> transport,
      PeerInfoStore<Handle, Identifier> infoStore, SecureHistory history,
      AuthenticatorStore<Identifier> authOutStore, AuditProtocol<Handle, Identifier> auditProtocol,
      CommitmentProtocol<Handle, Identifier> commitmentProtocol) {
    this.peerreview = peerReviewImpl;
    this.transport = transport;
    this.infoStore = infoStore;
    this.history = history;
    this.authOutStore = authOutStore;
    this.auditProtocol = auditProtocol;
    this.commitmentProtocol = commitmentProtocol;
    
    this.logger = peerreview.getEnvironment().getLogManager().getLogger(ChallengeResponseProtocolImpl.class, null);
  }
  
  protected void copyAndEnqueueTail(Handle source, Evidence evidence, 
      boolean isAccusation, Identifier subject, Identifier originator, long evidenceSeq, 
      Map<String, Object> options) {
    LinkedList<PacketInfo<Handle, Identifier>> list = queue.get(source);
    if (list == null) {
      list = new LinkedList<PacketInfo<Handle,Identifier>>();
      queue.put(source, list);
    }    
    list.addLast(new PacketInfo<Handle, Identifier>(source,evidence,isAccusation,subject,originator,evidenceSeq,options));
  }
  
  protected void deliver(PacketInfo<Handle, Identifier> pi) {
    try {
      if (pi.isAccusation) {
        infoStore.addEvidence(pi.originator, pi.subject, pi.evidenceSeq, pi.message, pi.source);
      } else {
        commitmentProtocol.handleIncomingMessage(pi.source, (UserDataMessage<Handle>)pi.message, pi.options);
      } 
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /* If a node goes back to TRUSTED, we deliver all pending messages */

  public void notifyStatusChange(Identifier id, int newStatus) {
    switch (newStatus) {
    case STATUS_TRUSTED:
      LinkedList<PacketInfo<Handle, Identifier>> list = queue.remove(id);
      if (list != null) {
        for (PacketInfo<Handle, Identifier> pi : list) {
          deliver(pi);
        }
      }
      break;
    case STATUS_EXPOSED:
      queue.remove(id);
      break;
    case STATUS_SUSPECTED:
      break;
    }
  }
  
  /**
   * Called when another node sends us a challenge. If the challenge is valid, we respond. 
   */
  public void handleChallenge(Handle source, ChallengeMessage<Identifier> challenge, Map<String, Object> options) throws IOException {
    short type = challenge.getChallengeType(); 
    switch (type) {

      /* AUDIT challenges: We respond with a serialized log snippet */
        
      case CHAL_AUDIT: {
        ChallengeAudit audit = (ChallengeAudit)challenge.getChallenge();

        byte flags = audit.flags;
        long seqFrom = audit.from.getSeq();
        long seqTo = audit.to.getSeq();
        if (logger.level <= Logger.FINER) logger.log(
            "Received an AUDIT challenge for ["+seqFrom+","+seqTo+"] from "+source+
            " (eseq="+challenge.evidenceSeq+", flags="+audit.flags+")");
        
        if (seqTo < seqFrom) {
          if (logger.level <= Logger.WARNING) logger.log("Received an AUDIT challenge with seqTo<seqFrom:"+seqTo+"<"+seqFrom);
          return;
        }
        
        if ((seqFrom < history.getBaseSeq()) || (seqTo > history.getLastSeq())) {
          if (logger.level <= Logger.WARNING) logger.log(
              "Received an AUDIT whose range ["+seqFrom+"-"+seqTo+"] is outside our history range ["+
              history.getBaseSeq()+"-"+history.getLastSeq()+"]");
          return;
        }
         
        /* If the challenge asks for a snippet that starts with a checkpoint (FLAG_INCLUDE_CHECKPOINT set),
           we look up the last such entry; otherwise we start at the specified sequence number */
        
        short[] chkpointTypes = new short[]{ EVT_CHECKPOINT, EVT_INIT };
        long idxFrom = ((flags & FLAG_INCLUDE_CHECKPOINT) == FLAG_INCLUDE_CHECKPOINT) ? history.findLastEntry(chkpointTypes, seqFrom) : history.findSeq(seqFrom);
        long idxTo = history.findSeq(seqTo);
  
        if ((idxFrom >= 0) && (idxTo >= 0)) {
          IndexEntry fromEntry = history.statEntry(idxFrom);
          if (fromEntry == null) throw new RuntimeException("Cannot get beginSeq during AUDIT challenge");
          short beginType = fromEntry.getType();
          long beginSeq = fromEntry.getSeq();
//            
          /* Log entries with consecutive sequence numbers correspond to events that have happened
             'at the same time' (i.e. without updating the clock). In order to be able to replay
             this properly, we need to start at the first such event, which we locate by rounding
             down to the closest multiple of 1000. */
            
          if (((beginSeq % 1000000) > 0) && !((flags & FLAG_INCLUDE_CHECKPOINT) == FLAG_INCLUDE_CHECKPOINT)) {
            beginSeq -= (beginSeq % 1000000);
            idxFrom = history.findSeq(beginSeq);
            if (logger.level <= Logger.FINEST) logger.log("Moving beginSeq to "+beginSeq+" (idx="+idxFrom+")");
            assert(idxFrom >= 0);
          }
          
          /* Similarly, we should always 'round up' to the next multiple of 1000 */
          
          long followingSeq;
          
          IndexEntry toEntry;
          while ((toEntry = history.statEntry(idxTo+1)) != null) {
            followingSeq = toEntry.getSeq();
            if ((followingSeq % 1000000) == 0)
              break;
            
            idxTo++;
            if (logger.level <= Logger.FINEST) logger.log( "Advancing endSeq past "+followingSeq+" (idx="+idxTo+")");
          }
                    
//          unsigned char endType;
          if ((toEntry = history.statEntry(idxTo)) == null) { 
            throw new RuntimeException("Cannot get endType during AUDIT challenge "+idxTo);
          }
          short endType = toEntry.getType();
          if (endType == EVT_RECV) {
            idxTo++;
          }
  
          /* Serialize the requested log snippet */
        
          HashPolicy hashPolicy = new ChallengeHashPolicy<Identifier>(flags, challenge.originator, peerreview.getIdSerializer());
//          SimpleOutputBuffer sob = new SimpleOutputBuffer();
          LogSnippet snippit;
          if ((snippit = history.serializeRange(idxFrom, idxTo, hashPolicy)) != null) {
//            ByteBuffer buf = sob.getByteBuffer();
//            int size = buf.remaining();
            /* Put together a RESPONSE message */
            ResponseMessage<Identifier> response = new ResponseMessage<Identifier>(
                challenge.originator,peerreview.getLocalId(),challenge.evidenceSeq,new AuditResponse<Handle>(peerreview.getLocalHandle(),snippit));
            
            /* ... and send it back to the challenger */
  
            if (logger.level <= Logger.FINER) logger.log("Answering AUDIT challenge with "+snippit.entries.size()+"-entry log snippet"); 
            peerreview.transmit(source, response, null, options);
          } else {
            if (logger.level <= Logger.WARNING) logger.log("Error accessing history in handleChallenge("+source+","+challenge+")");
          }
        } else {
          if (logger.level <= Logger.WARNING) logger.log(
              "Cannot respond to AUDIT challenge ["+seqFrom+"-"+seqTo+",flags="+flags+
              "]; entries not found (iF="+idxFrom+"/iT="+idxTo+")");
        }
        break;
      }
      
      /* SEND challenges: We accept the message if necessary and then respond with an ACK. At this point,
         the statement protocol has already checked the signature and filed the authenticator. */
      
      case CHAL_SEND:
      {
        UserDataMessage<Handle> udm = (UserDataMessage<Handle>)challenge.getChallenge();        

        if (logger.level <= Logger.INFO) logger.log( "Received a SEND challenge");

        Tuple<AckMessage<Identifier>, Boolean> ret = commitmentProtocol.logMessageIfNew(udm);
        AckMessage<Identifier> response = ret.a();
        boolean loggedPreviously = ret.b();
        
        /* We ONLY deliver the message to the application if we haven't done so
           already (i.e. if it was logged previously) */
          
        if (!loggedPreviously) {
          if (logger.level <= Logger.FINER) logger.log( "Delivering message in CHAL_SEND "+udm);
          peerreview.getApp().messageReceived(udm.getSenderHandle(), udm.getPayload(), options);
        }
  
        /* Put together a RESPONSE with an authenticator for the message in the challenge */

        ResponseMessage<Identifier> rMsg = new ResponseMessage<Identifier>(challenge.originator,peerreview.getLocalId(),challenge.evidenceSeq,ret.a());

        /* ... and send it back to the challenger */
        
        if (logger.level <= Logger.FINER) logger.log( "Returning a  response");
        peerreview.transmit(source, rMsg, null, options);        
        break;
      }
      
      /* These are all the challenges we know */
      
      default:
      {
        if (logger.level <= Logger.WARNING) logger.log("Unknown challenge #"+type+" from "+source);
        break;
      }
    }

  }

  /* Called when we've challenged another node, and it has sent us a response */

  protected void handleResponse(ResponseMessage<Identifier> message, Map<String, Object> options) throws IOException {
    /* If this is a response to an AUDIT, we let the AuditProtocol handle it */

    if (message.originator.equals(peerreview.getLocalId())) {
      Evidence auditEvidence = auditProtocol.statOngoingAudit(message.subject, message.evidenceSeq); 
      if (auditEvidence != null) {
        if (isValidResponse(message.subject, auditEvidence, message.evidence, true)) {
          if (logger.level <= Logger.FINE) logger.log( "Received response to ongoing AUDIT from "+message.subject);
          auditProtocol.processAuditResponse(message.subject, message.evidenceSeq, (AuditResponse<Handle>)message.evidence);
        } else {
          if (logger.level <= Logger.WARNING) logger.log("Invalid response to ongoing audit of "+message.subject);
        }
        
        return;
      }
    }
    
    /* It's not an AUDIT, so let's see whether it matches any of our evidence (if not, the
       sender is responding to a challenge we haven't even made) */
    
//    int evidenceLen = 0;
//    bool isProof = false;
//    bool haveResponse = false;
    Handle interestedParty = null;
    
    EvidenceRecord<Handle, Identifier> record = infoStore.findEvidence(message.originator, message.subject, message.evidenceSeq);
    interestedParty = record.getInterestedParty();
    if (record == null) {
      if (logger.level <= Logger.WARNING) logger.log("Received response, but matching request is missing; discarding");
      return;
    }
    
    /* The evidence has to be a CHALLENGE; for PROOFs there is no valid response */
    
    if (record.isProof()) {
      if (logger.level <= Logger.WARNING) logger.log("Received an alleged response to a proof; discarding");
      return;
    }
    
    /* If we get a duplicate response, discard it */
    
    if (record.hasResponse()) {
      if (logger.level <= Logger.WARNING) logger.log("Received duplicate response; discarding");
      return;
    }
    
    /* Retrieve the evidence */
    try {
      Evidence evidence = infoStore.getEvidence(message.originator, message.subject, message.evidenceSeq);
      
      /* Check the response against the evidence; if it is valid, add it to our store */
      
      if (isValidResponse(message.subject, evidence, message.evidence)) {
        if (logger.level <= Logger.FINE) logger.log(
            "Received valid response (orig="+message.originator+", subject="+message.subject+", t="+message.evidenceSeq+"); adding");
        infoStore.addResponse(message.originator, message.subject, message.evidenceSeq, message.evidence);
        
        /* If we've only relayed this challenge for another node (probably in our
           capacity as a witness), we need to forward the response back to
           the original challenger. */
        
        if (interestedParty != null) {
          if (logger.level <= Logger.FINE) logger.log("Relaying response to interested party "+interestedParty);
          peerreview.transmit(interestedParty, message, null, options);
        }
      } else {
        if (logger.level <= Logger.WARNING) logger.log("Invalid response; discarding");
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  boolean isValidResponse(Identifier subject, Evidence evidence, Evidence response) throws IOException {
    return isValidResponse(subject, evidence, response, false);
  }
  
  /**
   * This method takes a challenge and a response, and it checks whether the
   * response is valid, i.e. is well-formed and answers the challenge.
   */
  boolean isValidResponse(Identifier subject, Evidence evidence, Evidence response, boolean extractAuthsFromResponse) {
    if (evidence.getEvidenceType() != response.getEvidenceType()) {
      return false;
    }
    
    switch(evidence.getEvidenceType()) {
    case CHAL_AUDIT: {
      ChallengeAudit evidenceAudit = (ChallengeAudit)evidence;
      AuditResponse<Handle> responseAudit = (AuditResponse<Handle>)response;
      long requestedBeginSeq = evidenceAudit.from.getSeq();
      long finalSeq = evidenceAudit.to.getSeq();
      boolean includePrevCheckpoint = evidenceAudit.isIncludePrevCheckpoint();

      long firstSeq = responseAudit.getFirstSeq();
//      int readptr = 0;
//      readByte(response, (unsigned int*)&readptr); /* RESP_AUDIT */
//      Handle subjectHandle = peerreview->readNodeHandle(response, (unsigned int*)&readptr, responseLen);
//      long firstSeq = readLongLong(response, (unsigned int*)&readptr);
//      readptr += 1 + response[readptr]; /* skip extInfo */
//      unsigned char baseHash[hashSizeBytes];
//      memcpy(baseHash, &response[readptr], hashSizeBytes);
//      readptr += hashSizeBytes;
//
//      unsigned char subjectHandleInBytes[MAX_HANDLE_SIZE];
//      unsigned int subjectHandleInBytesLen = 0;
//      subjectHandle->write(subjectHandleInBytes, &subjectHandleInBytesLen, sizeof(subjectHandleInBytes));
//
      if ((requestedBeginSeq % 1000000) > 0) {
        requestedBeginSeq -= requestedBeginSeq % 1000000;
      }

      long fromNodeMaxSeq = requestedBeginSeq + 999999;
//
      if ((firstSeq > requestedBeginSeq) || (!includePrevCheckpoint && (firstSeq < requestedBeginSeq))) {
        if (logger.level <= Logger.WARNING) logger.log("Log snippet starts at "+firstSeq+", but we asked for "+requestedBeginSeq+" (ilc="+(includePrevCheckpoint ? "yes" : "no")+"); flagging invalid");
        return false;
      }

      boolean snippetOK = peerreview.getEvidenceTool().checkSnippetSignatures(
          responseAudit.getLogSnippet(), responseAudit.getLogOwner(), extractAuthsFromResponse ? authOutStore : null,
              evidenceAudit.flags,commitmentProtocol,evidenceAudit.from.getHash(),fromNodeMaxSeq); 
      //&response[readptr], responseLen - readptr, firstSeq, baseHash, subjectHandle, extractAuthsFromResponse ? authOutStore : NULL, 
      // includePrevCheckpoint, peerreview, peerreview, commitmentProtocol, &evidence[2+sizeof(long long)], fromNodeMaxSeq);

      if (!snippetOK)
        return false;
      
      break;
    }
    case CHAL_SEND: {
      UserDataMessage<Handle> evidenceUDM = (UserDataMessage<Handle>)evidence;
      long senderSeq = evidenceUDM.getTopSeq(); //readLongLong(evidence, &pos);
      Handle senderHandle = evidenceUDM.getSenderHandle(); //peerreview->readNodeHandle(evidence, &pos, evidenceLen);
      byte[] senderHtopMinusOne = evidenceUDM.getHTopMinusOne();
      byte[] senderSignature = evidenceUDM.getSignature();      
//      byte relevantCode = evidenceUDM.getRelevantCode(); //readByte(evidence, &pos);
      int relevantLen = evidenceUDM.getRelevantLen(); 
      
      AckMessage<Identifier> responseAck = (AckMessage<Identifier>)response;
      
//      unsigned int pos2 = 0;
//      readByte(response, &pos2); // CHAL_SEND
      Identifier receiverID = responseAck.getNodeId(); //peerreview->readIdentifier(response, &pos2, responseLen);
      long ackSenderSeq = responseAck.getSendEntrySeq();
      long ackReceiverSeq = responseAck.getRecvEntrySeq();
      byte[] receiverHtopMinusOne = responseAck.getHashTopMinusOne();
      byte[] receiverSignature = responseAck.getSignature();
      
      boolean okay = true;
      if (ackSenderSeq != senderSeq) {
        if (logger.level <= Logger.WARNING) logger.log(
            "RESP.SEND: ACK contains sender seq "+ackSenderSeq+", but challenge contains "+senderSeq+"; flagging invalid");
        okay = false;
      }

      if (okay) {      
        byte[] innerHash = evidenceUDM.getInnerHash(transport);
        
        //unsigned char authenticator[sizeof(long long) + hashSizeBytes + signatureSizeBytes];
        Authenticator authenticator = peerreview.extractAuthenticator(receiverID, ackReceiverSeq, EVT_RECV, innerHash, receiverHtopMinusOne, receiverSignature);
        if (authenticator != null) {
          if (logger.level <= Logger.FINE) logger.log( "Auth OK");
        } else {
          if (logger.level <= Logger.WARNING) logger.log("RESP.SEND: Signature on ACK is invalid");
          okay = false;
        }
      }
      
      if (!okay)
        return false;

      break;
    }
    default:
      if (logger.level <= Logger.WARNING) logger.log("Cannot check whether response type #"+evidence.getEvidenceType()+" is valid; answering no");
      throw new RuntimeException("Cannot check whether response type #"+evidence.getEvidenceType()+" is valid; answering no");
    }
    return true;
  }

  /**
   *  Handle an incoming RESPONSE or ACCUSATION from another node 
   * @throws IOException */

  public void handleStatement(Handle source, PeerReviewMessage m, Map<String, Object> options) throws IOException {
    assert(m.getType() == MSG_ACCUSATION || m.getType() == MSG_RESPONSE);

//    unsigned int pos = 1;
//    Identifier originator = peerreview->readIdentifier(statement, &pos, statementLen);
//    Identifier subject = peerreview->readIdentifier(statement, &pos, statementLen);
//    long long evidenceSeq = readLongLong(statement, &pos);
//    unsigned char *payload = &statement[pos];
//    int payloadLen = statementLen - pos;

    /* We get called from the StatementProtocol, so we already know that the statement
       is well-formed, and that we have acquired all the necessary supplementary material,
       such as certificates or hash maps */
       
    switch(m.getType()) {
    case MSG_RESPONSE: {
        ResponseMessage<Identifier> message = (ResponseMessage<Identifier>)m;
        if (logger.level <= Logger.FINE) 
          logger.log("Statement completed: RESPONSE  (orig="+message.originator+
              ", subject="+message.subject+", ts="+message.evidenceSeq+")");
        handleResponse(message, options);
        if (infoStore.getStatus(peerreview.getIdentifierExtractor().extractIdentifier(source)) == STATUS_SUSPECTED) {
          if (logger.level <= Logger.FINE) logger.log( "RECHALLENGE "+source);
          challengeSuspectedNode(source);
        }
        return;
      }
    case MSG_ACCUSATION:
      AccusationMessage<Identifier> message = (AccusationMessage<Identifier>)m;
      if (logger.level <= Logger.FINE) 
        logger.log("Statement completed: ACCUSATION  (orig="+message.originator+
            ", subject="+message.subject+", ts="+message.evidenceSeq+")");
       
      int status = infoStore.getStatus(peerreview.getIdentifierExtractor().extractIdentifier(source));
        switch (status) {
        case STATUS_EXPOSED:
          if (logger.level <= Logger.FINE) logger.log( "Got an accusation from exposed node "+source+"; discarding");
          break;
        case STATUS_TRUSTED:
          try {
            Evidence foo = infoStore.getEvidence(message.originator, message.subject, message.evidenceSeq);
            if (foo == null) {
              infoStore.addEvidence(message.originator, message.subject, message.evidenceSeq, message.evidence, source);
            } else {
              if (logger.level <= Logger.FINE) logger.log( "We already have a copy of that challenge; discarding");
            }
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          break;
        case STATUS_SUSPECTED:
        {
          /* If the status is SUSPECTED, we queue the message for later delivery */
  
          if (logger.level <= Logger.WARNING) logger.log("Incoming accusation from SUSPECTED node "+source+"; queueing and challenging the node");
          copyAndEnqueueTail(source, message.evidence, true, message.subject, message.originator, message.evidenceSeq, options);
  
          /* Furthermore, we must have an unanswered challenge, which we send to the remote node */
  
          challengeSuspectedNode(source);
          break;
        }
        default:
          throw new RuntimeException("Unknown status: #"+status);
      }
    }
  }
  
  
  /**
   * Looks up the first unanswered challenge to a SUSPECTED node, and sends it to that node 
   */
  public void challengeSuspectedNode(Handle target) {    
//    Identifier originator;
//    long evidenceSeq;
//    int evidenceLen;
    
    Identifier tIdentifier = peerreview.getIdentifierExtractor().extractIdentifier(target);
    EvidenceRecord<Handle, Identifier> record = infoStore.statFirstUnansweredChallenge(tIdentifier);
    if (record == null) {
      throw new RuntimeException("Node "+target+" is SUSPECTED, but I cannot retrieve an unanswered challenge?!?");      
    }
    
    try {
      /* Construct a CHALLENGE message ... */
      ChallengeMessage<Identifier> challenge = 
        new ChallengeMessage<Identifier>(
            record.getOriginator(),record.getTimeStamp(),
            infoStore.getEvidence(record.getOriginator(), tIdentifier, record.getTimeStamp())
            );
      /* ... and send it */
      peerreview.transmit(target, challenge, null, null);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }        
  }
  
  public void handleIncomingMessage(Handle source, UserDataMessage<Handle> message, Map<String, Object> options) throws IOException {
    int status = infoStore.getStatus(peerreview.getIdentifierExtractor().extractIdentifier(source));
    switch (status) {
      case STATUS_EXPOSED:
        if (logger.level <= Logger.FINE) logger.log("Got a user message from exposed node "+source+"; discarding");
        return;
      case STATUS_TRUSTED:
        commitmentProtocol.handleIncomingMessage(source, message, options);
        return;
    }

    assert(status == STATUS_SUSPECTED);
    
    /* If the status is SUSPECTED, we queue the message for later delivery */

    if (logger.level <= Logger.WARNING) logger.log("Incoming message from SUSPECTED node "+source+"; queueing and challenging the node");
    copyAndEnqueueTail(source, message, false, null, null, 0, options);

    /* Furthermore, we must have an unanswered challenge, which we send to the remote node */

    challengeSuspectedNode(source);
  }


}
