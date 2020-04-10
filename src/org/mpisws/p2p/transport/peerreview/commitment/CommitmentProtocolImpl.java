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
package org.mpisws.p2p.transport.peerreview.commitment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SignatureException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.history.HashSeq;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtAck;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtRecv;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSend;
import org.mpisws.p2p.transport.peerreview.history.logentry.EvtSign;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.OutgoingUserDataMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.tuples.Tuple;
import rice.selector.TimerTask;

public class CommitmentProtocolImpl<Handle extends RawSerializable, Identifier extends RawSerializable> implements 
    CommitmentProtocol<Handle, Identifier>, PeerReviewConstants {
  public int MAX_PEERS = 250;
  public int INITIAL_TIMEOUT_MILLIS = 1000;
  public int RETRANSMIT_TIMEOUT_MILLIS = 1000;
  public int RECEIVE_CACHE_SIZE = 100;
  public int MAX_RETRANSMISSIONS = 2;
  public int TI_PROGRESS = 1;
  public int PROGRESS_INTERVAL_MILLIS = 1000;
  public int MAX_ENTRIES_PER_MS = 1000000;      /* Max number of entries per millisecond */

  /**
   * We need to keep some state for each peer, including separate transmit and
   * receive queues
   */
  Map<Identifier, PeerInfo<Handle>> peer = new HashMap<Identifier, PeerInfo<Handle>>();
  
  /**
   * We cache a few recently received messages, so we can recognize duplicates.
   * We also remember the location of the corresponding RECV entry in the log,
   * so we can reproduce the matching acknowledgment
   */
  Map<Tuple<Identifier, Long>, ReceiveInfo<Identifier>> receiveCache;
  
  AuthenticatorStore<Identifier> authStore;
  SecureHistory history;
  PeerReview<Handle, Identifier> peerreview;
  PeerInfoStore<Handle, Identifier> infoStore;
  IdentityTransport<Handle, Identifier> transport;
  
  /**
   * If the time is more different than this from a peer, we discard the message
   */
  long timeToleranceMillis;
  int nextReceiveCacheEntry;
  int signatureSizeBytes;
  int hashSizeBytes;
  
  TimerTask makeProgressTask;
  Logger logger;
  
  public CommitmentProtocolImpl(PeerReview<Handle,Identifier> peerreview,
      IdentityTransport<Handle, Identifier> transport,
      PeerInfoStore<Handle, Identifier> infoStore, AuthenticatorStore<Identifier> authStore,
      SecureHistory history,
      long timeToleranceMillis) throws IOException {
    this.peerreview = peerreview;
    this.transport = transport;
    this.infoStore = infoStore;
    this.authStore = authStore;
    this.history = history;
    this.nextReceiveCacheEntry = 0;
//    this.numPeers = 0;
    this.timeToleranceMillis = timeToleranceMillis;
    
    this.logger = peerreview.getEnvironment().getLogManager().getLogger(CommitmentProtocolImpl.class, null);

    initReceiveCache();
    makeProgressTask = new TimerTask(){    
      @Override
      public void run() {        
        makeProgressAllPeers();
      }    
    };
    
    peerreview.getEnvironment().getSelectorManager().schedule(makeProgressTask, PROGRESS_INTERVAL_MILLIS, PROGRESS_INTERVAL_MILLIS);    
  }
  
  /**
   * Load the last events from the history into the cache
   */
  protected void initReceiveCache() throws IOException {
    receiveCache = new LinkedHashMap<Tuple<Identifier, Long>, ReceiveInfo<Identifier>>(RECEIVE_CACHE_SIZE, 0.75f, true) {
      protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > RECEIVE_CACHE_SIZE;
      }
    };
    
    for (long i=history.getNumEntries()-1; (i>=1) && (receiveCache.size() < RECEIVE_CACHE_SIZE); i--) {
      IndexEntry hIndex = history.statEntry(i);
      if (hIndex.getType() == PeerReviewConstants.EVT_RECV) {
        // NOTE: this could be more efficient, because we don't need the whole thing
        SimpleInputBuffer sib = new SimpleInputBuffer(history.getEntry(hIndex, hIndex.getSizeInFile()));
        Identifier thisSender = peerreview.getIdSerializer().deserialize(sib);
        
        // NOTE: the message better start with the sender seq, but this is done within this protocol
        addToReceiveCache(thisSender, sib.readLong(), i);
      }
    }
  }

  protected void addToReceiveCache(Identifier id, long senderSeq, long indexInLocalHistory) {
    receiveCache.put(new Tuple<Identifier, Long>(id,senderSeq), new ReceiveInfo<Identifier>(id, senderSeq, indexInLocalHistory));
  }
  
  protected PeerInfo<Handle> lookupPeer(Handle handle) {
    PeerInfo<Handle> ret = peer.get(peerreview.getIdentifierExtractor().extractIdentifier(handle));
    if (ret != null) return ret;
    
    ret = new PeerInfo<Handle>(handle); 
    peer.put(peerreview.getIdentifierExtractor().extractIdentifier(handle), ret);
    return ret;
  }
  
  public void notifyCertificateAvailable(Identifier id) {
    makeProgress(id);
  }
  

  /**
   * Checks whether an incoming message is already in the log (which can happen with duplicates).
   * If not, it adds the message to the log. 
   * @return The ack message and whether it was already logged.
   * @throws SignatureException 
   */
  public Tuple<AckMessage<Identifier>,Boolean> logMessageIfNew(UserDataMessage<Handle> udm) {
    try {
      boolean loggedPreviously; // part of the return statement
      long seqOfRecvEntry;
      byte[] myHashTop;
      byte[] myHashTopMinusOne;
      
  //    SimpleInputBuffer sib = new SimpleInputBuffer(message);
  //    UserDataMessage<Handle> udm = UserDataMessage.build(sib, peerreview.getHandleSerializer(), peerreview.getHashSizeInBytes(), peerreview.getSignatureSizeInBytes());
      
      /* Check whether the log contains a matching RECV entry, i.e. one with a message
      from the same node and with the same send sequence number */
  
      long indexOfRecvEntry = findRecvEntry(peerreview.getIdentifierExtractor().extractIdentifier(udm.getSenderHandle()), udm.getTopSeq());
  
      /* If there is no such RECV entry, we append one */
  
      if (indexOfRecvEntry < 0L) {
        /* Construct the RECV entry and append it to the log */
  
        myHashTopMinusOne = history.getTopLevelEntry().getHash();
        EvtRecv<Handle> recv = udm.getReceiveEvent(transport);
        history.appendEntry(EVT_RECV, true, recv.serialize());
              
        HashSeq foo = history.getTopLevelEntry();
        myHashTop = foo.getHash();
        seqOfRecvEntry = foo.getSeq();
        
        addToReceiveCache(peerreview.getIdentifierExtractor().extractIdentifier(udm.getSenderHandle()), 
            udm.getTopSeq(), history.getNumEntries() - 1);
        if (logger.level < Logger.FINE) logger.log("New message logged as seq#"+seqOfRecvEntry);
  
        /* Construct the SIGN entry and append it to the log */
        
        history.appendEntry(EVT_SIGN, true, new EvtSign(udm.getHTopMinusOne(),udm.getSignature()).serialize());
        loggedPreviously = false;
      } else {
        loggedPreviously = true;
        
        /* If the RECV entry already exists, retrieve it */
        
  //      unsigned char type;
  //      bool ok = true;
        IndexEntry i2 = history.statEntry(indexOfRecvEntry); //, &seqOfRecvEntry, &type, NULL, NULL, myHashTop);
        IndexEntry i1 = history.statEntry(indexOfRecvEntry-1); //, NULL, NULL, NULL, NULL, myHashTopMinusOne);
        assert(i1 != null && i2 != null && i2.getType() == EVT_RECV) : "i1:"+i1+" i2:"+i2;
        seqOfRecvEntry = i2.getSeq();
        myHashTop = i2.getNodeHash();
        myHashTopMinusOne = i1.getNodeHash();
        if (logger.level < Logger.FINE) logger.log("This message has already been logged as seq#"+seqOfRecvEntry);
      }
  
      /* Generate ACK = (MSG_ACK, myID, remoteSeq, localSeq, myTopMinusOne, signature) */
  
      byte[] hToSign = transport.hash(ByteBuffer.wrap(MathUtils.longToByteArray(seqOfRecvEntry)), ByteBuffer.wrap(myHashTop));
  
      AckMessage<Identifier> ack = new AckMessage<Identifier>(
          peerreview.getLocalId(),
          udm.getTopSeq(),
          seqOfRecvEntry,
          myHashTopMinusOne,
          transport.sign(hToSign));
      
      return new Tuple<AckMessage<Identifier>,Boolean>(ack, loggedPreviously);
    } catch (IOException ioe) {
      RuntimeException throwMe = new RuntimeException("Unexpect error logging message :"+udm);
      throwMe.initCause(ioe);
      throw throwMe;
    }
  }
  
  public void notifyStatusChange(Identifier id, int newStatus) {
    makeProgressAllPeers();
  }

  protected void makeProgressAllPeers() {
    for (Identifier i : peer.keySet()) {
      makeProgress(i);
    }
  }

  /**
   * Tries to make progress on the message queue of the specified peer, e.g. after that peer
   * has become TRUSTED, or after it has sent us an acknowledgment 
   */    
  protected void makeProgress(Identifier idx) {
//    logger.log("makeProgress("+idx+")");
    PeerInfo<Handle> info = peer.get(idx);
    if (info == null || (info.xmitQueue.isEmpty() && info.recvQueue.isEmpty())) {
      return;
    }
    
    /* Get the public key. If we don't have it (yet), ask the peer to send it */

    if (!transport.hasCertificate(idx)) {
      peerreview.requestCertificate(info.handle, idx);
      return;
    }

    /* Transmit queue: If the peer is suspected, challenge it; otherwise, send the next message
       or retransmit the one currently in flight */

    if (!info.xmitQueue.isEmpty()) {
      int status = infoStore.getStatus(idx);
      switch (status) {
        case STATUS_EXPOSED: /* Node is already exposed; no point in sending it any further messages */
          if (logger.level <= Logger.WARNING) logger.log("Releasing messages sent to exposed node "+idx);
          info.clearXmitQueue();
          return;
        case STATUS_SUSPECTED: /* Node is suspected; send the first unanswered challenge */
          if (info.lastChallenge < (peerreview.getTime() - info.currentChallengeInterval)) {
            if (logger.level <= Logger.WARNING) logger.log(
                "Pending message for SUSPECTED node "+info.getHandle()+"; challenging node (interval="+info.currentChallengeInterval+")");
            info.lastChallenge = peerreview.getTime();
            info.currentChallengeInterval *= 2;
            peerreview.challengeSuspectedNode(info.handle);
          }
          return;
        case STATUS_TRUSTED: /* Node is trusted; continue below */
          info.lastChallenge = -1;
          info.currentChallengeInterval = PeerInfo.INITIAL_CHALLENGE_INTERVAL_MICROS;
          break;
      }
    
      /* If there are no unacknowledged packets to that node, transmit the next packet */
    
      if (info.numOutstandingPackets == 0) {
        info.numOutstandingPackets++;
        info.lastTransmit = peerreview.getTime();
        info.currentTimeout = INITIAL_TIMEOUT_MILLIS;
        info.retransmitsSoFar = 0;
        OutgoingUserDataMessage<Handle> oudm = info.xmitQueue.getFirst();
//        try {
          peerreview.transmit(info.getHandle(), oudm, null, oudm.getOptions());
//        } catch (IOException ioe) {
//          info.xmitQueue.removeFirst();
//          oudm.sendFailed(ioe);
//          return;
//        }
      } else if (peerreview.getTime() > (info.lastTransmit + info.currentTimeout)) {
      
        /* Otherwise, retransmit the current packet a few times, up to the specified limit */
      
        if (info.retransmitsSoFar < MAX_RETRANSMISSIONS) {
          if (logger.level <= Logger.WARNING) logger.log(
              "Retransmitting a "+info.xmitQueue.getFirst().getPayload().remaining()+"-byte message to "+info.getHandle()+
              " (lastxmit="+info.lastTransmit+", timeout="+info.currentTimeout+", type="+
              info.xmitQueue.getFirst().getType()+")");
          info.retransmitsSoFar++;
          info.currentTimeout = RETRANSMIT_TIMEOUT_MILLIS;
          info.lastTransmit = peerreview.getTime();
          OutgoingUserDataMessage<Handle> oudm = info.xmitQueue.getFirst();
//          try {
            peerreview.transmit(info.handle, oudm, null, oudm.getOptions());
//          } catch (IOException ioe) {
//            info.xmitQueue.removeFirst();
//            oudm.sendFailed(ioe);
//            return;
//          }
        } else {
        
          /* If the peer still won't acknowledge the message, file a SEND challenge with its witnesses */
          
          if (logger.level <= Logger.WARNING) logger.log(info.handle+
              " has not acknowledged our message after "+info.retransmitsSoFar+
              " retransmissions; filing as evidence");
          OutgoingUserDataMessage<Handle> challenge = info.xmitQueue.removeFirst();
          challenge.sendFailed(new IOException("Peer Review Giving Up sending message to "+idx));
          
          long evidenceSeq = peerreview.getEvidenceSeq();

          try {
            infoStore.addEvidence(peerreview.getLocalId(), 
                peerreview.getIdentifierExtractor().extractIdentifier(info.handle), 
                evidenceSeq, challenge, null);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          peerreview.sendEvidenceToWitnesses(peerreview.getIdentifierExtractor().extractIdentifier(info.handle), 
              evidenceSeq, challenge);
          
          info.numOutstandingPackets --;
        }
      }
    }

    /* Receive queue */

    if (!info.recvQueue.isEmpty() && !info.isReceiving) {
      info.isReceiving = true;
    
      /* Dequeue the packet. After this point, we must either deliver it or discard it */

      Tuple<UserDataMessage<Handle>, Map<String, Object>> t = info.recvQueue.removeFirst();
      UserDataMessage<Handle> udm = t.a();
      
      /* Extract the authenticator */
      Authenticator authenticator;
      byte[] innerHash = udm.getInnerHash(peerreview.getLocalId(), transport);
    
      authenticator = peerreview.extractAuthenticator(
          peerreview.getIdentifierExtractor().extractIdentifier(udm.getSenderHandle()), 
          udm.getTopSeq(), 
          EVT_SEND, 
          innerHash, udm.getHTopMinusOne(), udm.getSignature());
//      logger.log("received message, extract auth from "+udm.getSenderHandle()+" seq:"+udm.getTopSeq()+" "+
//          MathUtils.toBase64(innerHash)+" htop-1:"+MathUtils.toBase64(udm.getHTopMinusOne())+" sig:"+MathUtils.toBase64(udm.getSignature()));
      if (authenticator != null) {

        /* At this point, we are convinced that:
              - The remote node is TRUSTED [TODO!!]
              - The message has an acceptable sequence number
              - The message is properly signed
           Now we must check our log for an existing RECV entry:
              - If we already have such an entry, we generate the ACK from there
              - If we do not yet have the entry, we log the message and deliver it  */

        Tuple<AckMessage<Identifier>, Boolean> ret = logMessageIfNew(udm);

        /* Since the message is not yet in the log, deliver it to the application */

        if (!ret.b()) {
          if (logger.level <= Logger.FINE) logger.log(
              "Delivering message from "+udm.getSenderHandle()+" via "+info.handle+" ("+
              udm.getPayloadLen()+" bytes; "+udm.getRelevantLen()+"/"+udm.getPayloadLen()+" relevant)");
          try {
            peerreview.getApp().messageReceived(udm.getSenderHandle(), udm.getPayload(), t.b()); 
          } catch (IOException ioe) {
            logger.logException("Error handling "+udm, ioe);
          }
        } else {
          if (logger.level <= Logger.FINE) logger.log(
              "Message from "+udm.getSenderHandle()+" via "+info.getHandle()+" was previously logged; not delivered");
        }

        /* Send the ACK */

        if (logger.level <= Logger.FINE) logger.log("Returning ACK to"+info.getHandle());
//        try {
          peerreview.transmit(info.handle, ret.a(), null, t.b());
//        } catch (IOException ioe) {
//          throw new RuntimeException("Major problem, ack couldn't be serialized." +ret.a(),ioe);
//        }
      } else {
        if (logger.level <= Logger.WARNING) logger.log("Cannot verify signature on message "+udm.getTopSeq()+" from "+info.getHandle()+"; discarding");
      }
      
      /* Release the message */

      info.isReceiving = false;
      makeProgress(idx);
    }
  }
  
  protected long findRecvEntry(Identifier id, long seq) {
    ReceiveInfo<Identifier> ret = receiveCache.get(new Tuple<Identifier, Long>(id,seq));
    if (ret == null) return -1;
    return ret.indexInLocalHistory;
  }
  
  protected long findAckEntry(Identifier id, long seq) {
    return -1;
  }
  
  /**
   * Handle an incoming USERDATA message 
   */
  public void handleIncomingMessage(Handle source, UserDataMessage<Handle> msg, Map<String, Object> options) throws IOException {
//    char buf1[256];    

    /* Check whether the timestamp (in the sequence number) is close enough to our local time.
       If not, the node may be trying to roll forward its clock, so we discard the message. */
    long txmit = (msg.getTopSeq() / MAX_ENTRIES_PER_MS);

    if ((txmit < (peerreview.getTime()-timeToleranceMillis)) || (txmit > (peerreview.getTime()+timeToleranceMillis))) {
      if (logger.level <= Logger.WARNING) logger.log("Invalid sequence no #"+msg.getTopSeq()+" on incoming message (dt="+(txmit-peerreview.getTime())+"); discarding");
      return;
    }

    /**
     * Append a copy of the message to our receive queue. If the node is
     * trusted, the message is going to be delivered directly by makeProgress();
     * otherwise a challenge is sent.
     */
    lookupPeer(source).recvQueue.addLast(new Tuple<UserDataMessage<Handle>, Map<String,Object>>(msg,options));

    makeProgress(peerreview.getIdentifierExtractor().extractIdentifier(source));
  }

  public MessageRequestHandle<Handle, ByteBuffer> handleOutgoingMessage(
      final Handle target, final ByteBuffer message, 
      MessageCallback<Handle, ByteBuffer> deliverAckToMe,
      final Map<String, Object> options) {
    int relevantlen = message.remaining();
    if (options != null && options.containsKey(PeerReview.RELEVANT_LENGTH)) {
      Number n = (Number)options.get(PeerReview.RELEVANT_LENGTH);
      relevantlen = n.intValue();      
    }
    assert(relevantlen >= 0);

    /* Append a SEND entry to our local log */
    
    byte[] hTopMinusOne, hTop, hToSign;
//    long topSeq;
    hTopMinusOne = history.getTopLevelEntry().getHash();
    EvtSend<Identifier> evtSend;
    if (relevantlen < message.remaining()) {      
      evtSend = new EvtSend<Identifier>(peerreview.getIdentifierExtractor().extractIdentifier(target),message,relevantlen,transport);
    } else {
      evtSend = new EvtSend<Identifier>(peerreview.getIdentifierExtractor().extractIdentifier(target),message);
    }
    

    try {      
//      logger.log("XXXa "+Arrays.toString(evtSend.serialize().array()));
      history.appendEntry(evtSend.getType(), true, evtSend.serialize());
    } catch (IOException ioe) {
      MessageRequestHandle<Handle, ByteBuffer> ret = new MessageRequestHandleImpl<Handle, ByteBuffer>(target,message,options);
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, ioe);
      return ret;
    }
    
    //  hTop, &topSeq
    HashSeq top = history.getTopLevelEntry();
    
    /* Sign the authenticator */
//    logger.log("about to sign: "+top.getSeq()+" "+MathUtils.toBase64(top.getHash()));
    hToSign = transport.hash(ByteBuffer.wrap(MathUtils.longToByteArray(top.getSeq())), ByteBuffer.wrap(top.getHash()));

    byte[] signature = transport.sign(hToSign);
    
    /* Append a SENDSIGN entry */
    
    ByteBuffer relevantMsg = message;
    if (relevantlen < message.remaining()) {
      relevantMsg = ByteBuffer.wrap(message.array(), message.position(), relevantlen);
    } else {
      relevantMsg = ByteBuffer.wrap(message.array(), message.position(), message.remaining());
    }
    try {
      history.appendEntry(EVT_SENDSIGN, true, relevantMsg, ByteBuffer.wrap(signature));
    } catch (IOException ioe) {
      MessageRequestHandle<Handle, ByteBuffer> ret = new MessageRequestHandleImpl<Handle, ByteBuffer>(target,message,options);
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(ret, ioe);
      return ret;
    }
    
    /* Construct a USERDATA message... */
    
    assert((relevantlen == message.remaining()) || (relevantlen < 255));    

    PeerInfo<Handle> pi = lookupPeer(target);

    OutgoingUserDataMessage<Handle> udm = new OutgoingUserDataMessage<Handle>(top.getSeq(), peerreview.getLocalHandle(), hTopMinusOne, signature, message, relevantlen, options, pi, deliverAckToMe);
    
    /* ... and put it into the send queue. If the node is trusted and does not have any
       unacknowledged messages, makeProgress() will simply send it out. */
    pi.xmitQueue.addLast(udm);
    makeProgress(peerreview.getIdentifierExtractor().extractIdentifier(target));
    
    return udm;
  }
  /* This is called if we receive an acknowledgment from another node */

  public void handleIncomingAck(Handle source, AckMessage<Identifier> ackMessage, Map<String, Object> options) throws IOException {
//  AckMessage<Identifier> ackMessage = AckMessage.build(sib, peerreview.getIdSerializer(), hasher.getHashSizeBytes(), transport.signatureSizeInBytes());

    /* Acknowledgment: Log it (if we don't have it already) and send the next message, if any */

    if (logger.level <= Logger.FINE) logger.log("Received an ACK from "+source);
    // TODO: check that ackMessage came from the source
        
    if (transport.hasCertificate(ackMessage.getNodeId())) {
      PeerInfo<Handle> p = lookupPeer(source);

      boolean checkAck = true;
      OutgoingUserDataMessage<Handle> udm = null;
      if (p.xmitQueue.isEmpty()) {
        checkAck = false;  // don't know why this happens, but maybe the ACK gets duplicated somehow
      } else {
        udm = p.xmitQueue.getFirst();
      }
      
      /* The ACK must acknowledge the sequence number of the packet that is currently
         at the head of the send queue */

      if (checkAck && ackMessage.getSendEntrySeq() == udm.getTopSeq()) {
        
        /* Now we're ready to check the signature */

        /* The peer will have logged a RECV entry, and the signature is calculated over that
        entry. To verify the signature, we must reconstruct that RECV entry locally */

        byte[] innerHash = udm.getInnerHash(transport);

        
        Authenticator authenticator = peerreview.extractAuthenticator(
            ackMessage.getNodeId(), ackMessage.getRecvEntrySeq(), EVT_RECV, innerHash, 
            ackMessage.getHashTopMinusOne(), ackMessage.getSignature());
        if (authenticator != null) {

          /* Signature is okay... append an ACK entry to the log */

          if (logger.level <= Logger.FINE) logger.log("ACK is okay; logging "+ackMessage);
          
          EvtAck<Identifier> evtAck = new EvtAck<Identifier>(ackMessage.getNodeId(), ackMessage.getSendEntrySeq(), ackMessage.getRecvEntrySeq(), ackMessage.getHashTopMinusOne(), ackMessage.getSignature());
          history.appendEntry(EVT_ACK, true, evtAck.serialize());
          udm.sendComplete(); //ackMessage.getSendEntrySeq());

          /* Remove the message from the xmit queue */

          p.xmitQueue.removeFirst();
          p.numOutstandingPackets--;

          /* Make progress (e.g. by sending the next message) */

          makeProgress(peerreview.getIdentifierExtractor().extractIdentifier(p.getHandle()));
        } else {
          if (logger.level <= Logger.WARNING) logger.log("Invalid ACK from <"+ackMessage.getNodeId()+">; discarding");
        }
      } else {
        if (findAckEntry(ackMessage.getNodeId(), ackMessage.getSendEntrySeq()) < 0) {
          if (logger.level <= Logger.WARNING) logger.log("<"+ackMessage.getNodeId()+"> has ACKed something we haven't sent ("+ackMessage.getSendEntrySeq()+"); discarding");
        } else {
          if (logger.level <= Logger.WARNING) logger.log("Duplicate ACK from <"+ackMessage.getNodeId()+">; discarding");
        }
      }
    } else {
      if (logger.level <= Logger.WARNING) logger.log("We got an ACK from <"+ackMessage.getNodeId()+">, but we don't have the certificate; discarding");     
    }
  }

  public void setTimeToleranceMillis(long timeToleranceMillis) {
    this.timeToleranceMillis = timeToleranceMillis;
  }
}
