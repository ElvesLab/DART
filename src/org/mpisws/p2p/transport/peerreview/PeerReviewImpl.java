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
package org.mpisws.p2p.transport.peerreview;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.peerreview.audit.AuditProtocol;
import org.mpisws.p2p.transport.peerreview.audit.AuditProtocolImpl;
import org.mpisws.p2p.transport.peerreview.audit.EvidenceTool;
import org.mpisws.p2p.transport.peerreview.audit.EvidenceToolImpl;
import org.mpisws.p2p.transport.peerreview.authpush.AuthenticatorPushProtocol;
import org.mpisws.p2p.transport.peerreview.authpush.AuthenticatorPushProtocolImpl;
import org.mpisws.p2p.transport.peerreview.challenge.ChallengeResponseProtocol;
import org.mpisws.p2p.transport.peerreview.challenge.ChallengeResponseProtocolImpl;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializerImpl;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStore;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStoreImpl;
import org.mpisws.p2p.transport.peerreview.commitment.CommitmentProtocol;
import org.mpisws.p2p.transport.peerreview.commitment.CommitmentProtocolImpl;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceSerializerImpl;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceTransferProtocol;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceTransferProtocolImpl;
import org.mpisws.p2p.transport.peerreview.evidence.ProofInconsistent;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.HashSeq;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactoryImpl;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransportCallback;
import org.mpisws.p2p.transport.peerreview.identity.UnknownCertificateException;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.EvidenceSerializer;
import org.mpisws.p2p.transport.peerreview.infostore.IdStrTranslator;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStoreImpl;
import org.mpisws.p2p.transport.peerreview.infostore.StatusChangeListener;
import org.mpisws.p2p.transport.peerreview.message.AccusationMessage;
import org.mpisws.p2p.transport.peerreview.message.AckMessage;
import org.mpisws.p2p.transport.peerreview.message.AuthPushMessage;
import org.mpisws.p2p.transport.peerreview.message.AuthRequest;
import org.mpisws.p2p.transport.peerreview.message.AuthResponse;
import org.mpisws.p2p.transport.peerreview.message.ChallengeMessage;
import org.mpisws.p2p.transport.peerreview.message.PeerReviewMessage;
import org.mpisws.p2p.transport.peerreview.message.ResponseMessage;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.peerreview.replay.VerifierFactory;
import org.mpisws.p2p.transport.peerreview.replay.VerifierFactoryImpl;
import org.mpisws.p2p.transport.peerreview.replay.record.RecordSM;
import org.mpisws.p2p.transport.peerreview.statement.Statement;
import org.mpisws.p2p.transport.peerreview.statement.StatementProtocolImpl;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.Serializer;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.random.RandomSource;
import rice.environment.random.simple.SimpleRandomSource;
import rice.environment.time.simulated.DirectTimeSource;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.selector.TimerTask;

/**
 * 
 * @author Jeff Hoye
 *
 * @param <Handle> (Usually a NodeHandle)
 * @param <Identifier> (Permanent Identifier), can get an Identifier from a Handle
 */
public class PeerReviewImpl<Handle extends RawSerializable, Identifier extends RawSerializable> implements 
    TransportLayerCallback<Handle, ByteBuffer>,
    PeerReview<Handle, Identifier>, StatusChangeListener<Identifier> {

  // above/below layers
  protected PeerReviewCallback<Handle, Identifier> callback;
  protected IdentityTransport<Handle, Identifier> transport;

  // strategies for management of Generics
  protected Serializer<Handle> handleSerializer;
  protected Serializer<Identifier> idSerializer;
  protected IdentifierExtractor<Handle, Identifier> identifierExtractor;
  protected IdStrTranslator<Identifier> stringTranslator;
  protected EvidenceSerializer evidenceSerializer;
  protected AuthenticatorSerializer authenticatorSerialilzer;
  protected EvidenceTool<Handle, Identifier> evidenceTool;
  
  // compatibility with rice environment
  protected Environment env;
  protected Logger logger;

  // storage
  protected AuthenticatorStore<Identifier> authInStore;
  protected AuthenticatorStore<Identifier> authOutStore;
  protected AuthenticatorStore<Identifier> authCacheStore;
  protected AuthenticatorStore<Identifier> authPendingStore;
  protected PeerInfoStore<Handle, Identifier> infoStore;
  protected SecureHistoryFactory historyFactory;
  protected SecureHistory history;
  protected VerifierFactory<Handle, Identifier> verifierFactory;

  // protocols
  protected CommitmentProtocol<Handle, Identifier> commitmentProtocol;
  protected EvidenceTransferProtocol<Handle, Identifier> evidenceTransferProtocol;
  protected AuthenticatorPushProtocol<Handle, Identifier> authPushProtocol;
  protected AuditProtocol<Handle, Identifier> auditProtocol;
  protected ChallengeResponseProtocol<Handle, Identifier> challengeProtocol;
  protected StatementProtocolImpl<Handle, Identifier> statementProtocol;

  long nextEvidenceSeq = 0L;
  private TimerTask maintenanceTask;
  private TimerTask authPushTask;
  private TimerTask checkpointTask;
  
  protected RandomSource random;

  public RandomSource getRandomSource() {
    return random;
  }
  
  long lastLogEntry = -1;
  boolean initialized = false;
  protected long timeToleranceMillis = DEFAULT_TIME_TOLERANCE_MILLIS;
  
  public PeerReviewImpl(IdentityTransport<Handle, Identifier> transport,
      Environment env, Serializer<Handle> handleSerializer,
      Serializer<Identifier> idSerializer,      
      IdentifierExtractor<Handle, Identifier> identifierExtractor,
      IdStrTranslator<Identifier> stringTranslator
//      AuthenticatorSerializer authenticatorSerialilzer,
//      EvidenceSerializer evidenceSerializer
      ) {
    super();
    if (!(env.getSelectorManager() instanceof RecordSM)) {
      throw new IllegalArgumentException("Environment.getSelectorManager() must return a RecordSM");
    }
    this.transport = transport;
    this.transport.setCallback(this);
    this.stringTranslator = stringTranslator;
    this.env = env;
    this.logger = env.getLogManager().getLogger(PeerReviewImpl.class, null);
    this.idSerializer = idSerializer;
    this.handleSerializer = handleSerializer;
    this.identifierExtractor = identifierExtractor;

    this.authenticatorSerialilzer = new AuthenticatorSerializerImpl(transport.getHashSizeBytes(),transport.getSignatureSizeBytes());
    this.evidenceSerializer = new EvidenceSerializerImpl<Handle, Identifier>(handleSerializer,
        idSerializer,transport.getHashSizeBytes(),transport.getSignatureSizeBytes());
    this.historyFactory = getSecureHistoryFactory(transport, env);
    random = new SimpleRandomSource(env.getLogManager(),"peerreview");
  }
  
  protected SecureHistoryFactory getSecureHistoryFactory(IdentityTransport<Handle, Identifier> transport, Environment env) {
    return new SecureHistoryFactoryImpl(transport, env);
  }
  
  /**
   * PeerReview checks the timestamps on messages against the local clock, and
   * ignores them if the timestamp is too far out of sync. The definition of
   * 'too far' can be controlled with this method.
   */
  public void setTimeToleranceMillis(long timeToleranceMicros) {
   this.timeToleranceMillis = timeToleranceMicros;
   if (commitmentProtocol != null)
     commitmentProtocol.setTimeToleranceMillis(timeToleranceMicros);
  }


  /* Gets a fresh, unique sequence number for evidence */
  public long getEvidenceSeq() {
    if (nextEvidenceSeq < getTime()) {
      nextEvidenceSeq = getTime();
    }
    return nextEvidenceSeq++;
  }

  /* PeerReview only updates its internal clock when it returns to the main loop, but not
  in between (e.g. while it is handling messages). When the clock needs to be
  updated, this function is called. */

  protected void updateLogTime() {
    if (logger.level <= Logger.FINEST) logger.log("updateLogTime()");
    long now = env.getTimeSource().currentTimeMillis();
  
    if (now > lastLogEntry) {
      if (!history.setNextSeq(now * 1000000))
        throw new RuntimeException("PeerReview: Cannot roll back history sequence number from "+history.getLastSeq()+" to "+now*1000000+"; did you change the local time?");
        
      lastLogEntry = now;
    }
  }

//  /* Called by applications to log some application-specific event, such as PAST_GET. */
//
//  void logEvent(short type, ByteBuffer entry) throws IOException {
//    assert(initialized && (type > EVT_MAX_RESERVED));
//    updateLogTime();
//    history.appendEntry(type, true, entry);   
//  }
//
//  /* Called internally to log events */
//
//  void logEventInternal(short type, ByteBuffer entry) throws IOException {
//    assert(initialized && (type <= EVT_MAX_RESERVED));
//    updateLogTime();
//    history.appendEntry(type, true, entry);   
//  }
  
  public MessageRequestHandle<Handle, ByteBuffer> sendMessage(Handle target,
      ByteBuffer message,
      final MessageCallback<Handle, ByteBuffer> deliverAckToMe,
      Map<String, Object> options) {
    
    /*
     * If the 'DON_TCOMMIT' flag is set, the message is passed through to the
     * transport layer. This is used e.g. for liveness/proximity pings in
     * Pastry.
     */
    if (options != null && options.containsKey(DONT_COMMIT)) {
      final MessageRequestHandleImpl<Handle, ByteBuffer> ret = new MessageRequestHandleImpl<Handle, ByteBuffer>(
          target, message, options);
      ByteBuffer msg = ByteBuffer.allocate(message.remaining() + 1);
      msg.put(PEER_REVIEW_PASSTHROUGH);
      msg.put(message);
      msg.flip();
      ret.setSubCancellable(transport.sendMessage(target, msg,
          new MessageCallback<Handle, ByteBuffer>() {

            public void ack(MessageRequestHandle<Handle, ByteBuffer> msg) {
              if (deliverAckToMe != null)
                deliverAckToMe.ack(ret);
            }

            public void sendFailed(
                MessageRequestHandle<Handle, ByteBuffer> msg, Exception reason) {
              if (deliverAckToMe != null)
                deliverAckToMe.sendFailed(ret, reason);
            }
          }, options));
      return ret;
    }

    assert(initialized);

    /* Maybe do some mischief for testing? */

    // if (misbehavior)
    // misbehavior.maybeTamperWithData((unsigned char*)message, msglen);
    updateLogTime();

    /* Pass the message to the Commitment protocol */
    return commitmentProtocol.handleOutgoingMessage(target, message,
        deliverAckToMe, options);
  }
  
  public void setApp(PeerReviewCallback<Handle, Identifier> callback) {
    if (logger.level <= Logger.INFO) logger.log("setApp("+callback+")");
    this.callback = callback;
  }
  
  /**
   * 
   */
  public void setCallback(TransportLayerCallback<Handle, ByteBuffer> callback) {
    setApp((PeerReviewCallback<Handle, Identifier>)callback);
  }

  public void messageReceived(Handle handle, ByteBuffer message, Map<String, Object> options) throws IOException {
    
    assert(initialized);
    
    if (infoStore.getStatus(identifierExtractor.extractIdentifier(handle)) == STATUS_EXPOSED) {
      if (logger.level <= Logger.WARNING) logger.log("Received a message from an exposed node "+handle+" -- ignoring");
      return;
    }

    
    /* Maybe do some mischief for testing */
    
    /* Deliver datagrams */
    byte passthrough = message.get();    
    switch(passthrough) {
    case PEER_REVIEW_PASSTHROUGH:
      callback.messageReceived(handle, message, options);
      break;      
    case PEER_REVIEW_COMMIT:
      Statement<Identifier> m = null;
      updateLogTime();
      byte type = message.get();      
      SimpleInputBuffer sib = new SimpleInputBuffer(message);
      switch (type) {      
      case MSG_AUTHPUSH :
        authPushProtocol.handleIncomingAuthenticators(handle, AuthPushMessage.build(new SimpleInputBuffer(message),idSerializer,authenticatorSerialilzer));
        break;
      case MSG_AUTHREQ :
        auditProtocol.handleIncomingDatagram(handle, new AuthRequest<Identifier>(new SimpleInputBuffer(message),idSerializer));      
        break;
      case MSG_AUTHRESP :      
        auditProtocol.handleIncomingDatagram(handle, new AuthResponse<Identifier>(new SimpleInputBuffer(message),idSerializer,transport.getHashSizeBytes(),transport.getSignatureSizeBytes()));
        break;
      case MSG_ACK:        
        commitmentProtocol.handleIncomingAck(handle, AckMessage.build(sib,idSerializer,transport.getHashSizeBytes(),transport.getSignatureSizeBytes()), options);
        break;
      case MSG_CHALLENGE:
        ChallengeMessage<Identifier> challenge = new ChallengeMessage<Identifier>(sib,idSerializer,evidenceSerializer);
        challengeProtocol.handleChallenge(handle, challenge, options);
        break;
      case MSG_ACCUSATION:        
        statementProtocol.handleIncomingStatement(handle, new AccusationMessage<Identifier>(sib, idSerializer, evidenceSerializer), options);
        break;
      case MSG_RESPONSE:
        statementProtocol.handleIncomingStatement(handle, new ResponseMessage<Identifier>(sib, idSerializer, evidenceSerializer), options);
        break;
      case MSG_USERDATA:
        UserDataMessage<Handle> udm = UserDataMessage.build(sib, handleSerializer, transport.getHashSizeBytes(), transport.getSignatureSizeBytes());
        challengeProtocol.handleIncomingMessage(handle, udm, options);
  //      commitmentProtocol.handleIncomingMessage(handle, udm, options);
        break;
      default:
        throw new RuntimeException("Unknown message type in PeerReview: #"+ type);
      }
    }    
  }

  public static String getStatusString(int status) {
    switch(status) {
    case STATUS_EXPOSED:
      return "exposed";
    case STATUS_TRUSTED:
      return "trusted";
    case STATUS_SUSPECTED:
      return "suspected";
    }
    return "unknown status:"+status;
  }
  
  /**
   * Helper function called internally from the library. It takes a (potentially
   * new) authenticator and adds it to our local store if (a) it hasn't been
   * recorded before, and (b) its signature is valid.
   */
  public boolean addAuthenticatorIfValid(AuthenticatorStore<Identifier> store, Identifier subject, Authenticator auth) {
    // see if we can exit early
        
    Authenticator existingAuth = null;
    if (store != null) existingAuth = store.statAuthenticator(subject, auth.getSeq());
    if (existingAuth != null) {       
      /* If yes, then it should be bit-wise identical to the new one */
    
      if (auth.equals(existingAuth)) {
        return true;
      }
    }
   
     /* maybe the new authenticator is a forgery? Let's check the signature! 
        If the signature doesn't check out, then we simply discard the 'authenticator' and
        move on. */
     assert(transport.hasCertificate(subject));

     try {
//       System.out.println("Verifying "+auth.getSeq()+" "+MathUtils.toBase64(auth.getHash()));
       byte[] signedHash = transport.hash(auth.getPartToHashThenSign());
       int sigResult = transport.verify(subject, signedHash, auth.getSignature());
       assert((sigResult == SIGNATURE_OK) || sigResult == SIGNATURE_BAD);
       if (sigResult != SIGNATURE_OK) return false;
       
       if (!verify(subject,auth)) {
         return false; 
       }
//    char buf1[1000];
       
       /* Do we already have an authenticator with the same sequence number and from the same node? */
       if (existingAuth != null) {
         /* The signature checks out, so the node must have signed two different authenticators
         with the same sequence number! This is a proof of misbehavior, and we must
         notify the witness set! */
             if (logger.level < Logger.WARNING) logger.log("Authenticator conflict for "+subject+" seq #"+auth.getSeq());
             if (logger.level < Logger.FINE) logger.log("Existing: ["+existingAuth+"]");
             if (logger.level < Logger.FINE) logger.log("New:      ["+auth+"]");
             
             /**
              * PROOF_INCONSISTENT
              * byte type = PROOF_INCONSISTENT
              * authenticator auth1
              * char whichInconsistency   // 0=another auth, 1=a log snippet
              * -----------------------
              * authenticator auth2       // if whichInconsistency==0
              * -----------------------
              * long long firstSeq        // if whichInconsistency==1
              * hash baseHash
              * [entries]
              */
             ProofInconsistent proof = new ProofInconsistent(auth,existingAuth);
        long evidenceSeq = getEvidenceSeq();
        infoStore.addEvidence(identifierExtractor.extractIdentifier(transport.getLocalIdentifier()), subject, evidenceSeq, proof, null);
        sendEvidenceToWitnesses(subject, evidenceSeq, proof);
         return false;
       }
       
     
       /* We haven't seen this authenticator... Signature is ok, so we keep the new authenticator in our store. */  
       if (store != null) store.addAuthenticator(subject, auth);
       return true;       
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Cancellable requestCertificate(Handle source, Identifier certHolder, Continuation<X509Certificate, Exception> c, Map<String, Object> options) {
    return transport.requestCertificate(source, certHolder, c, options);
  }
  
  public Cancellable requestCertificate(final Handle source, final Identifier certHolder) {
    return transport.requestCertificate(source, certHolder, new Continuation<X509Certificate, Exception>() {

      public void receiveException(Exception exception) {
        if (logger.level <= Logger.WARNING) logger.logException("error receiving cert for "+certHolder+" from "+source, exception);
      }

      public void receiveResult(X509Certificate result) {
        notifyCertificateAvailable(certHolder);
      }        
    }, null);
  }

  public void notifyCertificateAvailable(Identifier id) {
    commitmentProtocol.notifyCertificateAvailable(id); 
    authPushProtocol.notifyCertificateAvailable(id);
    statementProtocol.notifyCertificateAvailable(id);
  }

  public void writeCheckpoint() throws IOException {
//    const int maxlen = 1048576*96;
//    unsigned char *buffer = (unsigned char*) malloc(maxlen);
    int size = 0;

//    if (prng != null)
//      size += prng->storeCheckpoint(&buffer[size], maxlen - size);
    SimpleOutputBuffer sob = new SimpleOutputBuffer();;
    callback.storeCheckpoint(sob); //&buffer[size], maxlen - size);

//    if ((size < 0) || (size >= maxlen))
//      panic("Cannot write checkpoint (size=%d)", size);

    updateLogTime();
    if (logger.level <= Logger.INFO) logger.log( "Writing checkpoint ("+sob.getWritten()+" bytes)");
    history.appendEntry(EVT_CHECKPOINT, true, sob.getByteBuffer());
  }  
  
  /**
   * Periodic timer for pushing batches of authenticators to the witnesses 
   */
  protected void doAuthPush() {
    if (logger.level <= Logger.INFO) logger.log("Doing authenticator push");
    authPushProtocol.push();
  }
  
  /**
   * Periodic maintenance timer; used to garbage-collect old authenticators 
   */
  protected void doMaintenance() {
    if (logger.level <= Logger.INFO) logger.log("Doing maintenance");
    try {
      authInStore.garbageCollect();
      authOutStore.garbageCollect();
      authPendingStore.garbageCollect();    
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  /**
   * Periodic timer for writing checkpoints 
   */
  protected void doCheckpoint() {
    HashSeq foo = history.getTopLevelEntry();
    long topSeq = foo.getSeq();
    try {
      long topIdx = history.findSeq(topSeq);
      if (history.statEntry(topIdx).getType() != EVT_CHECKPOINT) {
        writeCheckpoint();    
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Error during checkpoint",ioe);
    }
  }
  
  public void notifyStatusChange(final Identifier id, final int newStatus) {
//    char buf1[256];
    if (logger.level <= Logger.INFO) logger.log("Status change: <"+id+"> becomes "+getStatusString(newStatus));
//    logger.logException("Status change: <"+id+"> becomes "+getStatusString(newStatus),new Exception("Stack Trace"));
    challengeProtocol.notifyStatusChange(id, newStatus);
    commitmentProtocol.notifyStatusChange(id, newStatus);
    
    // let pr finish first
    env.getSelectorManager().schedule(new TimerTask() {    
      public void run() {
        callback.notifyStatusChange(id, newStatus);
      }
      public String toString() {
        return "NotifyStatusChangeTask: "+id+"=>"+newStatus;
      }
    }, 3);
    
  }
  
  public void init(String dirname) throws IOException {    
    assert(callback != null);
    File dir = new File(dirname);
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new IllegalStateException("Cannot open PeerReview directory: "+dir.getAbsolutePath());
      }
    }
    if (!dir.isDirectory()) throw new IllegalStateException("Cannot open PeerReview directory: "+dir.getAbsolutePath());
    
    File namebuf = new File(dir,"peers");
    
    infoStore = new PeerInfoStoreImpl<Handle, Identifier>(transport, stringTranslator, authenticatorSerialilzer, evidenceSerializer, env);
    infoStore.setStatusChangeListener(this);

    /* Open history */

    boolean newLogCreated = false;
    String historyName = dirname+"/local";
    
    this.history = historyFactory.open(historyName, "w");
    if (this.history == null) {
      this.history = historyFactory.create(historyName, 0, transport.getEmptyHash());      
      newLogCreated = true;
    }
    
    updateLogTime();
    
    if (!infoStore.setStorageDirectory(namebuf)) {
      throw new IllegalStateException("Cannot open info storage directory '"+namebuf+"'");
    }
    
    /* Initialize authenticator store */
    
    authInStore = new AuthenticatorStoreImpl<Identifier>(this);
    authInStore.setFilename(new File(dir,"authenticators.in"));

    authOutStore = new AuthenticatorStoreImpl<Identifier>(this);
    authOutStore.setFilename(new File(dir,"authenticators.out"));

    authPendingStore = new AuthenticatorStoreImpl<Identifier>(this, true);
    authPendingStore.setFilename(new File(dir,"authenticators.pending"));

    authCacheStore = new AuthenticatorStoreImpl<Identifier>(this, true);
    authCacheStore.setFilename(new File(dir,"authenticators.cache"));

    /* Remaining protocols */
    this.evidenceTransferProtocol = new EvidenceTransferProtocolImpl<Handle, Identifier>(this,transport,infoStore);
    this.commitmentProtocol = new CommitmentProtocolImpl<Handle, Identifier>(this,transport,infoStore,authOutStore,history, timeToleranceMillis);    
    this.authPushProtocol = new AuthenticatorPushProtocolImpl<Handle, Identifier>(this, authInStore, authOutStore, authPendingStore, transport, infoStore, evidenceTransferProtocol, env);
    this.auditProtocol = new AuditProtocolImpl<Handle, Identifier>(this, history, infoStore, authInStore, transport, authOutStore, evidenceTransferProtocol, authCacheStore);
    this.challengeProtocol = new ChallengeResponseProtocolImpl<Handle, Identifier>(this, transport, infoStore, history, authOutStore, auditProtocol, commitmentProtocol);
    this.statementProtocol = new StatementProtocolImpl<Handle, Identifier>(this, challengeProtocol, infoStore, transport);
    
    this.evidenceTool = new EvidenceToolImpl<Handle, Identifier>(this, handleSerializer, idSerializer, transport.getHashSizeBytes(), transport.getSignatureSizeBytes()); // TODO: implement
    this.verifierFactory = new VerifierFactoryImpl<Handle, Identifier>(this);
    
    initialized = true;

    maintenanceTask = env.getSelectorManager().schedule(new TimerTask() {    
      @Override
      public void run() {
        doMaintenance();
      }    
      public String toString() {
        return "DoMaintenanceTask";
      }
    },MAINTENANCE_INTERVAL_MILLIS, MAINTENANCE_INTERVAL_MILLIS);
    
    authPushTask = env.getSelectorManager().schedule(new TimerTask() {    
      @Override
      public void run() {
        doAuthPush();
      }    
      public String toString() {
        return "AuthPushTask";
      }
    },DEFAULT_AUTH_PUSH_INTERVAL_MILLIS, DEFAULT_AUTH_PUSH_INTERVAL_MILLIS);
    
    checkpointTask = env.getSelectorManager().schedule(new TimerTask() {    
      @Override
      public void run() {
        doCheckpoint();
      }    
      public String toString() {
        return "CheckpointTask";
      }
    },newLogCreated ? 1 : DEFAULT_CHECKPOINT_INTERVAL_MILLIS, DEFAULT_CHECKPOINT_INTERVAL_MILLIS);
        
    /* Append an INIT entry to the log */
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    transport.getLocalIdentifier().serialize(sob);
    history.appendEntry(EVT_INIT, true, sob.getByteBuffer());

    callback.init();
    writeCheckpoint();    
  }
    
  public PeerReviewCallback<Handle, Identifier> getApp() {
    return callback;
  }
  
  public SocketRequestHandle<Handle> openSocket(Handle i, SocketCallback<Handle> deliverSocketToMe, Map<String, Object> options) {
    return transport.openSocket(i, deliverSocketToMe, options);
  }

  public void incomingSocket(P2PSocket<Handle> s) throws IOException {
    callback.incomingSocket(s);
  }

//  public void panic(String s) {
//    if (logger.level <= Logger.SEVERE) logger.log("panic:"+s);
//    env.destroy();
//  }
  
  public void acceptMessages(boolean b) {
    transport.acceptMessages(b);
  }

  public void acceptSockets(boolean b) {
    transport.acceptSockets(b);
  }

  public Identifier getLocalId() {
    return identifierExtractor.extractIdentifier(transport.getLocalIdentifier());
  }
  
  public Handle getLocalHandle() {
    return transport.getLocalIdentifier();
  }

  public Handle getLocalIdentifier() {
    return transport.getLocalIdentifier();
  }

  public void setErrorHandler(ErrorHandler<Handle> handler) {
    // TODO Auto-generated method stub
    
  }

  public void destroy() {
    transport.destroy();
  }

  public AuthenticatorSerializer getAuthenticatorSerializer() {
    return authenticatorSerialilzer;
  }

  public Environment getEnvironment() {
    return env;
  }

  public Serializer<Identifier> getIdSerializer() {
    return idSerializer;
  }

  public long getTime() {
    return env.getTimeSource().currentTimeMillis();
  }
  
  /** 
   * A helper function that extracts an authenticator from an incoming message and adds it to our local store. 
   */
  public Authenticator extractAuthenticator(long seq, short entryType, byte[] entryHash, byte[] hTopMinusOne, byte[] signature) {
    byte[] hash = transport.hash(seq,entryType,hTopMinusOne, entryHash);
    Authenticator ret = new Authenticator(seq,hash,signature);    
    return ret;
  }
  
  public Authenticator extractAuthenticator(Identifier id, long seq, short entryType, byte[] entryHash, byte[] hTopMinusOne, byte[] signature) {
//    *(long long*)&authenticator[0] = seq;
    
//    byte[] hash = transport.hash(seq,entryType,hTopMinusOne, entryHash);
    Authenticator ret = extractAuthenticator(seq, entryType, entryHash, hTopMinusOne, signature); //new Authenticator(seq,hash,signature);
    if (addAuthenticatorIfValid(authOutStore, id, ret)) {
      return ret;
    }
    return null;
  }

  public Serializer<Handle> getHandleSerializer() {
    return handleSerializer;
  }

  public int getHashSizeInBytes() {
    return transport.getHashSizeBytes();
  }

  public int getSignatureSizeInBytes() {
    return transport.getSignatureSizeBytes();
  }

  public IdentifierExtractor<Handle, Identifier> getIdentifierExtractor() {
    return identifierExtractor;
  }

  public void challengeSuspectedNode(Handle handle) {
    challengeProtocol.challengeSuspectedNode(handle);
  }

  /**
   * Called internally by other classes if they have found evidence against one of our peers.
   * We ask the EvidenceTransferProtocol to send it to the corresponding witness set. 
   */
  public void sendEvidenceToWitnesses(Identifier subject, long evidenceSeq,
      Evidence evidence) {
    AccusationMessage<Identifier> accusation = new AccusationMessage<Identifier>(getLocalId(),subject,evidenceSeq,evidence);
   
    if (logger.level <= Logger.FINE) logger.log("Relaying evidence to <"+subject+">'s witnesses");
    evidenceTransferProtocol.sendMessageToWitnesses(subject, accusation, null, null);  
  }

  /**
   * Note, must include PEER_REVIEW_COMMIT and the type
   * 
   * @param dest
   * @param message
   * @param deliverAckToMe
   * @param options
   * @return
   */
  public void transmit(Handle dest, 
      PeerReviewMessage message,
      MessageCallback<Handle, ByteBuffer> deliverAckToMe, 
      Map<String, Object> options) {
    try {
      
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      sob.writeByte(PeerReview.PEER_REVIEW_COMMIT);
      sob.writeByte((byte)message.getType());
      message.serialize(sob);
      
      ByteBuffer buf = sob.getByteBuffer();
      
      transport.sendMessage(dest, buf, deliverAckToMe, options);
    } catch (IOException ioe) {
      throw new RuntimeException("Error serializing:"+message,ioe);
    }
  }

  public boolean hasCertificate(Identifier id) {
    return transport.hasCertificate(id);
  }

  public byte[] sign(byte[] bytes) {
    return transport.sign(bytes);
  }

  public short getSignatureSizeBytes() {
    return transport.getSignatureSizeBytes();
  }

  public boolean verify(Identifier id, Authenticator auth) {
    byte[] signedHash = transport.hash(auth.getPartToHashThenSign());
    int result = transport.verify(id, signedHash, auth.getSignature());
    return result == SIGNATURE_OK; 
  }
  
  public int verify(Identifier id, byte[] msg, byte[] signature) {
    return transport.verify(id, msg, signature);
  }

  public byte[] getEmptyHash() {
    return transport.getEmptyHash();
  }

  public short getHashSizeBytes() {
    return transport.getHashSizeBytes();
  }

  public byte[] hash(long seq, short type, byte[] nodeHash, byte[] contentHash) {
    return transport.hash(seq, type, nodeHash, contentHash);
  }

  public byte[] hash(ByteBuffer... hashMe) {
    return transport.hash(hashMe);
  }

  public EvidenceSerializer getEvidenceSerializer() {
    return evidenceSerializer;
  }

  public EvidenceTool<Handle, Identifier> getEvidenceTool() {
    return evidenceTool;
  }

  public SecureHistoryFactory getHistoryFactory() {
    return historyFactory;
  }
  
  public VerifierFactory<Handle, Identifier> getVerifierFactory() {
    return verifierFactory;
  }
  
//  void notifyWitnessesExt(int numSubjects, Identifier **subjects, int *witnessesPerSubject, Handle witnesses) {
//  protected void continuePush(Map<Identifier, Collection<Handle>> subjects) {
//    authPushProtocol.continuePush(subjects);
//  }
  
  void disableAuthenticatorProcessing() {
    assert(initialized);

    maintenanceTask.cancel();
    authInStore.disableMemoryBuffer();
    authOutStore.disableMemoryBuffer();
    authPendingStore.disableMemoryBuffer();
  }

  public SecureHistory getHistory() {
    return history;
  }

  public long getTimeToleranceMillis() {
    return timeToleranceMillis;
  }

  public void sendEvidence(Handle dest, Identifier evidenceAgainst) {
    evidenceTransferProtocol.sendEvidence(dest, evidenceAgainst);
  }
}
