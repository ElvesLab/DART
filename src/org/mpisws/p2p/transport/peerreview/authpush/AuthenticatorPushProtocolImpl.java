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
package org.mpisws.p2p.transport.peerreview.authpush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStore;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceTransferProtocol;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.message.AuthPushMessage;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * This protocol collects authenticators from incoming messages and, once in a
 * while, batches them together and sends them to the witnesses.
 */
public class AuthenticatorPushProtocolImpl<Handle extends RawSerializable, Identifier extends RawSerializable> implements AuthenticatorPushProtocol<Handle, Identifier> {

  static final int MAX_SUBJECTS_PER_WITNESS = 110;
  static final int MAX_WITNESSES_PER_SUBJECT = 110;
  /**
   * The MTU of the Datagram
   */
  static final int MSS = 1200;

  AuthenticatorStore<Identifier> authOutStore;
  AuthenticatorStore<Identifier> authInStore;
  AuthenticatorStore<Identifier> authPendingStore;
  PeerInfoStore<Handle, Identifier> infoStore;
  PeerReview<Handle, Identifier> peerreview;
  IdentityTransport<Handle, Identifier> transport;
  EvidenceTransferProtocol<Handle, Identifier> evidenceTransferProtocol;
  boolean probabilistic;
  double pXmit;
  Random random = new Random();

  Environment environment;
  Logger logger;
  
  public AuthenticatorPushProtocolImpl(
      PeerReview<Handle, Identifier> peerreview, AuthenticatorStore<Identifier> inStore, 
      AuthenticatorStore<Identifier> outStore, AuthenticatorStore<Identifier> pendingStore, 
      IdentityTransport<Handle, Identifier> transport, 
      PeerInfoStore<Handle, Identifier> infoStore,
      EvidenceTransferProtocol<Handle, Identifier> evidenceTransferProtocol, Environment env) {
    this.authInStore = inStore;
    this.authOutStore = outStore;
    this.authPendingStore = pendingStore;
    this.transport = transport;
    this.infoStore = infoStore;
    this.peerreview = peerreview;
    this.evidenceTransferProtocol = evidenceTransferProtocol;
    this.probabilistic = false;
    this.pXmit = 1.0;
    this.environment = env;
    this.logger = env.getLogManager().getLogger(AuthenticatorPushProtocolImpl.class, null);
  }

  public void enableProbabilisticChecking(double pXmit) {
    this.probabilistic = true;
    this.pXmit = pXmit;
  }
  
  /* Start an authenticator push. We begin by determining the witness sets of all the nodes
  for which we have authenticators. */

  public void push() {
    if (logger.level <= Logger.FINE) logger.log("Authenticator push initiated with "+authOutStore.getNumSubjects()+" subjects");

    evidenceTransferProtocol.requestWitnesses(authOutStore.getSubjects(), 
        new Continuation<Map<Identifier,Collection<Handle>>, Exception>() {

          public void receiveException(Exception exception) {
            logger.logException("Error requesting witnesses "+authOutStore.getSubjects(), exception);
          }

          public void receiveResult(Map<Identifier, Collection<Handle>> result) {
            continuePush(result);
          }
        });
    
  }

  /*
   * Once we have all the witness sets, we bundle up all of our authenticators
   * in AUTHPUSH messages and send them to the witnesses.
   * 
   * NOTE: in the c++ impl, this was done via peerreview, now it's done via 
   * a continuation.
   */
  public void continuePush(Map<Identifier, Collection<Handle>> subjectsToWitnesses) {

    if (logger.level <= Logger.FINE) logger.log("Continuing authenticator push with "+subjectsToWitnesses.size()+" subjects");

    /*
     * We're given a list mapping subjects to witnesses, whereas what we really
     * want is a list mapping witnesses to subjects (so, if a node is a witness
     * for multiple subjects, we can put all of the subjects' authenticators
     * into a single datagram). We invert the list here.
     */

    Map<Handle, Map<Identifier, List<Authenticator>>> witnesses = new HashMap<Handle, Map<Identifier, List<Authenticator>>>();
    for (Entry<Identifier, Collection<Handle>> entry : subjectsToWitnesses.entrySet()) {
      for (Handle witness : entry.getValue()) {
        Map<Identifier, List<Authenticator>> m = witnesses.get(witness);
        if (m == null) {
          m = new HashMap<Identifier, List<Authenticator>>();
          witnesses.put(witness, m);
        }
        m.put(entry.getKey(), authOutStore.getAuthenticators(entry.getKey()));
      }
    }

    if (logger.level <= Logger.FINER) logger.log("Found " + witnesses.size() + " unique witnesses");

    /*
     * For each witness, create all the AUTHPUSH messages we need to send to it.
     * The messages are datagrams, so we ask the transport layer for the MSS and
     * split the messages where appropriate.
     */
    try {
      for (Entry<Handle, Map<Identifier, List<Authenticator>>> e : witnesses
          .entrySet()) {
        sendAuthenticators(e.getKey(), e.getValue());
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Error sending authenticators.", ioe);
    }

    /* Once all the messages are sent, we don't need the authenticators any more */
    if (logger.level <= Logger.FINER) logger.log("Push completed; releasing authenticators");
    for (Identifier i : subjectsToWitnesses.keySet()) {
      authOutStore.flush(i);
    }
  }

  /**
   * TODO: make this use udp, or smaller AuthPushMessages
   * 
   * @param h
   * @param authenticators
   */
  protected void sendAuthenticators(Handle h, Map<Identifier, List<Authenticator>> authenticators) throws IOException {
    if (probabilistic) {
      for (Entry<Identifier, List<Authenticator>> e : authenticators.entrySet()) {
        /*
         * If probabilistic checking is enabled, we can skip the authenticators
         * with some probability
         */
        Iterator<Authenticator> i = e.getValue().iterator();
        while (i.hasNext()) {
          if (random.nextFloat() < 1.0-pXmit) {
            i.remove();
          }
        }
      }
    }

    peerreview.transmit(h, new AuthPushMessage<Identifier>(authenticators), null, null);
  }
  
  /**
   * Called when some other node sends us an AUTHPUSH message. The message may
   * contain authenticators from multiple nodes. If, for some node, we don't
   * have a certificate, we store its auths in the authPendingStore and request
   * the certificate from the sender, otherwise we check the auths' signatures
   * and put them into the authInStore.
   */
  public void addAuthenticatorsIfValid(List<Authenticator> authenticators,
      Identifier id) {
    for (Authenticator thisAuth : authenticators) {
      if (!peerreview.addAuthenticatorIfValid(authInStore, id, thisAuth))
        if (logger.level <= Logger.WARNING) logger.log("Authenticator "+thisAuth+" has invalid signature; discarding");
    }
  }

  /**
   * Called when some other node sends us an AUTHPUSH message. The message may
   * contain authenticators from multiple nodes. If, for some node, we don't
   * have a certificate, we store its auths in the authPendingStore and request
   * the certificate from the sender, otherwise we check the auths' signatures
   * and put them into the authInStore.
   */
  public void handleIncomingAuthenticators(Handle source, AuthPushMessage<Identifier> msg) {
    if (logger.level <= Logger.INFO) logger.log("Received authenticators from "+source);
    for (Entry<Identifier, List<Authenticator>> e : msg.authenticators.entrySet()) {
      Identifier id = e.getKey();
      if (logger.level <= Logger.FINE) logger.log("  Subject <"+id+">, "+e.getValue().size()+" authenticators");
      if (transport.hasCertificate(id)) {
        addAuthenticatorsIfValid(e.getValue(), id);
      } else {
        if (logger.level <= Logger.FINE) logger.log("  Missing certificate for this subject; requesting from "+source+" and recording auths as pending");
        for (Authenticator a : e.getValue()) {
          authPendingStore.addAuthenticator(id, a);
        }         
        peerreview.requestCertificate(source, id);
      }           
      if (infoStore.getStatus(id) != STATUS_TRUSTED) {
        evidenceTransferProtocol.sendEvidence(source, id);
      }
    }
  }

  /**
   * When we receive a new certificate, we may be able to check some more
   * signatures on auths in the authPendingStore
   */
  public void notifyCertificateAvailable(Identifier id) {
    int numAuths = authPendingStore.numAuthenticatorsFor(id);
    if (numAuths > 0) {
      if (logger.level <= Logger.FINE)
        logger.log("Found " + numAuths + " pending authenticators for <"+id+">; processing...");

      List<Authenticator> buffer = authPendingStore.getAuthenticators(id);
      addAuthenticatorsIfValid(buffer, id);
      authPendingStore.flushAuthenticatorsFor(id);
    }
  }


}