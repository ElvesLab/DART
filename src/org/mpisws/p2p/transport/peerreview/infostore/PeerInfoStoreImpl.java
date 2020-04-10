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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.message.UserDataMessage;
import org.mpisws.p2p.transport.util.FileInputBuffer;
import org.mpisws.p2p.transport.util.FileOutputBuffer;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * In this class, the PeerReview library keeps information about its peers.
 * Specifically, it stores the last checked authenticator plus any challenges,
 * responses or proofs that are known about the peer.
 */
public class PeerInfoStoreImpl<Handle, Identifier> implements
    PeerInfoStore<Handle, Identifier> {
  IdentityTransport<Handle, Identifier> transport;

  File directory;
  // Subject -> PeerInfoRecord
  Map<Identifier, PeerInfoRecord<Handle, Identifier>> peerInfoRecords;
  StatusChangeListener<Identifier> listener;
  boolean notificationEnabled;

  protected Environment environment;

  protected Logger logger;
  IdStrTranslator<Identifier> stringTranslator;
  AuthenticatorSerializer authSerializer;

  EvidenceSerializer evidenceSerializer;
  
  
  public PeerInfoStoreImpl(
      IdentityTransport<Handle, Identifier> transport, 
      IdStrTranslator<Identifier> stringTranslator, 
      AuthenticatorSerializer authSerializer, 
      EvidenceSerializer evidenceSerializer, 
      Environment env) {
    this.peerInfoRecords = new HashMap<Identifier, PeerInfoRecord<Handle, Identifier>>();
    this.directory = null;
    this.notificationEnabled = true;
    this.transport = transport;
    this.listener = null;
    this.stringTranslator = stringTranslator;
    this.authSerializer = authSerializer;
    this.environment = env;
    this.evidenceSerializer = evidenceSerializer;
    this.logger = env.getLogManager().getLogger(PeerInfoStoreImpl.class, null);
  }
  
  public static boolean isProof(Evidence e) {
    switch (e.getEvidenceType()) {
      case CHAL_AUDIT:
        return false;
      case CHAL_SEND:
        return false;
      case PROOF_INCONSISTENT:
        return true;
      case PROOF_NONCONFORMANT:
        return true;
      default:
        throw new IllegalArgumentException("Cannot evaluate isProof("+e+"):"+e.getEvidenceType());
//        panic("Cannot evaluate isProof("+e.getType()+")");
    }    
  }
    
  public void setStatusChangeListener(StatusChangeListener<Identifier> listener) {
    this.listener = listener;
  }

  /* Locates evidence, or creates a new entry if 'create' is set to true */
  public EvidenceRecord<Handle, Identifier> findEvidence(Identifier originator, Identifier subject, long timestamp) {
    return findEvidence(originator, subject, timestamp, false);
  }
  public EvidenceRecord<Handle, Identifier> findEvidence(Identifier originator, Identifier subject, long timestamp, boolean create) {
    PeerInfoRecord<Handle, Identifier> rec = find(subject, create);
    if (rec == null)
      return null;
    return rec.findEvidence(originator, timestamp, create);
  }
  
  /**
   * This is called when new evidence becomes available, or (during startup) for
   * all evidence files on disk. We only keep some metadata in memory; the
   * actual evidence is stored in a separate file on disk.
   */

  public void markEvidenceAvailable(Identifier originator, Identifier subject,
      long timestamp, boolean isProof, Handle interestedParty) {
    PeerInfoRecord<Handle, Identifier> rec = find(subject, true);
    EvidenceRecord<Handle, Identifier> evi = rec.findEvidence(originator,
        timestamp, true);

    assert (rec != null && evi != null);
    /* Create or update metadata */

    if (interestedParty != null) {
      evi.setInterestedParty(interestedParty);
    }

    evi.setIsProof(isProof);
  }

  /**
   * This is called when another node answers one of our challenges. Again, we
   * only update the metadata in memory; the actual response is kept in a file
   * on disk.
   */
  public void markResponseAvailable(Identifier originator, Identifier subject, long timestamp) {
   PeerInfoRecord<Handle, Identifier> rec = find(subject, true);
   EvidenceRecord<Handle, Identifier> evi = rec.findEvidence(originator, timestamp, true);

   evi.setHasResponse(); 
  }
  
  /* Add a new piece of evidence */

  public void addEvidence(Identifier originator, Identifier subject, long timestamp, Evidence evidence) throws IOException {
    addEvidence(originator, subject, timestamp, evidence, null);
  }
  public void addEvidence(Identifier originator, Identifier subject, long timestamp, Evidence evidence, Handle interestedParty) throws IOException {
//    char namebuf[200], buf1[200], buf2[200];
    if (logger.level <= Logger.FINE) logger.log("addEvidence(orig="+originator+", subj="+subject+", seq="+timestamp+")");

    boolean proof = isProof(evidence);

    /* Write the actual evidence to disk */
//  sprintf(namebuf, "%s/%s-%s-%lld.%s", dirname, subject->render(buf1), originator->render(buf2), timestamp, proof ? "proof" : "challenge");

    
    File outFile = getFile(subject, originator, timestamp, (proof ? "proof" : "challenge"));
        
    FileOutputBuffer buf = new FileOutputBuffer(outFile);
    buf.writeByte((byte)evidence.getEvidenceType());      
    evidence.serialize(buf);
    buf.close();
    
    /* Update metadata in memory */
    
    markEvidenceAvailable(originator, subject, timestamp, proof, interestedParty);
  }

  protected File getFile(Identifier subject, Identifier originator, long timestamp, String suffix) {
    File outFile = new File(directory, stringTranslator.toString(subject)+"-"+stringTranslator.toString(originator)+"-"+timestamp+"."+suffix);  
    return outFile;
  }
  
  /* Find out whether a node is TRUSTED, SUSPECTED or EXPOSED */

  public int getStatus(Identifier id) {
    PeerInfoRecord<Handle, Identifier> rec = find(id, false);
    return (rec != null) ? rec.getStatus() : STATUS_TRUSTED;
  }

  /* Called during startup to inform the store where its files are located */

  public boolean setStorageDirectory(File directory) throws IOException {
    /* Create the directory if it doesn't exist yet */
    if (! directory.exists()) {
      directory.mkdirs();
    } else {
      if (!directory.isDirectory()) {
        throw new IllegalArgumentException(directory.getAbsolutePath()+" is not a directory.");
      }
    }
    this.directory = directory;
    
//    if (!dir) {
//      if (mkdir(dirname, 0755) < 0)
//        return false;
//      if ((dir = opendir(dirname)) == NULL)
//        return false;
//    }
      
//    strncpy(this->dirname, dirname, sizeof(this->dirname));

    /* To prevent a flood of status updates, we temporarily disable updates
       while we inspect the existing evidence on disk */

    boolean notificationWasEnabled = notificationEnabled;
    notificationEnabled = false;
      
    /* Read the entire directory */
    File[] bar = directory.listFiles();
    if (bar != null) {
      for (File ent : bar) {
        if (ent.isDirectory()) continue;
        String d_name = ent.getName();
        String[] foo = d_name.split("\\.");
        if (foo.length != 2) continue;
        String first = foo[0];
        String suffix = foo[1];
        if (suffix.equals("info")) {
          /* INFO files contain the last checked authenticator */
          Identifier id = stringTranslator.readIdentifierFromString(first);
          FileInputBuffer buf = new FileInputBuffer(ent, logger);
          Authenticator lastAuth = authSerializer.deserialize(buf);
          buf.close();
          setLastCheckedAuth(id, lastAuth);
        } else if (suffix.equals("challenge") || suffix.equals("response") || suffix.equals("proof")) {
  //        char namebuf[200];
  //        struct stat statbuf;
  //        sprintf(namebuf, "%s/%s", dirname, ent->d_name);
  //        int statRes = stat(namebuf, &statbuf);
  //        assert(statRes == 0);
  
  
          
          /* PROOF, CHALLENGE and RESPONSE files */
          String[] parts = first.split("-");
          if (parts.length != 3) throw new IOException("Error reading filename :"+ent+" did not split into 3 parts:"+Arrays.toString(parts));
          Identifier subject = stringTranslator.readIdentifierFromString(parts[0]);
          Identifier originator = stringTranslator.readIdentifierFromString(parts[1]);
          long seq = Long.parseLong(parts[2]);
  
          if (suffix.equals("challenge")) {
            markEvidenceAvailable(originator, subject, seq, false, null);
          } else if (suffix.equals("proof")) {
            markEvidenceAvailable(originator, subject, seq, true, null);
          } else if (suffix.equals("response")){
            markResponseAvailable(originator, subject, seq);
          }
        }
      }
    }
    notificationEnabled = notificationWasEnabled;
    
    return true;
  }
  
  public PeerInfoRecord<Handle, Identifier> find(Identifier id) {
    return find(id,false);
  }
  public PeerInfoRecord<Handle, Identifier> find(Identifier id, boolean create) {
    PeerInfoRecord<Handle, Identifier> ret = peerInfoRecords.get(id);
    
    if (ret == null && create) {
      ret = new PeerInfoRecord<Handle, Identifier>(id, this);
      peerInfoRecords.put(id, ret);
    }
    return ret;
  }
  
  public Authenticator getLastCheckedAuth(Identifier id) {
    PeerInfoRecord<Handle, Identifier> rec = find(id, false);
    if (rec == null) return null;
      
    return rec.getLastCheckedAuth();
  }

  public void setLastCheckedAuth(Identifier id, Authenticator auth) {
    PeerInfoRecord<Handle, Identifier> rec = find(id, true);
    try {
      rec.setLastCheckedAuth(auth, directory, stringTranslator);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  /* Retrieve some information about a given piece of evidence */
  // Use findEvidence()
//  bool PeerInfoStore::statEvidence(Identifier *originator, Identifier *subject, long long timestamp, int *evidenceLen, bool *isProof, bool *haveResponse, NodeHandle **interestedParty)

  /**
   * Get the actual bytes of a piece of evidence
   */
  public Evidence getEvidence(Identifier originator, Identifier subject, long timestamp) throws IOException {
    EvidenceRecord<Handle, Identifier> evi = findEvidence(originator, subject, timestamp, false);
    if (evi == null) return null;
    
    File infile = getFile(subject, originator, timestamp, evi.isProof() ? "proof" : "challenge");
    // done automatically by FIB
//    if (!infile.exists()) throw new FileNotFoundException("Cannot read '"+infile+"'");

    FileInputBuffer buf = new FileInputBuffer(infile, logger);
    byte type = buf.readByte();
    Evidence e = evidenceSerializer.deserialize(buf, type, false);
    buf.close();
    return e;
  }
  
  

  /**
   * Record a response to a challenge
   */
  public void addResponse(Identifier originator, Identifier subject, long timestamp, Evidence response) throws IOException {
    EvidenceRecord<Handle, Identifier> evi = findEvidence(originator, subject, timestamp);
    assert(evi != null && !evi.isProof() && !evi.hasResponse());
    
//    char namebuf[200], buf1[200], buf2[200];
//    sprintf(namebuf, "%s/%s-%s-%lld.response", dirname, subject->render(buf1), originator->render(buf2), timestamp);
//    
//    FILE *outfile = fopen(namebuf, "w+");
//    if (!outfile) 
//      panic("Cannot create '%s'", namebuf);
//      
    FileOutputBuffer outfile = new FileOutputBuffer(getFile(subject, originator, timestamp, "response"));
    outfile.writeByte((byte)response.getEvidenceType());
    response.serialize(outfile);
    outfile.close();
    
    markResponseAvailable(originator, subject, timestamp);
  }

  public String getHistoryName(Identifier subject) {
    return new File(directory,stringTranslator.toString(subject)+"-log").toString();
  }
  
  public void notifyStatusChanged(Identifier subject, int newStatus) {
    if (!notificationEnabled || listener == null) return;
    listener.notifyStatusChange(subject, newStatus);
  }

  /**
   * Look up the first unanswered challenge to a given node
   */
  public EvidenceRecord<Handle, Identifier> statFirstUnansweredChallenge(Identifier subject) {
    PeerInfoRecord<Handle, Identifier> rec = find(subject);
    if (rec == null)
      return null;
    return rec.getFirstUnansweredChallenge();
  }

  public EvidenceRecord<Handle, Identifier> statProof(Identifier subject) {
    PeerInfoRecord<Handle, Identifier> rec = find(subject);
    if (rec == null) return null;
    return rec.getFirstProof();
  }

}
