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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.util.Serializer;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.RandomAccessFileIOBuffer;

public class AuthenticatorStoreImpl<Identifier extends RawSerializable> implements AuthenticatorStore<Identifier> {
  
  protected boolean allowDuplicateSeqs;
  PeerReview<?, Identifier> peerreview;
  int numSubjects;
  RandomAccessFileIOBuffer authFile;

  Map<Identifier,SortedSet<Authenticator>> authenticators;
  
  Logger logger;
  Serializer<Identifier> idSerializer;
  AuthenticatorSerializer authenticatorSerializer;

  boolean memoryBufferDisabled = false;
  
  public AuthenticatorStoreImpl(PeerReview<?, Identifier> peerreview) {
    this(peerreview,false);
  }

  public AuthenticatorStoreImpl(PeerReview<?, Identifier> peerreview, boolean allowDuplicateSeqs) {
    this.allowDuplicateSeqs = allowDuplicateSeqs;
    this.authenticators = new HashMap<Identifier, SortedSet<Authenticator>>();
    this.authFile = null;
    this.numSubjects = 0;
    this.peerreview = peerreview;
    this.authenticatorSerializer = peerreview.getAuthenticatorSerializer();
    this.idSerializer = peerreview.getIdSerializer();
    
    logger = peerreview.getEnvironment().getLogManager().getLogger(AuthenticatorStoreImpl.class, null);
  }
  
  public void destroy() {
    authenticators.clear();
    
    if (authFile != null) {
      try {
        authFile.close();
      } catch (IOException ioe) {
        logger.logException("Couldn't close authFile "+authFile,ioe);
      } finally {
        authFile = null;
      }
    }
  }
  
  /**
   * Read in the Authenticators from a file.
   * 
   *  Each instance of this class has just a single file in which to store authenticators.
   *  The file format is (<id> <auth>)*; authenticators from different peers can be
   *  mixed. This method sets the name of the file and reads its current contents
   *  into memory. 
   */
  public void setFilename(File file) throws IOException {
    if (authFile != null) {
      authFile.close();
      authFile = null;
    }
    
    authFile = new RandomAccessFileIOBuffer(file, "rw"); //O_RDWR | O_CREAT, 0644);
    
    // read in authenticators
    int authenticatorsRead = 0;
    int bytesRead = 0;
    long pos = 0;
    authFile.seek(pos);
    while (authFile.bytesRemaining() > 0) {
      
      try {
        Identifier id = idSerializer.deserialize(authFile); //idbuf, &pos, sizeof(idbuf));
        Authenticator authenticator = authenticatorSerializer.deserialize(authFile);
        addAuthenticatorToMemory(id, authenticator);
        authenticatorsRead++;
        pos = authFile.getFilePointer();
      } catch (IOException ioe) {
        break;
      }
    }
     
    // clobber anything in the file after the ioexception
    authFile.setLength(pos);
    authFile.seek(authFile.length());
  }

  /**
   * Add a new authenticator. Note that in memory, the authenticators are sorted by nodeID
   * and by sequence number, whereas on disk, they are not sorted at all. 
   */

  protected void addAuthenticatorToMemory(Identifier id, Authenticator authenticator) {
    SortedSet<Authenticator> list = authenticators.get(id);
    if (list == null) {
      list = new TreeSet<Authenticator>();
      authenticators.put(id, list);
    }
    if (!allowDuplicateSeqs) {
      SortedSet<Authenticator> sub = list.subSet(
          new Authenticator(authenticator.getSeq()+1, null, null),
          new Authenticator(authenticator.getSeq(),null,null));
      if (!sub.isEmpty()) {
        if (!sub.contains(authenticator)) {
          throw new RuntimeException("Adding duplicate auths for the same sequence number is not allowed for this store old:"+sub.first()+" new:"+authenticator);
        }
      }
    }
    list.add(authenticator);
  }
  
  /**
   *  Discard the authenticators in a certain sequence range (presumably because we just checked 
   *  them against the corresponding log segment, and they were okay) 
   */
  protected void flushAuthenticatorsFromMemory(Identifier id, long minseq, long maxseq) {

    SortedSet<Authenticator> list = authenticators.get(id);    

    if (list != null) {
      list.removeAll(getAuthenticators(id, minseq, maxseq));
    }    
  }
  
  protected SortedSet<Authenticator> findSubject(Identifier id) {
    return authenticators.get(id);
  }

  public void addAuthenticator(Identifier id, Authenticator authenticator) {
    try {
      if (authFile != null) {
        idSerializer.serialize(id,authFile);
        authenticator.serialize(authFile);
      }
      if (!memoryBufferDisabled) addAuthenticatorToMemory(id, authenticator);
    } catch (IOException ioe) {
      throw new RuntimeException("Error in addAuthenticator("+id+","+authenticator+","+authFile+")",ioe);
    }
  }

  public void flushAuthenticatorsFor(Identifier id, long minseq, long maxseq) {
    flushAuthenticatorsFromMemory(id, minseq, maxseq);
  }

  public void flushAuthenticatorsFor(Identifier id) {
    flushAuthenticatorsFor(id, Long.MIN_VALUE, Long.MAX_VALUE-1);
  }
  

  public void garbageCollect() throws IOException {
    if (authFile == null) return;

    // clobber the file
    authFile.setLength(0);
    authFile.seek(0);

    // write all the elements
    for (Identifier i : authenticators.keySet()) {
      SortedSet<Authenticator> list = authenticators.get(i);
      for (Authenticator a : list) {
        idSerializer.serialize(i,authFile);
        a.serialize(authFile);
      }
    }
  }

  public int getAuthenticatorSizeBytes() {
    return authenticatorSerializer.getSerializedSize();
  }

  public List<Authenticator> getAuthenticators(Identifier id, long minseq,
      long maxseq) {
    SortedSet<Authenticator> list = authenticators.get(id);    
    //logger.log("getAuthenticators("+id+","+minseq+"->"+maxseq+"): total:"+list);
    if (list != null) {
      SortedSet<Authenticator> subList = list.subSet(new Authenticator(maxseq+1,null,null), new Authenticator(minseq,null,null));      
      //logger.log("getAuthenticators: "+subList);
      return new ArrayList<Authenticator>(subList);
    }    
    return Collections.emptyList();
  }

  public List<Authenticator> getAuthenticators(Identifier id) {
    return getAuthenticators(id,Long.MIN_VALUE, Long.MAX_VALUE-1);
  }

  public Authenticator getLastAuthenticatorBefore(Identifier id, long seq) {
    List<Authenticator> list = getAuthenticators(id, Long.MIN_VALUE, seq);
    if (list.isEmpty()) return null;
    return list.get(0);
  }

  public Authenticator getMostRecentAuthenticator(Identifier id) {
    SortedSet<Authenticator> list = authenticators.get(id);    
    if (list == null || list.isEmpty()) return null;
    return list.first();
  }

  public int getNumSubjects() {
    return authenticators.size();
  }

  public Authenticator getOldestAuthenticator(Identifier id) {
    SortedSet<Authenticator> list = authenticators.get(id);    
    if (list == null) return null;
    return list.last();
  }

  public List<Identifier> getSubjects() {
    return new ArrayList<Identifier>(authenticators.keySet());
  }

  public int numAuthenticatorsFor(Identifier id) {
    SortedSet<Authenticator> list = authenticators.get(id);    
    if (list == null) return 0;
    return list.size();
  }

  public int numAuthenticatorsFor(Identifier id, long minseq, long maxseq) {
    return getAuthenticators(id, minseq, maxseq).size();    
  }

  public Authenticator statAuthenticator(Identifier id, long seq) {
    List<Authenticator> ret = getAuthenticators(id, seq, seq);
    if (ret == null || ret.isEmpty()) return null;
    return ret.get(0);
  }
  
  public void flush(Identifier id) {
    authenticators.remove(id);
  }

  public void flushAll() {
    authenticators.clear();
  }

  public void disableMemoryBuffer() { 
    this.memoryBufferDisabled = true; 
  }

}
