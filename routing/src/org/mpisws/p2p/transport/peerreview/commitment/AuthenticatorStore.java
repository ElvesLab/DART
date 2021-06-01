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
import java.util.List;

/**
 * Witnesses use instances of this class to store authenticators. Typically
 * there are three instances: authInStore, authPendingStore and authOutStore.
 * The former two contain authenticators about nodes for which the local node is
 * a witness, while the latter contains authenticators about other nodes which
 * haven't been sent to the corresponding witness sets yet.
 */
public interface AuthenticatorStore<Identifier> {
  public void setFilename(File file) throws IOException;
  public Authenticator getMostRecentAuthenticator(Identifier id);
  public Authenticator getOldestAuthenticator(Identifier id);
  public Authenticator getLastAuthenticatorBefore(Identifier id, long seq);
  
  public void disableMemoryBuffer();

  /**
   * Also writes it to disk.
   * @param id
   * @param authenticator
   * @throws IOException 
   */
  public void addAuthenticator(Identifier id, Authenticator authenticator);
  
  /**
   * Commits the Authenticators in memory to disk, overwriting the old store.
   * 
   * Since the authenticator file on disk is append-only, we need to garbage
   * collect it from time to time. When this becomes necessary, we clear the
   * file and then write out the authenticators currently in memory.
   * @throws IOException 
   */
  public void garbageCollect() throws IOException;
  
  public int numAuthenticatorsFor(Identifier id);
  
  public int numAuthenticatorsFor(Identifier id, long minseq, long maxseq);
  
  public void flushAuthenticatorsFor(Identifier id, long minseq, long maxseq);
  public void flushAuthenticatorsFor(Identifier id);

  public Authenticator statAuthenticator(Identifier id, long seq);

  /**
   * Retrieve all the authenticators within a given range of sequence numbers
   */
  public List<Authenticator> getAuthenticators(Identifier id, long minseq, long maxseq);
  public List<Authenticator> getAuthenticators(Identifier id);
  public List<Identifier> getSubjects();
  public int getNumSubjects();
  
  public int getAuthenticatorSizeBytes();

  public void flush(Identifier id);
  public void flushAll();
  
}
