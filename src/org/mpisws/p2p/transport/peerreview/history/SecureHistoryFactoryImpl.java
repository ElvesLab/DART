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
package org.mpisws.p2p.transport.peerreview.history;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;


import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.RandomAccessFileIOBuffer;

public class SecureHistoryFactoryImpl implements SecureHistoryFactory, IndexEntryFactory, PeerReviewConstants {
  Environment environment;
  
  HashProvider hashProv;

  public SecureHistoryFactoryImpl(HashProvider hashProv, Environment env) {    
    this.hashProv = hashProv;
    this.environment = env;
  }
  
  /**
   * Creates a new history (aka log). Histories are stored as two files: The 'index' file has a 
   * fixed-size record for each entry, which contains the sequence number, content and node
   * hashes, as well as an index into the data file. The 'data' file just contains the raw
   * bytes for each entry. Note that the caller must specify the node hash and the sequence
   * number of the first log entry, which forms the base of the hash chain.
   */
  public SecureHistory create(String name, long baseSeq, byte[] baseHash) throws IOException {
    RandomAccessFileIOBuffer indexFile, dataFile;
    
    if (name == null) {
      name = "peerReview."+environment.getTimeSource().currentTimeMillis()+"."+environment.getRandomSource().nextInt();
    }
    String indexName = name+".index";
    String dataName = name+".data";
    
    File f = new File(indexName);
    if (f.exists()) {
      throw new RuntimeException("File "+f+" already exists.");
    }
    f = new File(dataName);
    if (f.exists()) {
      throw new RuntimeException("File "+f+" already exists.");
    }
    
    indexFile = new RandomAccessFileIOBuffer(indexName, "rw"); // may be rw
    
    try {
      dataFile = new RandomAccessFileIOBuffer(dataName, "rw"); // may be rw
    } catch (IOException ioe) {
      indexFile.close();
      throw ioe;
    }

    IndexEntry entry = new IndexEntry(baseSeq, (long)0,EVT_INIT,-1, hashProv.getEmptyHash(), baseHash);
    
    entry.serialize(indexFile);
    
    SecureHistoryImpl history = makeSecureHistory(indexFile, dataFile, false, hashProv, this, environment.getLogManager().getLogger(SecureHistoryImpl.class, name));
    
    return history;
  }

  protected SecureHistoryImpl makeSecureHistory(RandomAccessFileIOBuffer indexFile, RandomAccessFileIOBuffer dataFile, boolean readOnly, HashProvider hashProv, IndexEntryFactory indexFactory, Logger logger) throws IOException {
    return new SecureHistoryImpl(indexFile, dataFile, readOnly, hashProv, indexFactory, logger);
  }
  
  /**
   *  Opens an existing history (aka log). The 'mode' can either be 'r' (read-only) or
   *  'w' (read/write). 
   */

  public SecureHistory open(String name, String mode) throws IOException {
    boolean readOnly = false;
    
    if (mode.equals("r")) {
      readOnly = true;
    } else if (!mode.equals("w")) {
      return null;
    }
    
    String fileMode;
    if (readOnly) {
      fileMode = "r";
    } else {
      fileMode = "rw"; // may be "rw"
    }
    
    File f = new File(name+".index");
    if (!f.exists()) {
      return null;
    }

    File f2 = new File(name+".data");
    if (!f2.exists()) {
      return null;
    }
    

    RandomAccessFileIOBuffer indexFile = new RandomAccessFileIOBuffer(name+".index",fileMode);

    RandomAccessFileIOBuffer dataFile;
    try {
      dataFile = new RandomAccessFileIOBuffer(name+".data",fileMode);
    } catch (IOException ioe) {
      indexFile.close();
      throw ioe;
    }

    return makeSecureHistory(indexFile, dataFile, readOnly, hashProv, this, environment.getLogManager().getLogger(SecureHistoryImpl.class, name));
  }

  public IndexEntry build(InputBuffer buf) throws IOException {
    long seq = buf.readLong();
    long fileIndex = buf.readLong();
    int sizeInFile = buf.readInt();
    short type = buf.readShort();
    byte[] contentHash = new byte[hashProv.getHashSizeBytes()];
    buf.read(contentHash);
    byte[] nodeHash = new byte[hashProv.getHashSizeBytes()];
    buf.read(nodeHash);
    return new IndexEntry(seq, fileIndex, type, sizeInFile, contentHash, nodeHash);
  }

  public int getSerializedSize() {
    return 8+8+4+2+hashProv.getHashSizeBytes()*2;
  }

  public SecureHistory createTemp(long baseSeq, byte[] baseHash)
      throws IOException {
    return create(null, baseSeq, baseHash);
  }

}
