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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.audit.LogSnippet;

import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.RandomAccessFileIOBuffer;

public interface SecureHistory extends PeerReviewConstants {
  public long getNumEntries();
  public long getBaseSeq();
  public long getLastSeq();
  
  /**
   *  Returns the node hash and the sequence number of the most recent log entry 
   */
  public HashSeq getTopLevelEntry();
  
  /**
   * Appends a new entry to the log. If 'storeFullEntry' is false, only the hash of the
   * entry is stored. If 'header' is not NULL, the log entry is formed by concatenating
   * 'header' and 'entry'; otherwise, only 'entry' is used. 
   * 
   * Takes an ordered list of ByteBuffers to append
   * 
   * @throws IOException 
   */
  public void appendEntry(short type, boolean storeFullEntry, ByteBuffer ... entry) throws IOException;
  
  /**
   * Append a new hashed entry to the log. Unlike appendEntry(), this only keeps
   * the content type, sequence number, and hash values. No entry is made in
   * the data file. 
   */
  public void appendHash(short type, byte[] hash) throws IOException;

  /**
   * Sets the next sequence number to be used. The PeerReview library typically
   * uses the format <xxxx...xxxyyyy>, where X is a timestamp in microseconds
   * and Y a sequence number. The sequence numbers need not be contigious
   * (and usually aren't) 
   */
  public boolean setNextSeq(long nextSeq); 

  /**
   * The destructor.  Closes the file handles.
   * @throws IOException 
   */
  public void close() throws IOException;
 
  
  public long findSeq(long seq) throws IOException;// { return findSeqOrHigher(seq, false); };

  /**
   * Look up a given sequence number, or the first sequence number that is 
   * not lower than a given number. The return value is the number of
   * the corresponding record in the index file, or -1 if no matching
   * record was found. 
   */
  public long findSeqOrHigher(long seq, boolean allowHigher) throws IOException;

  /** 
   * Serialize a given range of entries, and write the result to the specified file.
   * This is used when we need to send a portion of our log to some other node,
   * e.g. during an audit. The format of the serialized log segment is as follows:
   *     1. base hash value (size depends on hash function)
   *     2. entry type (1 byte)
   *     3. entry size in bytes (1 byte); 0x00=entry is hashed; 0xFF=16-bit size follows
   *     4. entry content (size as specified; omitted if entry is hashed)
   *     5. difference to next sequence number (1 byte)
   *           0x00: increment by one
   *           0xFF: 64-bit sequence number follows
   *           Otherwise:  Round down to nearest multiple of 1000, then add specified
   *               value times 1000
   *     6. repeat 2-5 as often as necessary; 5 is omitted on last entry.4
   * Note that the idxFrom and idxTo arguments are record numbers, NOT sequence numbers.
   * Use findSeqOrHigher() to get these if only sequence numbers are known. 
   */
  public LogSnippet serializeRange(long idxFrom, long idxTo, HashPolicy hashPolicy) throws IOException;
//  public boolean serializeRange(long idxFrom, long idxTo, HashPolicy hashPolicy, OutputBuffer outfile) throws IOException;

  /**
   *  Retrieve information about a given record 
   *  
   *  @param idx the index you are interested in
   */
  public IndexEntry statEntry(long idx) throws IOException;

  /**
   *  Get the content of a log entry, specified by its record number 
   */
  public byte[] getEntry(long idx, int maxSizeToRead) throws IOException;
  public byte[] getEntry(IndexEntry ie, int maxSizeToRead) throws IOException;

  /**
   * If the log already contains an entry in 'hashed' form and we learn the actual
   * contents later, this function is called. 
   */
  public boolean upgradeHashedEntry(int idx, ByteBuffer entry) throws IOException;
  
  /** 
   * Find the most recent entry whose type is in the specified set. Useful e.g. for
   * locating the last CHECKPOINT or INIT entry. 
   */
  public long findLastEntry(short[] types, long maxSeq) throws IOException;
  
  public void appendSnippetToHistory(LogSnippet snippet) throws IOException;

}
