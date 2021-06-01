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
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.mpisws.p2p.transport.peerreview.audit.LogSnippet;
import org.mpisws.p2p.transport.peerreview.audit.SnippetEntry;
import org.mpisws.p2p.transport.util.FileInputBuffer;
import org.mpisws.p2p.transport.util.FileOutputBuffer;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.RandomAccessFileIOBuffer;

/**
 * The following class implements PeerReview's log. A log entry consists of
 * a sequence number, a type, and a string of bytes. On disk, the log is
 * stored as two files: An index file and a data file.
 * 
 * @author Jeff Hoye
 * @author Andreas Haeberlen
 */
public class SecureHistoryImpl implements SecureHistory {

  protected Logger logger;
  
  protected HashProvider hashProv;
  protected boolean pointerAtEnd;
  protected IndexEntry topEntry;
  protected long baseSeq;
  protected long nextSeq;
  protected long numEntries;
  protected RandomAccessFileIOBuffer indexFile;
  protected RandomAccessFileIOBuffer dataFile;
  protected boolean readOnly;
  
  protected IndexEntryFactory indexFactory;
  
  public SecureHistoryImpl(RandomAccessFileIOBuffer indexFile, RandomAccessFileIOBuffer dataFile, boolean readOnly, HashProvider hashProv, IndexEntryFactory indexFactory, Logger logger) throws IOException {
    assert(indexFile != null && dataFile != null);
    
    this.logger = logger;
    if (logger == null) throw new IllegalArgumentException("logger is null");
    this.indexFactory = indexFactory;

//    if (hashProv.getHashSizeBytes() != HASH_LENGTH) throw new IllegalArgumentException("HashProvider must use the same hashLength");
    
    // read the first entry
    indexFile.seek(0);
    IndexEntry firstEntry = indexFactory.build(indexFile);
    baseSeq = firstEntry.seq;
    
    // read the last entry (topEntry)
    indexFile.seek(indexFile.length()-indexFactory.getSerializedSize());
    topEntry = indexFactory.build(indexFile);
    
    numEntries = (int)(indexFile.length()/indexFactory.getSerializedSize());
    
    dataFile.seek(dataFile.length());
    nextSeq = topEntry.seq+1;
    pointerAtEnd = true;
    
    this.indexFile = indexFile;
    this.dataFile = dataFile;
    this.readOnly = readOnly;
    this.hashProv = hashProv;
  }
  
//  private void writeEntry(IndexEntry entry, OutputBuffer fileWriter) throws IOException {
//    entry.serialize(fileWriter);
//  }
  
  public long getBaseSeq() {
    return baseSeq;
  }

  public long getLastSeq() {
    return topEntry.seq;
  }

  public long getNumEntries() {
    return numEntries;
  }

  /**
   *  Returns the node hash and the sequence number of the most recent log entry 
   */
  public HashSeq getTopLevelEntry() {
    return new HashSeq(topEntry.nodeHash, topEntry.seq);
  }

  /**
   * Appends a new entry to the log. If 'storeFullEntry' is false, only the hash of the
   * entry is stored. If 'header' is not NULL, the log entry is formed by concatenating
   * 'header' and 'entry'; otherwise, only 'entry' is used. 
   */
  public void appendEntry(short type, boolean storeFullEntry, ByteBuffer ... entry) throws IOException {
    assert(indexFile != null && dataFile != null);
    if (logger.level <= Logger.FINER) logger.log("appendEntry("+type+","+storeFullEntry+","+entry.length+","+entry[0].remaining()+"):"+nextSeq);
    // Sanity check (for debugging) 

    if (readOnly) throw new IllegalStateException("Cannot append entry to readonly history");
    
    
    // As an optimization, the log 'remembers' the last entry that was read or written.
    // However, this means that the file pointers do not necessarily point to the 
    // end of the index and data files. If they don't, we must reset them. 

    if (!pointerAtEnd) {
      indexFile.seek(indexFile.length()); 
      dataFile.seek(dataFile.length()); 
      pointerAtEnd = true;
    }
    
    IndexEntry e = new IndexEntry(nextSeq++);
    
    // Calculate the content hash */
    
    e.contentHash = hashProv.hash(entry);
    
    // Calculate the node hash. Note that this also covers the sequence number and
    // the entry type.

    e.nodeHash = hashProv.hash(e.seq, type, topEntry.nodeHash, e.contentHash);

    // Write the entry to the data file 

    e.type = type;
    e.fileIndex = dataFile.getFilePointer();
    if (storeFullEntry) {
      e.sizeInFile = 0;
      for (ByteBuffer ent : entry) {
        e.sizeInFile += ent.remaining();
        dataFile.write(ent.array(), ent.position(), ent.remaining());        
      }
    } else {
      e.sizeInFile = -1;
    }
    
    // Optionally, the index file entries can be padded to a multiple of 16 bytes,
    // so they're easier to read in a hexdump. 
 
//    #ifdef USE_PADDING
//     for (int i=0; i<sizeof(e.padding); i++)
//       e.padding[i] = 0xEE;
//    #endif

    // Write the entry to the index file 

    topEntry = e;
    topEntry.serialize(indexFile);
    numEntries++;    
  }
  
  /**
   * Append a new hashed entry to the log. Unlike appendEntry(), this only keeps
   * the content type, sequence number, and hash values. No entry is made in
   * the data file. 
   */
  public void appendHash(short type, byte[] hash) throws IOException {
    assert(indexFile != null && dataFile != null);
    
    // Sanity check (for debugging) 

    if (readOnly) throw new IllegalStateException("Cannot append entry to readonly history");
    
    if (!pointerAtEnd) {
      indexFile.seek(indexFile.length()); 
      dataFile.seek(dataFile.length()); 
      pointerAtEnd = true;
    }
    
    IndexEntry e = new IndexEntry(nextSeq++);

    e.contentHash = hash;
    
    e.nodeHash = hashProv.hash(e.seq, type, topEntry.nodeHash, e.contentHash);
    e.type = type;
    e.fileIndex = -1;
    e.sizeInFile = -1;
    
//    #ifdef USE_PADDING
//    for (int i=0; i<sizeof(e.padding); i++)
//      e.padding[i] = 0xEE;
//    #endif

    // Write an entry to the index file 
    topEntry = e;
    topEntry.serialize(indexFile);
    numEntries++;
  }  
  
  /**
   * Sets the next sequence number to be used. The PeerReview library typically
   * uses the format <xxxx...xxxyyyy>, where X is a timestamp in microseconds
   * and Y a sequence number. The sequence numbers need not be contigious
   * (and usually aren't) 
   */
  public boolean setNextSeq(long nextSeq) {
    if (nextSeq < this.nextSeq) return false;
    
    this.nextSeq = nextSeq;
    return true;
  }
  
  /**
   * The destructor.  Closes the file handles.
   */
  public void close() throws IOException {
    assert(indexFile != null && dataFile != null);
    if (logger.level <= Logger.FINE) logger.logException(this+".close()", new Exception("Stack Trace"));
    indexFile.close();
    dataFile.close();
    
    indexFile = null;
    dataFile = null;
  }
  
  public long findSeq(long seq) throws IOException { 
    return findSeqOrHigher(seq, false); 
  }

  /**
   * Look up a given sequence number, or the first sequence number that is 
   * not lower than a given number. The return value is the number of
   * the corresponding record in the index file, or -1 if no matching
   * record was found. 
   */
  public long findSeqOrHigher(long seq, boolean allowHigher) throws IOException {
    assert(indexFile != null && dataFile != null);
    
    // Some special cases where we know the answer without looking

    if (seq > topEntry.seq)
      return -1;
    
    if (allowHigher && (seq < baseSeq))
      return 0;
        
    if (seq == topEntry.seq)
      return numEntries - 1;
    
    // Otherwise, do a binary search
    
    pointerAtEnd = false;
    
    indexFile.seek(indexFile.length());
    long rbegin = 1;
    long rend = (indexFile.getFilePointer() / (long)indexFactory.getSerializedSize()) - 1;
    
    while (rbegin != rend) {
      assert(rend >= rbegin);

      long pivot = (rbegin+rend)/2;      
      indexFile.seek(pivot*indexFactory.getSerializedSize());

      IndexEntry ie = indexFactory.build(indexFile);
      if (ie.seq >= seq)
        rend = pivot;
      else 
        rbegin = pivot+1;
    }

    if (allowHigher)
      return rbegin;

    indexFile.seek(rbegin * indexFactory.getSerializedSize());
    IndexEntry ie = indexFactory.build(indexFile);
    if (ie.seq != seq)
      return -1;

    return rbegin;  
  }

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
   *     6. repeat 2-5 as often as necessary; 5 is omitted on last entry.
   * Note that the idxFrom and idxTo arguments are record numbers, NOT sequence numbers.
   * Use findSeqOrHigher() to get these if only sequence numbers are known. 
   */
  public LogSnippet serializeRange(long idxFrom, long idxTo, HashPolicy hashPolicy) throws IOException {
    assert((0 < idxFrom) && (idxFrom <= idxTo) && (idxTo < numEntries));

    IndexEntry ie;

    // Write base hash value

    pointerAtEnd = false;  
    indexFile.seek((idxFrom-1) * indexFactory.getSerializedSize());
    ie = indexFactory.build(indexFile);
    byte[] baseHash = ie.nodeHash;
    ArrayList<SnippetEntry> entryList = new ArrayList<SnippetEntry>();
    
    // Go through entries one by one    
    long previousSeq = -1;
    for (long idx=idxFrom; idx<=idxTo; idx++) {
      // Read index entry
      
      ie = indexFactory.build(indexFile);
      if (ie == null) 
        throw new IOException("History read error");
        
      // we're going to write directly to outfile, in the c++ impl it used header[]/bytesInHeader
//      byte[] header = new byte[200];
//      int bytesInHeader = 0;
      
      assert((previousSeq == -1) || (ie.seq > previousSeq));
      
      // If entry is not hashed, read contents from the data file       
      byte[] buffer = null;
      if (ie.sizeInFile > 0) {
        buffer = new byte[(int)ie.sizeInFile]; // grumble...  This needs to be long eventually...
//        buffer = (unsigned char*) malloc(ie.sizeInFile);
        assert(ie.fileIndex >= 0);
        dataFile.seek(ie.fileIndex);
        dataFile.read(buffer);
      }
      
      // The entry is hashed if (a) it is already hashed in the log file,
      // or (b) the hash policy tells us to.   
      boolean hashIt = (ie.sizeInFile<0) || (hashPolicy != null && hashPolicy.hashEntry(ie.type, buffer));
      
      // Encode the size of the entry

      if (hashIt) {        
        buffer = ie.contentHash;
      }        

      SnippetEntry entry = new SnippetEntry((byte)ie.type, ie.seq, hashIt, buffer);
      entryList.add(entry);
    }
    
    LogSnippet ret = new LogSnippet(baseHash,entryList);
//    logger.log("serializeRange("+idxFrom+","+idxTo+"):"+ret);
    return ret;
  }
  
  public boolean serializeRange2(long idxFrom, long idxTo, HashPolicy hashPolicy, OutputBuffer outfile) throws IOException {
    assert((0 < idxFrom) && (idxFrom <= idxTo) && (idxTo < numEntries));

    IndexEntry ie;

    // Write base hash value

    pointerAtEnd = false;  
    indexFile.seek((idxFrom-1) * indexFactory.getSerializedSize());
    ie = indexFactory.build(indexFile);
    outfile.write(ie.nodeHash, 0, ie.nodeHash.length);
    
    // Go through entries one by one    
    long previousSeq = -1;
    for (long idx=idxFrom; idx<=idxTo; idx++) {
    
      // Read index entry
    
      ie = indexFactory.build(indexFile);
      if (ie == null) 
        throw new IOException("History read error");
        
      // we're going to write directly to outfile, in the c++ impl it used header[]/bytesInHeader
//      byte[] header = new byte[200];
//      int bytesInHeader = 0;
      
      assert((previousSeq == -1) || (ie.seq > previousSeq));
      
      // This code compresses the common case, and falls back to a longer version if 
      // it can't encode it in the compressed form
      
      // Encode difference to previous sequence number (unless this is the first entry)
      
      if (previousSeq >= 0) {
        if (ie.seq == (previousSeq+1)) {
          outfile.writeByte((byte)0);
//          header[bytesInHeader++] = 0;
        } else {
          long dhigh = (ie.seq/1000) - (previousSeq/1000);
          if ((dhigh < 255) && ((ie.seq%1000)==0)) {
            outfile.writeByte((byte)(dhigh & 0xFF));
//            header[bytesInHeader++] = (byte)(dhigh & 0xFF);
          } else {
            outfile.writeByte((byte)0xFF);
//            header[bytesInHeader++] = (byte)0xFF;
            outfile.writeLong(ie.seq);
//          *(long*)&header[bytesInHeader] = ie.seq; 
//            bytesInHeader += sizeof(long long);
          }
        }
      }
      
      previousSeq = ie.seq;
      
      // Append entry type
      
      outfile.writeShort(ie.type);
      //header[bytesInHeader++] = ie.type;
      
      // If entry is not hashed, read contents from the data file       
      byte[] buffer = null;
      if (ie.sizeInFile > 0) {
        buffer = new byte[(int)ie.sizeInFile]; // grumble...  This needs to be long eventually...
//        buffer = (unsigned char*) malloc(ie.sizeInFile);
        assert(ie.fileIndex >= 0);
        dataFile.seek(ie.fileIndex);
        dataFile.read(buffer);
      }
      
      // The entry is hashed if (a) it is already hashed in the log file,
      // or (b) the hash policy tells us to.   
      boolean hashIt = (ie.sizeInFile<0) || (hashPolicy != null && hashPolicy.hashEntry(ie.type, buffer));

      // Encode the size of the entry

      if (hashIt) {
        outfile.writeByte((byte)0);
//        header[bytesInHeader++] = 0;
        outfile.write(ie.contentHash,0,ie.contentHash.length); 
//        for (int i=0; i<hashProv.getSizeOfHash(); i++)
//          header[bytesInHeader++] = ie.contentHash[i];
      } else if (ie.sizeInFile < 255) {
        outfile.writeByte((byte)(ie.sizeInFile & 0xFF));
//        header[bytesInHeader++] = (unsigned char) ie.sizeInFile;
      } else if (ie.sizeInFile < 65536) {
        outfile.writeByte((byte)0xFF);
//        header[bytesInHeader++] = 0xFF;
        outfile.writeShort((short)(ie.sizeInFile & 0xFFFF));
//        *(unsigned short*)&header[bytesInHeader] = (unsigned short) ie.sizeInFile;
//        bytesInHeader += sizeof(unsigned short);
      } else {
//  throw new RuntimeException("A");    
        outfile.writeByte((byte)0xFE);
        //header[bytesInHeader++] = 0xFE;
        outfile.writeLong(ie.sizeInFile);
//        *(unsigned int*)&header[bytesInHeader] = (unsigned int) ie.sizeInFile;
//        bytesInHeader += sizeof(unsigned int);
      }
      
      // Write the entry to the output file
      
      // don't need to do this in this impl, becuase we've been doing it all along
//      fwrite(&header, bytesInHeader, 1, outfile);

      if (!hashIt) { 
        outfile.write(buffer,0,buffer.length);
        //fwrite(buffer, ie.sizeInFile, 1, outfile);
      }        
    } // for

    return true;
  }
  
  /**
   *  Retrieve information about a given record 
   *  
   *  @param idx the index you are interested in
   */
  public IndexEntry statEntry(long idx) throws IOException {
    if ((idx < 0) || (idx >= numEntries))
      return null;
      
    IndexEntry ie;
    
    pointerAtEnd = false;
    indexFile.seek(idx*indexFactory.getSerializedSize());
    ie = indexFactory.build(indexFile);
    
    return ie;
  }

  /**
   *  Get the content of a log entry, specified by its record number 
   */
  public byte[] getEntry(long idx, int maxSizeToRead) throws IOException {
    return getEntry(statEntry(idx), maxSizeToRead);
  }
  
  public byte[] getEntry(IndexEntry ie, int maxSizeToRead) throws IOException {
    if (ie == null) return null;
    
    if (ie.sizeInFile < 0) return null;
    
    try {
      dataFile.seek(ie.fileIndex);
    } catch (IOException ioe) {
      logger.log("fileIndex:"+ie.fileIndex+" "+ie);
      throw ioe;
    }
    int bytesToRead = (maxSizeToRead>=ie.sizeInFile) ? ie.sizeInFile : maxSizeToRead;
    
    byte[] ret = new byte[bytesToRead];
    dataFile.read(ret);
    
    return ret;
  }
  
  /**
   * If the log already contains an entry in 'hashed' form and we learn the actual
   * contents later, this function is called. 
   */
  public boolean upgradeHashedEntry(int idx, ByteBuffer entry) throws IOException {
    if (readOnly)
      throw new IllegalStateException("Cannot upgrade hashed entry in readonly history");
    
    if ((idx<0) || (idx>=numEntries))
      return false;
      
    pointerAtEnd = false;
    
    indexFile.seek(idx*indexFactory.getSerializedSize());
    
    IndexEntry ie = indexFactory.build(indexFile);

    if (ie == null)
      return false;
      
    if (ie.sizeInFile >= 0)
      return false;
    
    dataFile.seek(dataFile.length());

    ie.fileIndex = dataFile.getFilePointer();
    ie.sizeInFile = entry.remaining();
    
    dataFile.write(entry.array(), entry.position(), entry.remaining());

    indexFile.seek(idx*indexFactory.getSerializedSize());
    ie.serialize(indexFile);
    
    return true;
  }

  /** 
   * Find the most recent entry whose type is in the specified set. Useful e.g. for
   * locating the last CHECKPOINT or INIT entry. 
   */
  public long findLastEntry(short[] types, long maxSeq) throws IOException {
    long maxIdx = findSeqOrHigher(Math.min(maxSeq, topEntry.seq), true);
    long currentIdx = maxIdx;
   
    while (currentIdx >= 0) {
      IndexEntry ie = statEntry(currentIdx);

      if (ie == null) throw new IllegalStateException("Cannot stat history entry #"+currentIdx+" (num="+numEntries+")");
       
      for (int i=0; i<types.length; i++)
        if (ie.type == types[i])
          return currentIdx;
         
      currentIdx--;
    }
    return -1;
  }

  public void appendSnippetToHistory(LogSnippet snippet) throws IOException {
//    long currentSeq = snippet.getFirstSeq();
    for (SnippetEntry sEntry : snippet.entries) {
//    int readptr = 0;
//
//    while (readptr < snippetLen) {
//      unsigned char entryType = snippet[readptr++];
//      unsigned char sizeCode = snippet[readptr++];
//      unsigned int entrySize = sizeCode;
//      bool entryIsHashed = (sizeCode == 0);
//
//      if (sizeCode == 0xFF) {
//        entrySize = *(unsigned short*)&snippet[readptr];
//        readptr += 2;
//      } else if (sizeCode == 0xFE) {
//        entrySize = *(unsigned int*)&snippet[readptr];
//        readptr += 4;
//      } else if (sizeCode == 0) {
//        entrySize = transport->getHashSizeBytes();
//      }

//  #ifdef VERBOSE
//      plog(3, "Entry type %d, size=%d, seq=%lld", entryType, entrySize, currentSeq);
//  #endif
//      logger.log("ZZZ "+sEntry);
      
      if (sEntry.seq > getLastSeq()) {
        if (!setNextSeq(sEntry.seq))
          throw new RuntimeException("Audit: Cannot set history sequence number?!?");
          
        if (!sEntry.isHash) {
          appendEntry(sEntry.type, true, ByteBuffer.wrap(sEntry.content));
        } else { 
          appendHash(sEntry.type, sEntry.content);
        }
      } else {
//  #ifdef VERBOSE
//        if (currentSeq >= skipEverythingBeforeSeq)
//          warning("Skipped entry because it is already present (top=%lld)", history->getLastSeq());
//  #endif
      }

//      readptr += entrySize;
//      if (readptr == snippetLen) // legitimate end
//        break;

//      unsigned char dseqCode = snippet[readptr++];
//      if (dseqCode == 0xFF) {
//        currentSeq = *(long long*)&snippet[readptr];
//        readptr += sizeof(long long);
//      } else if (dseqCode == 0) {
//        currentSeq ++;
//      } else {
//        currentSeq = currentSeq - (currentSeq%1000) + (dseqCode * 1000LL);
//      }
//
//      assert(readptr <= snippetLen);
    }
  }
  
  
}
