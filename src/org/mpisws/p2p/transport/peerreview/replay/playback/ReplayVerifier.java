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
package org.mpisws.p2p.transport.peerreview.replay.playback;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.replay.EventCallback;
import org.mpisws.p2p.transport.peerreview.replay.Verifier;
import org.mpisws.p2p.transport.util.Serializer;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

public abstract class ReplayVerifier<Identifier> implements Verifier<Identifier>,  PeerReviewConstants {

  protected Identifier localHandle;
  protected SecureHistory history;
  PeerReviewCallback<Identifier, ?> app;
  int numEventCallbacks;
  boolean foundFault;
  
  boolean haveNextEvent;
  long nextEventIndex;
  IndexEntry next;
  boolean nextEventIsHashed;
  InputBuffer nextEvent;
  
  boolean initialized;
  short signatureSizeBytes;
  short hashSizeBytes;
  int numTimers;
  int[] eventToCallback = new int[256];
  protected Logger logger;
  protected Serializer<Identifier> serializer; // was transport in c++ impl
  protected HashProvider hashProv;
  
  // these are shortcuts in the Java impl, they would all be true in the c++ impl, but in some cases it's more efficient if we can turn them off
  boolean useSendSign = false;  // true if we're sending the signature after the message
  boolean useSenderSeq = false;
  boolean useLogHashFlag = false;
//  boolean useBeginInitialized = true;
  
  public ReplayVerifier(      
      Serializer<Identifier> serializer, 
      HashProvider hashProv,
      SecureHistory history, 
      Identifier localHandle, 
      short signatureSizeBytes, 
      short hashSizeBytes, 
      int firstEntryToReplay, 
      Logger logger) /* : ReplayWrapper() */ throws IOException {
    this.logger = logger;
    this.history = history;
//    this->app = NULL;
    this.numEventCallbacks = 0;
    this.serializer = serializer;
    this.hashProv = hashProv;
    this.localHandle = localHandle;
    this.foundFault = false;
    this.haveNextEvent = false;
    this.nextEventIndex = firstEntryToReplay-1;
    this.initialized = false;
//    if (useBeginInitialized) this.initialized = true;
    this.signatureSizeBytes = signatureSizeBytes;
    this.hashSizeBytes = hashSizeBytes;
    this.numTimers = 0;
    
    for (int i=0; i<256; i++)
      eventToCallback[i] = -1;
      
    fetchNextEvent();
    if (!haveNextEvent)
      foundFault = true;
  }
  
  public boolean verifiedOK() { 
    return !foundFault; 
  };


  public IndexEntry getNextEvent() {
    return next;
  }
  
  /**
   * Callback when a message has arrived.
   * 
   * @param from
   * @param msg
   * @throws IOException
   */
  protected abstract void receive(Identifier from, ByteBuffer msg) throws IOException;
  
  /**
   * Callback when a socket comes in from a remote node.
   * 
   * @param from
   * @param socketId
   * @throws IOException
   */
  protected abstract void incomingSocket(Identifier from, int socketId) throws IOException;
  
  /**
   * Callback when a socket is ready to read/write.
   * 
   * Note that the simulated node should have already registered for this event, even though it is
   * not logged.  If the node is not registred for the event, it is an error.
   * 
   * @param socketId
   * @param canRead
   * @param canWrite
   * @throws IOException
   */
  protected abstract void socketIO(int socketId, boolean canRead, boolean canWrite) throws IOException;
  
  protected abstract void socketOpened(int socketId) throws IOException;

  protected abstract void socketException(int socketId, IOException ioe) throws IOException;

  public void setApplication(PeerReviewCallback app) {
    this.app = app;
  }
  
  /**
   * Fetch the next log entry, or set the EOF flag 
   */
  protected void fetchNextEvent() {
    haveNextEvent = false;
    nextEventIndex++;

//    unsigned char chash[hashSizeBytes];
    try {
      next = history.statEntry(nextEventIndex);
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.logException("Error fetching log entry #"+nextEventIndex,ioe);
      foundFault = true;
      return;
    }

    if (logger.level <= Logger.FINE) logger.log("fetchNextEvent():"+next);

    if (next == null)
      return;
      
    if (next.getSizeInFile()<0) {
      // make the nextEvent only the content hash
      
      nextEventIsHashed = true;
      nextEvent = new SimpleInputBuffer(next.getContentHash());
//      nextEventSize = hashSizeBytes;
//      memcpy(nextEvent, chash, hashSizeBytes);
      if (logger.level <= Logger.FINE) logger.log("Fetched log entry #"+nextEventIndex+" (type "+next.getType()+", hashed, seq="+next.getSeq()+")");
    } else {
      // load the nextEvent from the file
      
      nextEventIsHashed = false;
//      assert(nextEventSize < (int)sizeof(nextEvent));
      try {
        nextEvent = new SimpleInputBuffer(history.getEntry(nextEventIndex, next.getSizeInFile()));
      } catch (IOException ioe) {
        if (logger.level <= Logger.WARNING) logger.logException("Error fetching log entry #"+nextEventIndex+" (type "+next.getType()+", size "+next.getSizeInFile()+" bytes, seq="+next.getSeq()+")",ioe);
        foundFault = true;
        return;
      }
      if (logger.level <= Logger.FINE) logger.log("Fetched log entry #"+nextEventIndex+" (type "+next.getType()+", size "+next.getSizeInFile()+" bytes, seq="+next.getSeq()+")");
//      vdump(nextEvent, nextEventSize);
    }
    
    haveNextEvent = true;
  }
  
  /**
   * Called by the state machine when it wants to send a message 
   */
  protected void send(Identifier target, ByteBuffer message, int relevantLen) throws IOException {
    int msgLen = message.remaining();
    int pos = message.position();
    int lim = message.limit();
    
//    assert(!datagram);

    if (relevantLen < 0)
      relevantLen = message.remaining();

//    char buf1[256], buf2[256];
    if (logger.level <= Logger.FINE) logger.log("Verifier::send("+target+", "+relevantLen+"/"+message.remaining()+" bytes)");
    //vdump(message, msglen);
    
    // Sanity checks 
    
    if (!haveNextEvent) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: Send event after end of segment; marking as invalid");
      foundFault = true;
      return;
    }
    
    if (next.getType() == EVT_INIT) {
      if (logger.level <= Logger.FINER) logger.log("Skipped; next event is an INIT");
      return;
    }
    
    if (next.getType() != EVT_SEND) {
      if (logger.level <= Logger.WARNING) logger.log("Replay("+nextEventIndex+"): SEND event during replay, but next event in log is #"+next.getType()+"; marking as invalid");
      foundFault = true;
      return;
    }

    // If the SEND is hashed, simply compare it to the predicted entry
    
    if (nextEventIsHashed) {
      SimpleOutputBuffer buf = new SimpleOutputBuffer();
//      // this code serializes the target to buf
//      assert(relevantLen < 1024);
//      //unsigned char buf[MAX_ID_SIZE+1+1024+hashSizeBytes];
//      int pos = 0;
      serializer.serialize(target, buf);
//      buf.write(bb.array(), bb.position(), bb.remaining());
//      target->getIdentifier()->write(buf, &pos, sizeof(buf));
//      buf[pos++] = (relevantlen<msglen) ? 1 : 0;
      if (useLogHashFlag) buf.writeBoolean(relevantLen<msgLen);
//      if (relevantlen>0) {
//        memcpy(&buf[pos], message, relevantlen);
//        pos += relevantlen;
//      }
//      
//      // this code serializes the message
      buf.write(message.array(), message.position(), relevantLen);
      
//      assert(pos<(sizeof(buf)-hashSizeBytes));
      if (relevantLen<msgLen) {        
  // ugly; this should be an argument
//        if (msglen == (relevantlen+hashSizeBytes))
//          memcpy(&buf[pos], &message[relevantlen], hashSizeBytes);
//        else
        message.position(pos);
        message.limit(lim);
        byte[] hash = hashProv.hash(message);
//          
//        pos += hashSizeBytes;
      }
//      
//      // this code serializes the contentHash
//      unsigned char chash[hashSizeBytes];
//      hash(chash, buf, pos);
      byte[] cHash = hashProv.hash(ByteBuffer.wrap(buf.getBytes()));
      if (!cHash.equals(next.getContentHash())) {
//      if (memcmp(chash, nextEvent, hashSizeBytes)) {
        if (logger.level <= Logger.WARNING) logger.log("Replay: SEND is hashed, but hash of predicted SEND entry does not match hash in the log");
        foundFault = true;
        return;
      }

      if (useSendSign) {
        fetchNextEvent();
        assert(next.getType() == EVT_SENDSIGN);
      }
      fetchNextEvent();
      return;
    }

    // Are we sending to the same destination? 
    Identifier logReceiver;
//    try {
     logReceiver = serializer.deserialize(nextEvent);
//    } catch (IllegalArgumentException iae) {
//      if (logger.level <= Logger.WARNING) logger.log("Error deserializing event "+nextEventIndex+". send("+target+","+message+")");
//      throw iae;
//    }
    if (!logReceiver.equals(target)) {
      if (logger.level <= Logger.WARNING) logger.log("Replay("+nextEventIndex+"): SEND to "+target+" during replay, but log shows SEND to "+logReceiver+"; marking as invalid");      
      // reset nextEvent so next time we parse this event it isn't nonsense
      nextEvent = new SimpleInputBuffer(history.getEntry(next, next.getSizeInFile()));
      foundFault = true;
      return;
    }
    
    // Check the message against the message in the log
    boolean logIsHashed = false; 
    if (useLogHashFlag) logIsHashed = nextEvent.readBoolean();

    if (logIsHashed) {
      if (relevantLen >= msgLen) {
        if (logger.level <= Logger.WARNING) logger.log("Replay: Message sent during replay is entirely relevant, but log entry is partly hashed; marking as invalid");
        foundFault = true;
        return;
      }
      
      int logRelevantLen = nextEvent.bytesRemaining() - hashSizeBytes;
      assert(logRelevantLen >= 0);
      
      if (relevantLen != logRelevantLen) {
        if (logger.level <= Logger.WARNING) logger.log("Replay: Message sent during replay has "+relevantLen+" relevant bytes, but log entry has "+logRelevantLen+"; marking as invalid");
        foundFault = true;
        return;
      }
            
      byte[] loggedMsg = new byte[logRelevantLen];
      nextEvent.read(loggedMsg);
      ByteBuffer loggedMsgBB = ByteBuffer.wrap(loggedMsg);
      if ((relevantLen > 0) && message.equals(loggedMsgBB)) {
        if (logger.level <= Logger.WARNING) logger.log("Replay: Relevant part of partly hashed message differs");
        if (logger.level <= Logger.FINE) logger.log("Expected: ["+loggedMsgBB+"]");
        if (logger.level <= Logger.FINE) logger.log("Actual:   ["+message+"]");
        foundFault = true;
        return;
      }
      
      byte[] logHash = new byte[hashSizeBytes]; 
      nextEvent.read(logHash);
      byte[] msgHashBytes = message.array();
      byte[] msgHash = new byte[hashSizeBytes];
      System.arraycopy(msgHashBytes, msgHashBytes.length-hashSizeBytes, msgHash, 0, hashSizeBytes);
      assert(msgLen == (relevantLen + hashSizeBytes));
      if (!msgHash.equals(logHash)) {
        if (logger.level <= Logger.WARNING) logger.log("Replay: Hashed part of partly hashed message differs");
        if (logger.level <= Logger.FINE) logger.log("Expected: ["+logHash+"]");
        if (logger.level <= Logger.FINE) logger.log("Actual:   ["+msgHash+"]");
        foundFault = true;
        return;
      }
    } else {
      if (relevantLen < msgLen) {
        if (logger.level <= Logger.WARNING) logger.log("Replay: Message sent during replay is only partly relevant, but log entry is not hashed; marking as invalid");
        foundFault = true;
        return;
      }

      int logMsglen = nextEvent.bytesRemaining();
      if (msgLen != logMsglen) {
        if (logger.level <= Logger.WARNING) logger.log("Replay: Message sent during replay has "+msgLen+" bytes, but log entry has "+logMsglen+"; marking as invalid");
        foundFault = true;
        return;
      }
      
      byte[] loggedMsg = new byte[nextEvent.bytesRemaining()];
      nextEvent.read(loggedMsg);
      byte[] sentMsg = new byte[message.remaining()];
      message.get(sentMsg);
      
      if (loggedMsg.length != sentMsg.length) {
        if (logger.level <= Logger.WARNING) logger.log("Replay: Message sent during replay differs from message in the log by length log:"+loggedMsg.length+" sent:"+sentMsg.length);
        foundFault = true;
        return;        
      }
      
//      nextEvent.read(loggedMsg);
//      ByteBuffer loggedMsgBB = ByteBuffer.wrap(loggedMsg);
      if ((msgLen > 0) && !Arrays.equals(loggedMsg, sentMsg)) {
        if (logger.level <= Logger.WARNING) logger.log("Replay: Message sent during replay differs from message in the log");
        foundFault = true;
        return;
      }
    }

    if (useSendSign) {
      fetchNextEvent();
      assert(next.getType() == EVT_SENDSIGN);
    }
    fetchNextEvent();
  }
  
  /**
   * Return the new socketId
   * @param i
   * @return the new socketId, Integer.MIN_VALUE for an error
   */
  public int openSocket(Identifier target) throws IOException {
    if (!haveNextEvent) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: OpenSocket event after end of segment; marking as invalid");
      foundFault = true;
      return Integer.MIN_VALUE;
    }

    if (next.getType() != EVT_SOCKET_OPEN_OUTGOING) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: SOCKET_OPEN_OUTGOING event during replay, but next event in log is #"+next.getType()+"; marking as invalid");
      foundFault = true;
      return Integer.MIN_VALUE;
    }

    int ret = nextEvent.readInt(); 
    
    Identifier logReceiver;
    logReceiver = serializer.deserialize(nextEvent);
    if (!logReceiver.equals(target)) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: SOCKET_OPEN_OUTGOING to "+target+" during replay, but log shows SOCKET_OPEN_OUTGOING to "+logReceiver+"; marking as invalid");
      foundFault = true;
      return Integer.MIN_VALUE;
    }

    fetchNextEvent();
    
    return ret;
  }
  
  /**
   * Return the bytes read.
   * 
   * @param socketId
   * @return number of bytes read
   */
  public int readSocket(int socketId, ByteBuffer dst) throws IOException {
//    logger.log("readSocket("+socketId+","+dst+")");
    if (!haveNextEvent) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: ReadSocket event after end of segment; marking as invalid");
      foundFault = true;
      return 0;
    }
    
    if (next.getType() == EVT_SOCKET_CLOSED) {
      fetchNextEvent();
      return -1;
    }
    
    if (next.getType() != EVT_SOCKET_READ) {
      if (logger.level <= Logger.WARNING) logger.logException("Replay ("+nextEventIndex+"): SOCKET_READ event during replay, but next event in log is #"+next.getType()+"; marking as invalid", new Exception("Stack Trace"));
      foundFault = true;
      return Integer.MIN_VALUE;
    }

    int loggedSocket = nextEvent.readInt();
    if (loggedSocket != socketId) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: SOCKET_READ on socket "+socketId+" during replay, but log shows SOCKET_READ to "+loggedSocket+"; marking as invalid");
      foundFault = true;
      return 0;
    }
    
    // TODO: Change this when we make multiple reads a single event
    int ret = nextEvent.bytesRemaining();
    if (dst.remaining() < ret) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: SOCKET_READ reading a maximum of "+dst.remaining()+" on socket "+socketId+" during replay, but log shows SOCKET_READ reading "+ret+" bytes; marking as invalid");
      foundFault = true;
      return 0;
    }
    
    nextEvent.read(dst.array(), dst.position(), ret);
    dst.position(dst.position()+ret);
    fetchNextEvent();
    return ret;
  }
  
  public void generatedSocketException(int socketId, IOException ioe) {
    if (!haveNextEvent) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: WriteSocket event after end of segment; marking as invalid");
      foundFault = true;
      return;
    }
    
    if (next.getType() != EVT_SOCKET_EXCEPTION) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: EVT_SOCKET_EXCEPTION event during replay, but next event in log is #"+next.getType()+"; marking as invalid");
      foundFault = true;
      return;
    }

    try {
      int loggedSocket = nextEvent.readInt();
      if (loggedSocket != socketId) {
        if (logger.level <= Logger.WARNING) logger.log("Replay: EVT_SOCKET_EXCEPTION on socket "+socketId+" during replay, but log shows EVT_SOCKET_EXCEPTION to "+loggedSocket+"; marking as invalid");
        foundFault = true;
        return;
      }
    } catch (IOException ioe2) {
      if (logger.level <= Logger.WARNING) logger.logException("Replay: Error reading log", ioe2);      
    }
    
    // all good
    fetchNextEvent();
    return;
    
  }
  
  /**
   * Return the bytes written.
   * 
   * @param socketId
   * @return number of bytes written
   */
  public int writeSocket(int socketId, ByteBuffer src) throws IOException {
    if (!haveNextEvent) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: WriteSocket event after end of segment; marking as invalid");
      foundFault = true;
      return 0;
    }
    
    if (next.getType() == EVT_SOCKET_CLOSED) {
      fetchNextEvent();
      return -1;
    }
    
    if (next.getType() != EVT_SOCKET_WRITE) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: SOCKET_WRITE event during replay, but next event in log is #"+next.getType()+"; marking as invalid");
      foundFault = true;
      return Integer.MIN_VALUE;
    }

    int loggedSocket = nextEvent.readInt();
    if (loggedSocket != socketId) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: SOCKET_WRITE on socket "+socketId+" during replay, but log shows SOCKET_WRITE to "+loggedSocket+"; marking as invalid");
      foundFault = true;
      return 0;
    }
    
    // TODO: Change this when we make multiple reads a single event
    int ret = nextEvent.bytesRemaining();
    if (src.remaining() < ret) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: SOCKET_WRITE writing a maximum of "+src.remaining()+" on socket "+socketId+" during replay, but log shows SOCKET_WRITE writing "+ret+" bytes; marking as invalid");
      foundFault = true;
      return 0;
    }
    
    byte[] loggedMsg = new byte[ret];
    byte[] sentMsg = new byte[ret];
    nextEvent.read(loggedMsg);
    src.get(sentMsg);

    if (!Arrays.equals(loggedMsg, sentMsg)) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: Message wrote during replay differs from message in the log");
      foundFault = true;
      return 0;
    }
    
    fetchNextEvent();
    return ret;
  }
  
  public void close(int socketId) {
    if (!haveNextEvent) {
      if (logger.level <= Logger.WARNING) logger.log("Replay("+nextEventIndex+"): SOCKET_CLOSE event after end of segment; marking as invalid");
      foundFault = true;
      return;
    }
    
    if (next.getType() != EVT_SOCKET_CLOSE) {
      if (logger.level <= Logger.WARNING) logger.log("Replay("+nextEventIndex+"): SOCKET_CLOSE event during replay, but next event in log is #"+next.getType()+"; marking as invalid");
      foundFault = true;
      return;
    }

    int loggedSocket;
    try {
      loggedSocket = nextEvent.readInt();
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: Error deserializing event "+next);
      foundFault = true;
      return;
    }

    if (loggedSocket != socketId) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: SOCKET_CLOSE on socket "+socketId+" during replay, but log shows SOCKET_CLOSE to "+loggedSocket+"; marking as invalid");
      foundFault = true;
      return;
    }
    
    fetchNextEvent();
  }
  
  public void shutdownOutput(int socketId) {
    if (!haveNextEvent) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: EVT_SOCKET_SHUTDOWN_OUTPUT event after end of segment; marking as invalid");
      foundFault = true;
      return;
    }
    
    if (next.getType() != EVT_SOCKET_SHUTDOWN_OUTPUT) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: EVT_SOCKET_SHUTDOWN_OUTPUT event during replay, but next event in log is #"+next.getType()+"; marking as invalid");
      foundFault = true;
      return;
    }

    int loggedSocket;
    try {
      loggedSocket = nextEvent.readInt();
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: Error deserializing event "+next);
      foundFault = true;
      return;
    }

    if (loggedSocket != socketId) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: EVT_SOCKET_SHUTDOWN_OUTPUT on socket "+socketId+" during replay, but log shows EVT_SOCKET_SHUTDOWN_OUTPUT to "+loggedSocket+"; marking as invalid");
      foundFault = true;
      return;
    }
    
    fetchNextEvent();
  }
  

  /**
   * Maps EVT_XXX -> EventCallback
   */
  Map<Short, EventCallback> eventCallback = new HashMap<Short, EventCallback>();
  
  /**
   * This binds specific event types to one of the handlers 
   */
  public void registerEvent(EventCallback callback, short... eventType) {
    for (short s : eventType) {
      registerEvent(callback, s);
    }
  }
  
  public void registerEvent(EventCallback callback, short eventType) {
    if (eventCallback.containsKey(eventType)) {
      if (callback != eventCallback.get(eventType)) throw new IllegalStateException("Event #"+eventType+" registered twice");
    }
    eventCallback.put(eventType,callback);
  }
  
  /**
   * This is called by the Audit protocol to make another replay step; it returns true
   * if further calls are necessary, and false if the replay has finished. The idea
   * is that we can stop calling this if there is more important work to do, e.g. 
   * handle foreground requests 
   */
  public boolean makeProgress() {
    if (logger.level <= Logger.FINE) logger.log("makeProgress()");
    if (foundFault || !haveNextEvent)
      return false;
      
    if (!initialized && (next.getType() != EVT_CHECKPOINT) && (next.getType() != EVT_INIT)) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: No INIT or CHECKPOINT found at the beginning of the log; marking as invalid "+next);
      foundFault = true;
      return false;
    }
    
    /**
     * Handle any pending timers. Note that we have to be sure to call them in the exact same
     * order as in the main code; otherwise there can be subtle bugs and side-effects. 
     */    
    // This code is the job of the SelectorManager, it's done in the super class of ReplaySM
    
    if (!haveNextEvent)
      return false;  
    
    /* Sanity checks */

    if (logger.level <= Logger.FINE) logger.log("Replaying event #"+nextEventIndex+" (type "+next.getType()+", seq="+next.getSeq()+")");
      
    if (nextEventIsHashed && (next.getType() != EVT_CHECKPOINT) && (next.getType() != EVT_INIT)) {
      if (logger.level <= Logger.WARNING) logger.log("Replay: Trying to replay hashed event");
      foundFault = true;
      return false;
    }
      
    /* Replay the next event */

    try {
      switch (next.getType()) {
      case EVT_SEND : /* SEND events should have been handled by Verifier::send() */
//        if (logger.level <= Logger.FINE) logger.log("Replay: Encountered EVT_SEND, waiting for node.");
        if (logger.level <= Logger.WARNING) logger.logException("Replay: Encountered EVT_SEND evt #"+nextEventIndex+"; marking as invalid", new Exception("Stack Trace"));
//        transport->dump(2, nextEvent, next.getSizeInFile());
        foundFault = true;
        return false;
      case EVT_SOCKET_READ: {
        if (logger.level <= Logger.WARNING) logger.logException("Replay: Encountered EVT_SOCKET_READ evt #"+nextEventIndex+"; marking as invalid", new Exception("Stack Trace"));
        foundFault = true;
        return false;        
      }
      case EVT_SOCKET_CLOSE: {
        if (logger.level <= Logger.WARNING) logger.logException("Replay: Encountered EVT_SOCKET_CLOSE evt #"+nextEventIndex+"; marking as invalid", new Exception("Stack Trace"));
        foundFault = true;
        return false;        
      }
      case EVT_SOCKET_SHUTDOWN_OUTPUT: {
        if (logger.level <= Logger.WARNING) logger.logException("Replay: Encountered EVT_SOCKET_SHUTDOWN_OUTPUT evt #"+nextEventIndex+"; marking as invalid", new Exception("Stack Trace"));
        foundFault = true;
        return false;        
      }
        
      case EVT_RECV : /* Incoming message; feed it to the state machine */
        Identifier sender = serializer.deserialize(nextEvent);

        long senderSeq = 0;
        if (useSenderSeq) senderSeq = nextEvent.readLong();
        boolean hashed = false;
        if (useLogHashFlag) hashed = nextEvent.readBoolean();
        
        int msgLen = nextEvent.bytesRemaining();
        int relevantLen = hashed ? (msgLen-hashSizeBytes) : msgLen;

//        unsigned char *msgbuf = (unsigned char*) malloc(msglen);
//        memcpy(msgbuf, &nextEvent[headerSize], msglen);
        
        byte[] msgBytes = new byte[msgLen];
        nextEvent.read(msgBytes);
        ByteBuffer msgBuf = ByteBuffer.wrap(msgBytes);
        
        /* The next event is going to be a SIGN; skip it, since it's irrelevant here */

        if (useSendSign) {
          fetchNextEvent();
          if (!haveNextEvent || (next.getType() != EVT_SIGN) || (next.getSizeInFile() != (int)(hashSizeBytes+signatureSizeBytes))) {
            if (logger.level <= Logger.WARNING) logger.log("Replay: RECV event not followed by SIGN; marking as invalid");
            foundFault = true;
            return false;
          }
        }
        
        fetchNextEvent();
        
        /* Deliver the message to the state machine */
        
//        app->receive(sender, false, msgbuf, msglen);
        receive(sender, msgBuf);
        break;
      case EVT_SIGN : /* SIGN events should have been handled by the preceding RECV */
        if (logger.level <= Logger.WARNING) logger.log("Replay: Spurious SIGN event; marking as invalid");
        foundFault = true;
        return false;
      case EVT_ACK : /* Skip ACKs */
  // warning there should be an upcall here
        fetchNextEvent();
        break;
      case EVT_SENDSIGN : /* Skip SENDSIGN events; they are not relevant during replay */
        fetchNextEvent();
        break;
      case EVT_CHECKPOINT : /* Verify CHECKPOINTs */
//        if (!initialized) {
//          if (!nextEventIsHashed) {
//          
//            /* If the state machine hasn't been initialized yet, we can use this checkpoint */
//          
//            initialized = true;
//            if (!app.loadCheckpoint(nextEvent, next.getSizeInFile())) {
//              if (logger.level <= Logger.WARNING) logger.log("Cannot load checkpoint");
//              foundFault = true;
//            }
//          } else {
//            if (logger.level <= Logger.WARNING) logger.log("Replay: Initial checkpoint is hashed; marking as invalid");
//            foundFault = true;
//          }
//        } else {
//        
//          /* Ask the state machine to do a checkpoint now ... */
//        
//          int maxlen = 1048576*4;
//          unsigned char *buf = (unsigned char *)malloc(maxlen);
//          int actualCheckpointSize = app->storeCheckpoint(buf, maxlen);
//
//          /* ... and compare it to the contents of the CHECKPOINT entry */
//
//          if (!nextEventIsHashed) {
//            if (actualCheckpointSize != nextEventSize) {
//              if (logger.level <= Logger.WARNING) logger.log("Replay: Checkpoint has different size (expected %d bytes, but got %d); marking as invalid", nextEventSize, actualCheckpointSize);
//              plog(2, "Expected:");
//              transport->dump(2, nextEvent, nextEventSize);
//              plog(2, "Found:");
//              transport->dump(2, buf, actualCheckpointSize);
//              foundFault = true;
//              free(buf);
//              return false;
//            }
//          
//            if (memcmp(buf, nextEvent, nextEventSize) != 0) {
//              if (logger.level <= Logger.WARNING) logger.log("Replay: Checkpoint does not match");
//              plog(2, "Expected:");
//              transport->dump(2, nextEvent, nextEventSize);
//              plog(2, "Found:");
//              transport->dump(2, buf, nextEventSize);
//
//              foundFault = true;
//              free(buf);
//              return false;
//            }
//          } else {
//            if (nextEventSize != hashSizeBytes) {
//              if (logger.level <= Logger.WARNING) logger.log("Replay: Checkpoint is hashed but has the wrong length?!?");
//              foundFault = true;
//              free(buf);
//              return false;
//            }
//          
//            unsigned char checkpointHash[hashSizeBytes];
//            hash(checkpointHash, buf, actualCheckpointSize);
//            if (memcmp(checkpointHash, nextEvent, hashSizeBytes) != 0) {
//              if (logger.level <= Logger.WARNING) logger.log("Replay: Checkpoint is hashed, but does not match hash value in the log");
//              foundFault = true;
//              free(buf);
//              return false;
//            }
//
//            vlog(4, "Hashed checkpoint is OK");
//            history->upgradeHashedEntry(nextEventIndex, buf, actualCheckpointSize);
//          }
//          
//          free(buf);
//        }
//          
//        fetchNextEvent();
//        break;
      case EVT_INIT: /* State machine is reinitialized; issue upcall */
        initialized = true;
//        app->init();
        fetchNextEvent();
        break;
      case EVT_SOCKET_OPEN_INCOMING: {
//        logger.log(next+" s:"+nextEvent.bytesRemaining());
        int socketId = nextEvent.readInt();
        Identifier opener = serializer.deserialize(nextEvent);
        fetchNextEvent();
        incomingSocket(opener, socketId);          
        break;
      }
      case EVT_SOCKET_OPENED_OUTGOING: {
        int socketId = nextEvent.readInt();
        fetchNextEvent();
        socketOpened(socketId);
        break;
      }
      case EVT_SOCKET_CAN_READ: {
        int socketId = nextEvent.readInt();
        fetchNextEvent();
        socketIO(socketId, true, false);
        break;
      }
      case EVT_SOCKET_CAN_WRITE: {
        int socketId = nextEvent.readInt();
        fetchNextEvent();
        socketIO(socketId, false, true);
        break;
      }
      case EVT_SOCKET_CAN_RW: {
        int socketId = nextEvent.readInt();
        fetchNextEvent();
        socketIO(socketId, true, true);
        break;
      }
      case EVT_SOCKET_EXCEPTION: {
        int socketId = nextEvent.readInt();
        IOException ex = deserializeException(nextEvent);
        logger.log("deserializeException("+ex+")");
        fetchNextEvent();
        socketException(socketId, ex);
        break;
      }
      default:
        if (!eventCallback.containsKey(next.getType())) {
          if (logger.level <= Logger.WARNING) logger.log("Replay("+nextEventIndex+"): Unregistered event #"+next.getType()+"; marking as invalid");
          foundFault = true;
          return false;
        }

        IndexEntry temp = next;
        InputBuffer tempEvent = nextEvent;
        fetchNextEvent();
        eventCallback.get(temp.getType()).replayEvent(temp.getType(), tempEvent);
        break;
      }// switch
    } catch (IOException ioe) {
      if (logger.level <= Logger.WARNING) logger.logException("Exception handling event #"+nextEventIndex+" "+next,ioe);
      foundFault = true;
      return false;
    }
    
    return true;
  }

  public long getNextEventTime() {
    return next.getSeq()/1000000;
  }
  
  public boolean isSuccess() {
    if (initialized && verifiedOK()) {
      if (next == null) return true;
    }
//    logger.log("i:"+initialized+" v:"+verifiedOK()+" n:"+nextEvent);
    return false;
  }
  
  @SuppressWarnings("unchecked")
  protected IOException deserializeException(InputBuffer nextEvent) throws IOException {
    short exType = nextEvent.readShort();
    switch (exType) {
    case EX_TYPE_IO:
      return new IOException(nextEvent.readUTF());
    case EX_TYPE_ClosedChannel:
      return new ClosedChannelException(nextEvent.readUTF());
    case EX_TYPE_Unknown:
      String className = nextEvent.readUTF();
      String message = nextEvent.readUTF();     
      Class c;
      try {
        c = Class.forName(className);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException("Couldn't find class"+className+" "+message);                  
      }
      
      Class[] parameterTypes = new Class[1];
      parameterTypes[0] = String.class;      
      try {
        Constructor ctor = c.getConstructor(parameterTypes);
        IOException ioe = (IOException)ctor.newInstance(message);
        return ioe;
//      } catch (NoSuchMethodException nsme) {
//      } catch (IllegalAccessException iae) {        
//      } catch (InvocationTargetException ite) {
      } catch (Exception e) {
        try {
          Constructor ctor = c.getConstructor(new Class[0]);
          IOException ioe = (IOException)ctor.newInstance(message);
          return ioe;
        } catch (Exception e2) {
          throw new RuntimeException("Couldn't find constructor for"+className+" "+message);          
        }
      }
      // TODO: make sure this is an IOException
//      if (c.getInterfaces()
      
      
    default: throw new RuntimeException("Unknown EX_TYPE:"+exType);
    }
//    return new ClosedChannelException("Replay Exception"); //new IOException();
  } 
}
