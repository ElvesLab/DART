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
package org.mpisws.p2p.transport.peerreview.replay;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactoryImpl;
import org.mpisws.p2p.transport.peerreview.history.reader.EntryDeserializer;
import org.mpisws.p2p.transport.peerreview.history.reader.LogReader;
import org.mpisws.p2p.transport.peerreview.history.stub.NullHashProvider;

import rice.environment.Environment;
import rice.p2p.util.rawserialization.SimpleInputBuffer;

public class BasicEntryDeserializer implements PeerReviewConstants, EntryDeserializer {

  public String entryId(short id) {
    switch (id) {
    case EVT_SEND: return "Send";
    case EVT_RECV: return "Receive";
    case EVT_SIGN: return "Sign";
    case EVT_ACK: return "Ack";
    case EVT_CHECKPOINT: return "Checkpoint";
    case EVT_INIT: return "Init";
    case EVT_SENDSIGN: return "Send_sign";
    
    case EVT_SOCKET_OPEN_INCOMING: return "Socket_open_incoming";
    case EVT_SOCKET_OPEN_OUTGOING: return "Socket_open_outgoing";
    case EVT_SOCKET_OPENED_OUTGOING: return "Socket_opened_outgoing";
    case EVT_SOCKET_EXCEPTION: return "Socket_exception";
    case EVT_SOCKET_CLOSE: return "Socket_close";
    case EVT_SOCKET_CLOSED: return "Socket_closed";
    case EVT_SOCKET_CAN_READ: return "Socket_can_R";
    case EVT_SOCKET_CAN_WRITE: return "Socket_can_W";
    case EVT_SOCKET_CAN_RW: return "Socket_can_RW"; 
    case EVT_SOCKET_READ: return "Socket_R";
    case EVT_SOCKET_WRITE: return "Socket_W";
    case EVT_SOCKET_SHUTDOWN_OUTPUT: return "Socket_shutdown_output";

    default: return null;
    }
  }

  public String read(IndexEntry ie, SecureHistory history) throws IOException {
    if (ie.getType() >= EVT_MIN_SOCKET_EVT && ie.getType() <= EVT_MAX_SOCKET_EVT) {
      return entryId(ie.getType())+" n:"+ie.getSeq()+" i:"+ie.getFileIndex()+" sock:"+new SimpleInputBuffer(history.getEntry(ie, 4)).readInt();
    }
    return entryId(ie.getType())+" n:"+ie.getSeq()+" s:"+ie.getSizeInFile()+" i:"+ie.getFileIndex();
  }

  public static void printLog(String name, EntryDeserializer deserializer, Environment env) throws IOException {
    System.out.println("printLog("+name+")");
    String line;
    
    HashProvider hashProv = new NullHashProvider();
    
    int ctr = 0;
    LogReader reader = new LogReader(name, new SecureHistoryFactoryImpl(hashProv, env), deserializer);
    while ((line = reader.readEntry()) != null) {
      System.out.println("#"+ctr+" "+line);
      ctr++;
    }    
  }
  
  public static final void main(String[] args) throws IOException {
    printLog(args[0], new BasicEntryDeserializer(), new Environment());
  }
  
}
