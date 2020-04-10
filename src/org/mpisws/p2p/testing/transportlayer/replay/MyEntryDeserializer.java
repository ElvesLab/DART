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
package org.mpisws.p2p.testing.transportlayer.replay;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.mpisws.p2p.transport.peerreview.history.IndexEntry;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.replay.BasicEntryDeserializer;
import org.mpisws.p2p.transport.util.Serializer;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.rawserialization.SimpleInputBuffer;

public class MyEntryDeserializer extends BasicEntryDeserializer implements MyEvents {
  Serializer serializer;

  public MyEntryDeserializer(Serializer serializer) {
    this.serializer = serializer;
  }
  
  @Override
  public String entryId(short id) {
    String ret = super.entryId(id);
    if (ret != null) return ret;
    
    switch (id) {
    case EVT_BOOT: return "Boot";
    case EVT_SUBSCRIBE: return "Subscribe";
    case EVT_PUBLISH: return "Publish";
    default: return null;
    }
  }


  @Override
  public String read(IndexEntry ie, SecureHistory history) throws IOException {
    SimpleInputBuffer nextEvent = null;
    if (ie.getSizeInFile() > 0) nextEvent = new SimpleInputBuffer(history.getEntry(ie, ie.getSizeInFile()));
    switch (ie.getType()) {
    case EVT_SOCKET_OPEN_OUTGOING: {
      int socketId = nextEvent.readInt();      
      byte[] addrBytes = new byte[4];
      nextEvent.read(addrBytes);
      InetSocketAddress addr = new InetSocketAddress(InetAddress.getByAddress(addrBytes), nextEvent.readShort());
      return entryId(ie.getType())+" socketId:"+socketId+" addr:"+addr;
    }
    case EVT_SOCKET_OPENED_OUTGOING: {
      int socketId = nextEvent.readInt();      
      return entryId(ie.getType())+" socketId:"+socketId;
    }
    case EVT_SEND: {
      InputBuffer buf = new SimpleInputBuffer(history.getEntry(ie, ie.getSizeInFile()));
      return entryId(ie.getType())+" n:"+ie.getSeq()+" s:"+ie.getSizeInFile()+" i:"+ie.getFileIndex()+" ->"+serializer.deserialize(buf);  
    }
    default:
      return super.read(ie, history);
    }
  }
}
