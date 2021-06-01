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
package org.mpisws.p2p.transport.peerreview.replay.record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.peerreview.PeerReviewConstants;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;

import rice.environment.logging.Logger;
import rice.p2p.util.MathUtils;

public class RecordSocket<Identifier> extends SocketWrapperSocket<Identifier, Identifier> implements PeerReviewConstants {

  int socketId;
  ByteBuffer socketIdBuffer;
  RecordLayer<Identifier> recordLayer;
  boolean closed = false;
  boolean outputShutdown = false;
  
  public RecordSocket(Identifier identifier, P2PSocket<Identifier> socket, Logger logger, Map<String, Object> options, int socketId, ByteBuffer sib, RecordLayer<Identifier> recordLayer) {
    super(identifier, socket, logger, recordLayer.handler, options);
    this.socketId = socketId;
    this.socketIdBuffer = sib;
    this.recordLayer = recordLayer;
  }

  @Override
  public long read(ByteBuffer dsts) throws IOException {
    // remember the position
    int pos = dsts.position();
    
    // read the bytes
    int ret = (int)super.read(dsts);
    
    // do the proper logging
    if (ret < 0) {
      // the socket was closed
      try {
        socketIdBuffer.clear();
        recordLayer.logEvent(EVT_SOCKET_CLOSED, socketIdBuffer);
      } catch (IOException ioe) {
        if (logger.level <= Logger.WARNING) logger.logException(this+".read()",ioe); 
      }      
    } else {    
      // wrap the read bytes with a new BB
      ByteBuffer bits = ByteBuffer.wrap(dsts.array());
      bits.position(pos);
      bits.limit(pos+ret);
  
      try {
        socketIdBuffer.clear();
        recordLayer.logEvent(EVT_SOCKET_READ, socketIdBuffer, bits);
      } catch (IOException ioe) {
        if (logger.level <= Logger.WARNING) logger.logException(this+".read()",ioe); 
      }
    }
    
    return ret;
  }

  @Override
  public long write(ByteBuffer srcs) throws IOException {
    int pos = srcs.position();
    
    // write the bytes
    int ret = (int)super.write(srcs);
    
    // do the proper logging
    if (ret < 0) {
      // the socket was closed
      try {
        socketIdBuffer.clear();
        recordLayer.logEvent(EVT_SOCKET_CLOSED, socketIdBuffer);
      } catch (IOException ioe) {
        if (logger.level <= Logger.WARNING) logger.logException(this+".write()",ioe); 
      }      
    } else {    
      // wrap the read bytes with a new BB
      ByteBuffer bits = ByteBuffer.wrap(srcs.array());
      bits.position(pos);
      bits.limit(pos+ret);
  
      try {
        socketIdBuffer.clear();
        recordLayer.logEvent(EVT_SOCKET_WRITE, socketIdBuffer, bits);
      } catch (IOException ioe) {
        if (logger.level <= Logger.WARNING) logger.logException(this+".write()",ioe); 
      }
    }

    return ret;
  }
  
  @Override
  public void close() {
    try {
      closed = true;
//      logger.logException("close()",new Exception("close()"));
      socketIdBuffer.clear();      
      recordLayer.logEvent(EVT_SOCKET_CLOSE, socketIdBuffer);
    } catch (IOException ioe2) {
      if (logger.level <= Logger.WARNING) logger.logException(this+".receiveException()",ioe2); 
    }        
    super.close();
  }
  
  @Override
  public void shutdownOutput() {
    try {
      outputShutdown = true;
      
//    logger.logException("close()",new Exception("close()"));
      socketIdBuffer.clear();      
      recordLayer.logEvent(EVT_SOCKET_SHUTDOWN_OUTPUT, socketIdBuffer);
    } catch (IOException ioe2) {
      if (logger.level <= Logger.WARNING) logger.logException(this+".receiveException()",ioe2); 
    }        
    super.shutdownOutput();
  }

  @Override
  public void register(boolean wantToRead, boolean wantToWrite, final P2PSocketReceiver<Identifier> receiver) {
    if (closed) {
      receiver.receiveException(this, new ClosedChannelException("Socket "+this+" already closed."));
      return;
    }
    if (wantToWrite && outputShutdown) {
      receiver.receiveException(this, new ClosedChannelException("Socket "+this+" already shutdown output."));
      return;
    }
    
    super.register(wantToRead, wantToWrite, new P2PSocketReceiver<Identifier>(){

      public void receiveSelectResult(P2PSocket<Identifier> socket, boolean canRead, boolean canWrite) throws IOException {
        short evt;
        if (canRead && canWrite) {
          evt = EVT_SOCKET_CAN_RW;
        } else if (canRead) {
          evt = EVT_SOCKET_CAN_READ;
        } else if (canWrite) {
          evt = EVT_SOCKET_CAN_WRITE;            
        } else {
          throw new IOException("I can't read or write. canRead:"+canRead+" canWrite:"+canWrite);
        }
        try {
          socketIdBuffer.clear();
          recordLayer.logEvent(evt, socketIdBuffer);
        } catch (IOException ioe2) {
          if (logger.level <= Logger.WARNING) logger.logException(this+".receiveException()",ioe2); 
        }        
        receiver.receiveSelectResult(RecordSocket.this, canRead, canWrite);
      }
    
      public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
        try {
          socketIdBuffer.clear();
//          logger.logException(this+".register()", ioe);
          recordLayer.logSocketException(socketIdBuffer, ioe);
        } catch (IOException ioe2) {
          if (logger.level <= Logger.WARNING) logger.logException(this+"@"+socketId+".receiveException()",ioe2); 
        }
        receiver.receiveException(socket, ioe);
      }
    });
  }
}
