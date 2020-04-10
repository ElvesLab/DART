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
package org.mpisws.p2p.transport.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;

import rice.environment.logging.Logger;

/**
 * Just maps a socket from one form into another.
 * 
 * @author Jeff Hoye
 *
 * @param <Identifier>
 * @param <SubIdentifier>
 */
public class SocketWrapperSocket<Identifier, SubIdentifier> implements P2PSocket<Identifier>, P2PSocketReceiver<SubIdentifier> {

  protected Identifier identifier;
  protected P2PSocket<SubIdentifier> socket;
  protected Logger logger;
  protected Map<String, Object> options;
  // TODO: make getters
  protected P2PSocketReceiver<Identifier> reader, writer;
  protected ErrorHandler<Identifier> errorHandler;
  
  public SocketWrapperSocket(Identifier identifier, P2PSocket<SubIdentifier> socket, Logger logger, ErrorHandler<Identifier> errorHandler, Map<String, Object> options) {
    this.identifier = identifier;
    this.socket = socket;
    this.logger = logger;
    this.options = options;
    this.errorHandler = errorHandler;
  }
  
  public Identifier getIdentifier() {
    return identifier;
  }
  public void close() {    
    if (logger.level <= Logger.FINER) {
      logger.logException("Closing "+this, new Exception("Stack Trace"));
    } else if (logger.level <= Logger.FINE) logger.log("Closing "+this);
    socket.close();
  }

  public long read(ByteBuffer dsts) throws IOException {
    long ret = socket.read(dsts);
    if (logger.level <= Logger.FINEST) logger.log(this+"read():"+ret);
    return ret;
  }
  
  public void register(boolean wantToRead, boolean wantToWrite,
      final P2PSocketReceiver<Identifier> receiver) {
//    logger.log(this+"register("+wantToRead+","+wantToWrite+","+receiver+")");
    if (logger.level <= Logger.FINEST) logger.log(this+"register("+wantToRead+","+wantToWrite+","+receiver+")");
    if (wantToRead) {
      if (reader != null && reader != receiver) throw new IllegalStateException("Already registered "+reader+" for reading. Can't register "+receiver);
      reader = receiver;
    }
    if (wantToWrite) {
      if (writer != null && writer != receiver) throw new IllegalStateException("Already registered "+reader+" for writing. Can't register "+receiver);
      writer = receiver;
    }
    socket.register(wantToRead, wantToWrite, this);
    
//    new P2PSocketReceiver<SubIdentifier>() {    
//      public void receiveSelectResult(P2PSocket<SubIdentifier> socket, boolean canRead,
//          boolean canWrite) throws IOException {
//        if (logger.level <= Logger.FINEST) logger.log(SocketWrapperSocket.this+"rsr("+socket+","+canRead+","+canWrite+")");
//        receiver.receiveSelectResult(SocketWrapperSocket.this, canRead, canWrite);
//      }
//    
//      public void receiveException(P2PSocket<SubIdentifier> socket, IOException e) {
//        receiver.receiveException(SocketWrapperSocket.this, e);
//      }    
//      
//      public String toString() {
//        return SocketWrapperSocket.this+"$1";
//      }
//    });
  }

  public void receiveSelectResult(P2PSocket<SubIdentifier> socket, boolean canRead,
      boolean canWrite) throws IOException {
//    logger.log(this+"rsr("+socket+","+canRead+","+canWrite+")");
    if (logger.level <= Logger.FINEST) logger.log(this+"rsr("+socket+","+canRead+","+canWrite+")");
    if (canRead && canWrite && (reader == writer)) {      
      P2PSocketReceiver<Identifier> temp = reader;
      reader = null;
      writer = null;
      temp.receiveSelectResult(this, canRead, canWrite);
      return;
    }

    if (canRead) {      
      P2PSocketReceiver<Identifier> temp = reader;
      if (temp== null) {
        if (logger.level <= Logger.WARNING) logger.log("no reader in "+this+".rsr("+socket+","+canRead+","+canWrite+")");         
      } else {
        reader = null;
        temp.receiveSelectResult(this, true, false);
      }
    }

    if (canWrite) {      
      P2PSocketReceiver<Identifier> temp = writer;
      if (temp == null) {
        if (logger.level <= Logger.WARNING) logger.log("no writer in "+this+".rsr("+socket+","+canRead+","+canWrite+")");        
      } else {
        writer = null;
        temp.receiveSelectResult(this, false, true);
      }
    }

    
//    receiver.receiveSelectResult(SocketWrapperSocket.this, canRead, canWrite);
  }

  public void receiveException(P2PSocket<SubIdentifier> socket, Exception e) {
//    logger.log(this+".receiveException("+e+")");
    if (writer != null) {
      if (writer == reader) {
        P2PSocketReceiver<Identifier> temp = writer;
        writer = null;
        reader = null;
        temp.receiveException(this, e);
      } else {
        P2PSocketReceiver<Identifier> temp = writer;
        writer = null;
        temp.receiveException(this, e);
      }
    }
    
    if (reader != null) {
      P2PSocketReceiver<Identifier> temp = reader;
      reader = null;
      temp.receiveException(this, e);
    }
    if (reader == null && writer == null && errorHandler != null) errorHandler.receivedException(getIdentifier(), e);
//    receiver.receiveException(SocketWrapperSocket.this, e);
  }    

  
  public void shutdownOutput() {
    socket.shutdownOutput();
  }

  public long write(ByteBuffer srcs) throws IOException {
    long ret = socket.write(srcs);
    if (logger.level <= Logger.FINEST) logger.log(this+"write():"+ret);
    return ret;
  }

  @Override
  public String toString() {
    if (getIdentifier() == socket.getIdentifier()) return socket.toString();
    return identifier+"-"+socket;
  }

  public Map<String, Object> getOptions() {
    return options;
  }
}
