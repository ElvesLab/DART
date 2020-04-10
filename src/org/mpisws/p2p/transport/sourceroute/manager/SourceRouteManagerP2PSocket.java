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
package org.mpisws.p2p.transport.sourceroute.manager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;

import rice.environment.Environment;
import rice.environment.logging.Logger;

public class SourceRouteManagerP2PSocket<Identifier> extends SocketWrapperSocket<Identifier, SourceRoute<Identifier>> {

//implements P2PSocket<Identifier> {

//  P2PSocket<SourceRoute<Identifier>> socket;
//  Logger logger;
  
  public SourceRouteManagerP2PSocket(P2PSocket<SourceRoute<Identifier>> socket, Logger logger, ErrorHandler<Identifier> errorHandler, Environment env) {
    super(socket.getIdentifier().getLastHop(),socket,logger, errorHandler ,socket.getOptions());
//    this.socket = socket; 
//    this.logger = env.getLogManager().getLogger(SourceRouteManagerP2PSocket.class,null);
//    logger.log("ctor("+socket.getOptions()+")");
  }
  
//  public void close() {
//    socket.close();
//  }

//  public Identifier getIdentifier() {
//    return socket.getIdentifier().getLastHop();
//  }

//  public long read(ByteBuffer dsts) throws IOException {
//    return socket.read(dsts);
//  }

//  private P2PSocketReceiver<Identifier> registeredToRead = null;
//  private P2PSocketReceiver<Identifier> registeredToWrite = null;
//  private boolean exception = false;
//  
//  public void register(boolean wantToRead, boolean wantToWrite,
//      final P2PSocketReceiver<Identifier> receiver) {
//    if (wantToRead) {
//      registeredToRead = receiver;
//      //logger.logException(SourceRouteManagerP2PSocket.this+".register("+registeredToRead+")1",new Exception("Stack Trace"));
//    }
//    if (wantToWrite) registeredToWrite = receiver;
//    if (logger.level <= Logger.FINEST) logger.log(this+"register("+wantToRead+","+wantToWrite+","+receiver+")");
//    socket.register(wantToRead, wantToWrite, new P2PSocketReceiver<SourceRoute<Identifier>>(){    
//      public void receiveSelectResult(P2PSocket<SourceRoute<Identifier>> socket, boolean canRead, boolean canWrite) throws IOException {
//        if (socket != SourceRouteManagerP2PSocket.this.socket) throw new IllegalStateException("socket != this.socket"+socket+","+SourceRouteManagerP2PSocket.this.socket); // it is a bug if this gets tripped
//        if (canRead) {
//          //logger.logException(SourceRouteManagerP2PSocket.this+".register("+registeredToRead+")2",new Exception("Stack Trace"));
//          registeredToRead = null;
//        }
//        if (canWrite) registeredToWrite = null;
//        receiver.receiveSelectResult(SourceRouteManagerP2PSocket.this, canRead, canWrite);
//      }
//      public void receiveException(P2PSocket<SourceRoute<Identifier>> socket, IOException e) {
//        if (socket != SourceRouteManagerP2PSocket.this.socket) throw new IllegalStateException("socket != this.socket"+socket+","+SourceRouteManagerP2PSocket.this.socket); // it is a bug if this gets tripped
//        exception = true;
//        receiver.receiveException(SourceRouteManagerP2PSocket.this, e);
//      }    
//    });
//  }
//
//  public void shutdownOutput() {
//    socket.shutdownOutput();
//  }

//  public long write(ByteBuffer srcs) throws IOException {
//    return socket.write(srcs);
//  }

//  public Map<String, Object> getOptions() {
//    return socket.getOptions();
//  }

//  @Override
//  public String toString() {
//    return "SRMSocket("+socket.getIdentifier()+":"+getOptions()+")@"+System.identityHashCode(this)+" r:"+registeredToRead+" w:"+registeredToWrite+" e:"+exception;
//  }
}
