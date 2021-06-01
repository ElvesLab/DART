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
package rice.pastry.transport;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.pastry.NodeHandle;
import rice.pastry.socket.TransportLayerNodeHandle;

public class SocketAdapter<Identifier> implements AppSocket, P2PSocketReceiver<Identifier> {
  P2PSocket<Identifier> internal;
  Logger logger;
  Environment environment;
  
  public SocketAdapter(P2PSocket<Identifier> socket, Environment env) {
    this.internal = socket;
    this.logger = env.getLogManager().getLogger(SocketAdapter.class, null);
    this.environment = env;
  }

  public void close() {
    internal.close();
  }
  
  public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
    long ret = 0;
    for (int i = offset; i < offset+length; i++) {
      ret += internal.read(dsts[i]);
    }
    return ret;
  }

  AppSocketReceiver reader = null;
  AppSocketReceiver writer = null;
  public void register(boolean wantToRead, boolean wantToWrite, int timeout, AppSocketReceiver receiver) {
    if (wantToRead) {
      if (reader != null && reader != receiver) throw new IllegalStateException("Already registered "+reader+" for reading. Can't register "+receiver);
      reader = receiver;
    }
    if (wantToWrite) {
      if (writer != null && writer != receiver) throw new IllegalStateException("Already registered "+reader+" for writing. Can't register "+receiver);
      writer = receiver;
    }
//    logger.log("register("+wantToRead+","+wantToWrite+","+receiver+")");
//    internal.register(wantToRead, wantToWrite, new AppSocketReceiverWrapper(receiver, this, environment));
    internal.register(wantToRead, wantToWrite, this);
  }

  public void receiveException(P2PSocket<Identifier> s, Exception e) {
    if (writer != null) {
      if (writer == reader) {
        AppSocketReceiver temp = writer;
        writer = null;
        reader = null;
        temp.receiveException(this, e);
      } else {
        AppSocketReceiver temp = writer;
        writer = null;
        temp.receiveException(this, e);
      }
    }
    
    if (reader != null) {
      AppSocketReceiver temp = reader;
      reader = null;
      temp.receiveException(this, e);
    }
  }

  public void receiveSelectResult(P2PSocket<Identifier> s,
      boolean canRead, boolean canWrite) throws IOException {
    if (logger.level <= Logger.FINEST) logger.log(this+"rsr("+internal+","+canRead+","+canWrite+")");
    if (canRead && canWrite && (reader == writer)) {      
      AppSocketReceiver temp = reader;
      reader = null;
      writer = null;
      temp.receiveSelectResult(this, canRead, canWrite);
      return;
    }

    if (canRead) {      
      AppSocketReceiver temp = reader;
      if (temp== null) {
        if (logger.level <= Logger.WARNING) logger.log("no reader in "+this+".rsr("+internal+","+canRead+","+canWrite+")");         
      } else {
        reader = null;
        temp.receiveSelectResult(this, true, false);
      }
    }

    if (canWrite) {      
      AppSocketReceiver temp = writer;
      if (temp == null) {
        if (logger.level <= Logger.WARNING) logger.log("no writer in "+this+".rsr("+internal+","+canRead+","+canWrite+")");        
      } else {
        writer = null;
        temp.receiveSelectResult(this, false, true);
      }
    }
  }

  public void shutdownOutput() {
    internal.shutdownOutput();    
  }

  public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
    long ret = 0;
    for (int i = offset; i < offset+length; i++) {
      ret += internal.write(srcs[i]);
    }
    return ret;
  }

  public long read(ByteBuffer dst) throws IOException {
    return internal.read(dst);
  }

  public long write(ByteBuffer src) throws IOException {
    return internal.write(src);
  }
  
  public String toString() {
    return "SA["+internal+"]";
  }
}
