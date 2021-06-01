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
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;

import rice.p2p.commonapi.rawserialization.RawSerializable;

public class ReplaySocket<Identifier extends RawSerializable> implements P2PSocket<Identifier>, SocketRequestHandle<Identifier> {

  protected Identifier identifier;
  protected int socketId;
  protected VerifierImpl<Identifier, ?> verifier;
  boolean closed = false;
  boolean outputShutdown = false;
  Map<String, Object> options;
  
  /**
   * TODO: Make extensible by putting into a factory.
   * 
   * @param identifier
   * @param socketId
   * @param verifier
   */
  public ReplaySocket(Identifier identifier, int socketId, VerifierImpl<Identifier, ?> verifier, Map<String, Object> options) {
    this.identifier = identifier;
    this.socketId = socketId;
    this.verifier = verifier;
    this.options = options;
  }

  public Identifier getIdentifier() {
    return identifier;
  }

  public Map<String, Object> getOptions() {
    return options;
  }

  public long read(ByteBuffer dst) throws IOException {
//    if (closed) throw new ClosedChannelException("Socket already closed.");
    return verifier.readSocket(socketId, dst);
  }

  public long write(ByteBuffer src) throws IOException {
//    if (closed || outputClosed) throw new ClosedChannelException("Socket already closed.");
    return verifier.writeSocket(socketId, src);
  }

  P2PSocketReceiver<Identifier> reader;
  P2PSocketReceiver<Identifier> writer;
  public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<Identifier> receiver) {
    if (closed) {
      receiver.receiveException(this, new ClosedChannelException("Socket "+this+" already closed."));
      return;
    }
    if (wantToWrite && outputShutdown) {
      receiver.receiveException(this, new ClosedChannelException("Socket "+this+" already shutdown output."));
      return;
    }

    if (wantToWrite) {
      if (writer != null) {
        if (writer != receiver) throw new IllegalStateException("Already registered "+writer+" for writing, you can't register "+receiver+" for writing as well!"); 
      }
    }
    
    if (wantToRead) {
      if (reader != null) {
        if (reader != receiver) throw new IllegalStateException("Already registered "+reader+" for reading, you can't register "+receiver+" for reading as well!"); 
      }
      reader = receiver; 
    }
    
    if (wantToWrite) {
      writer = receiver; 
    }
  }
  
  public void notifyIO(boolean canRead, boolean canWrite) throws IOException {
    if (!canRead && !canWrite) {
      throw new IOException("I can't read or write. canRead:"+canRead+" canWrite:"+canWrite);
    }
    if (canRead && canWrite) {
      if (writer != reader) throw new IllegalStateException("weader != writer canRead:"+canRead+" canWrite:"+canWrite);
      P2PSocketReceiver<Identifier> temp = writer;
      writer = null;
      reader = null;
      temp.receiveSelectResult(this, canRead, canWrite);
      return;
    } 
    
    if (canRead) {
      if (reader == null) throw new IllegalStateException("reader:"+reader+" canRead:"+canRead);
      P2PSocketReceiver<Identifier> temp = reader;
      reader = null;
      temp.receiveSelectResult(this, canRead, canWrite);
      return;
    } 
    
    if (canWrite) {
      if (writer == null) throw new IllegalStateException("writer:"+writer+" canWrite:"+canWrite);
      P2PSocketReceiver<Identifier> temp = writer;
      writer = null;
      temp.receiveSelectResult(this, canRead, canWrite);
      return;
    }     
  }

  public void close() {
    closed = true;
    
    verifier.close(socketId);
  }

  SocketCallback<Identifier> deliverSocketToMe;
  public void setDeliverSocketToMe(SocketCallback<Identifier> deliverSocketToMe) {
    this.deliverSocketToMe = deliverSocketToMe;
  }
  
  public void socketOpened() {
    deliverSocketToMe.receiveResult(this, this);
    deliverSocketToMe = null;
  }
  
  public void shutdownOutput() {
    outputShutdown = true;
    
    verifier.shutdownOutput(socketId);
//    throw new RuntimeException("Not implemented.");
  }

  public void receiveException(IOException ioe) {
    if (deliverSocketToMe != null) {
      deliverSocketToMe.receiveException(this, ioe);
      return;
    }
    if (writer != null) {
      if (writer == reader) {
        writer.receiveException(this, ioe);
        writer = null;
        reader = null;
      } else {
        writer.receiveException(this, ioe);
        writer = null;
      }
    }
    
    if (reader != null) {
      reader.receiveException(this, ioe);
      reader = null;
    }
  }
  
  public boolean cancel() {
    throw new RuntimeException("Not implemented.");
  }

}
