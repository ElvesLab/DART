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
package org.mpisws.p2p.transport.simpleidentity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.InsufficientBytesException;
import org.mpisws.p2p.transport.util.SocketInputBuffer;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * The purpose of this layer is to identify the opener of a TCP connection, because the 
 * socket is using an ephemeral port.
 * 
 * @author Jeff Hoye
 *
 * @param <Identifier>
 * @param <MessageType>
 */
public class SimpleIdentityTransportLayer<Identifier, MessageType> implements 
    TransportLayer<Identifier, MessageType>, TransportLayerCallback<Identifier, MessageType>{

  protected TransportLayer<Identifier, MessageType> tl;
  protected Logger logger;
  private TransportLayerCallback<Identifier, MessageType> callback;
  protected ErrorHandler<Identifier> errorHandler;
  protected Serializer<Identifier> serializer;  
  LocalIdentifierStrategy<Identifier> localIdStrategy; 
  
  /**
   * Sends the same identifier every time.
   * @author Jeff Hoye
   *
   * @param <Identifier>
   */
  class DefaultLocalIdentifierStrategy<Identifier> implements LocalIdentifierStrategy<Identifier> {
    byte[] localIdentifierBytes;
    public DefaultLocalIdentifierStrategy(Identifier i) throws IOException {
      SimpleOutputBuffer sob = new SimpleOutputBuffer();
      serializer.serialize(tl.getLocalIdentifier(), sob);
      localIdentifierBytes = sob.getBytes();      
    }
    public byte[] getLocalIdentifierBytes() {
      return localIdentifierBytes;
    }
  }
  
  public SimpleIdentityTransportLayer(TransportLayer<Identifier, MessageType> tl, Serializer<Identifier> serializer, LocalIdentifierStrategy<Identifier> localIdStrategy, Environment env, ErrorHandler<Identifier> handler) throws IOException {
    this.tl = tl;
    this.tl.setCallback(this);
    this.errorHandler = handler;
    this.logger = env.getLogManager().getLogger(getClass(), null);
    this.serializer = serializer;
    this.localIdStrategy = localIdStrategy;
    if (this.localIdStrategy == null) {
      this.localIdStrategy = new DefaultLocalIdentifierStrategy<Identifier>(tl.getLocalIdentifier());
    }
  }
  
  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  public SocketRequestHandle<Identifier> openSocket(Identifier i,
      final SocketCallback<Identifier> deliverSocketToMe, Map<String, Object> options) {
    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(i,options,logger);
    ret.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>() {
      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> sock) {
        // write the local identifier
        try {
          final ByteBuffer writeMe = ByteBuffer.wrap(localIdStrategy.getLocalIdentifierBytes());
          new P2PSocketReceiver<Identifier>() {
            public void receiveSelectResult(P2PSocket<Identifier> socket,
                boolean canRead, boolean canWrite) throws IOException {
              if (socket.write(writeMe) < 0) {
                deliverSocketToMe.receiveException(ret, new ClosedChannelException("Socket closed."));
                return;
              }
              if (writeMe.hasRemaining()) {
                socket.register(false, true, this);
                return;
              }
              // done
              deliverSocketToMe.receiveResult(ret, new SocketWrapperSocket<Identifier, Identifier>(socket.getIdentifier(),socket,logger,errorHandler,socket.getOptions()));
            }
            public void receiveException(P2PSocket<Identifier> socket,
                Exception ioe) {
              deliverSocketToMe.receiveException(ret, ioe);
            }
          }.receiveSelectResult(sock, false, true);
        } catch (IOException ioe) {
          deliverSocketToMe.receiveException(ret, ioe);
        }
      }
      public void receiveException(SocketRequestHandle<Identifier> s,
          Exception ex) {
        deliverSocketToMe.receiveException(ret, ex);
      }      
    }, options));
    return ret;
  }

  public MessageRequestHandle<Identifier, MessageType> sendMessage(
      Identifier i, MessageType m,
      MessageCallback<Identifier, MessageType> deliverAckToMe,
      Map<String, Object> options) {
    return tl.sendMessage(i, m, deliverAckToMe, options);
  }

  public void setCallback(
      TransportLayerCallback<Identifier, MessageType> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  public void destroy() {
    // TODO Auto-generated method stub
    
  }

  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
    final SocketInputBuffer sib = new SocketInputBuffer(s);
    
    new P2PSocketReceiver<Identifier>() {
      public void receiveSelectResult(P2PSocket<Identifier> socket,
          boolean canRead, boolean canWrite) throws IOException {
        try {
          Identifier remoteIdentifier = serializer.deserialize(sib, socket.getIdentifier(), socket.getOptions());
          callback.incomingSocket(new SocketWrapperSocket<Identifier, Identifier>(remoteIdentifier,socket,logger,errorHandler,socket.getOptions()));
        } catch (InsufficientBytesException ibe) {
          socket.register(true, false, this);          
        } // throw the rest        
      }

      public void receiveException(P2PSocket<Identifier> socket, Exception ioe) {
        errorHandler.receivedException(socket.getIdentifier(), ioe);
      }

    }.receiveSelectResult(s, true, false);
  }

  public void messageReceived(Identifier i, MessageType m,
      Map<String, Object> options) throws IOException {
    callback.messageReceived(i, m, options);
  }

}
