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
package org.mpisws.p2p.transport.multiaddress;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.ClosedChannelException;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.DefaultCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.InsufficientBytesException;
import org.mpisws.p2p.transport.util.SocketInputBuffer;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;
import org.mpisws.p2p.transport.wire.WireTransportLayer;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * This class adds an epoch and a list of InetSocketAddresses, and also disambiguates between them
 * for the lower layer.  This is useful in situations where the node is behind a NAT and is addressed
 * differently for nodes outside the NAT as inside.
 * 
 * 
 * Optimization: Pre-serialize localAddress in the ctor, then just mem-copy it to the buffer
 * 
 * @author Jeff Hoye
 *
 */
public class MultiInetAddressTransportLayerImpl implements MultiInetAddressTransportLayer, TransportLayerCallback<InetSocketAddress, ByteBuffer> {

  /**
   * The max addresses in an EpochInetSocketAddress
   */
  int MAX_NUM_ADDRESSES;
  TransportLayer<InetSocketAddress, ByteBuffer> wire;
  MultiInetSocketAddress localAddress; 
  TransportLayerCallback<MultiInetSocketAddress, ByteBuffer> callback;
  ErrorHandler<MultiInetSocketAddress> errorHandler;
  Logger logger;
  AddressStrategy strategy;
  
  private boolean sendIdentifier = true;
  
  public MultiInetAddressTransportLayerImpl(
      MultiInetSocketAddress localAddress, 
      TransportLayer<InetSocketAddress, ByteBuffer> wire, 
      Environment env,
      ErrorHandler<MultiInetSocketAddress> handler,
      AddressStrategy strategy) {
    this.logger = env.getLogManager().getLogger(MultiInetAddressTransportLayerImpl.class, null);
    this.wire = wire;
    this.errorHandler = handler;    
    this.localAddress = localAddress;
    this.strategy = strategy;
    MAX_NUM_ADDRESSES = env.getParameters().getInt("transport_epoch_max_num_addresses");
    if (wire == null) throw new IllegalArgumentException("TransportLayer<InetSocketAddress, ByteBuffer> wire must be non-null");
    if (localAddress == null) throw new IllegalArgumentException("EpochInetSocketAddress localAddress must be non-null");
    
    this.callback = new DefaultCallback<MultiInetSocketAddress, ByteBuffer>(env);
    
    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<MultiInetSocketAddress>(logger); 
    }
    if (this.strategy == null) {
      this.strategy = new SimpleAddressStrategy(); 
    }
    
    wire.setCallback(this);
  }
  
  public AddressStrategy getAddressStrategy() {
    return strategy;
  }
  
  // ******************************** Sockets **************************
  public SocketRequestHandle<MultiInetSocketAddress> openSocket(
      final MultiInetSocketAddress i,
      final SocketCallback<MultiInetSocketAddress> deliverSocketToMe,
      Map<String, Object> options) {
    
    if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");

    final SocketRequestHandleImpl<MultiInetSocketAddress> handle = new SocketRequestHandleImpl<MultiInetSocketAddress>(i, options, logger);
    
    if (logger.level <= Logger.INFO-50) logger.log("openSocket("+i+","+deliverSocketToMe+","+options+")");
    SimpleOutputBuffer sob = new SimpleOutputBuffer(localAddress.getSerializedLength());
    try {
      localAddress.serialize(sob);
    } catch (IOException ioe) {
      deliverSocketToMe.receiveException(handle, ioe);
      return null;
    }
    final ByteBuffer b = sendIdentifier ? sob.getByteBuffer() : null;
    
    InetSocketAddress addr = strategy.getAddress(getLocalIdentifier(), i);
        
    handle.setSubCancellable(wire.openSocket(addr, 
        new SocketCallback<InetSocketAddress>(){    
      
      public void receiveResult(SocketRequestHandle<InetSocketAddress> c, final P2PSocket<InetSocketAddress> result) {
        if (handle.getSubCancellable() != null && c != handle.getSubCancellable()) throw new RuntimeException("c != cancellable.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+handle.getSubCancellable());
        
        handle.setSubCancellable(new Cancellable() {        
          public boolean cancel() {
            result.close();
            return true;
          }        
        });

        if (logger.level <= Logger.FINER) logger.log("openSocket("+i+"):receiveResult("+result+")");
        if (sendIdentifier) {
          result.register(false, true, new P2PSocketReceiver<InetSocketAddress>() {        
            public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
                boolean canRead, boolean canWrite) throws IOException {
              if (canRead || !canWrite) throw new IOException("Expected to write! "+canRead+","+canWrite);
              
              // do the work
              if (socket.write(b) < 0) {
                deliverSocketToMe.receiveException(handle, new ClosedChannelException("Remote node closed socket while opening.  Try again."));
                return;
              }
              
              // keep working or pass up the new socket
              if (b.hasRemaining()) {
                socket.register(false, true, this); 
              } else {
                deliverSocketToMe.receiveResult(handle, new SocketWrapperSocket<MultiInetSocketAddress, InetSocketAddress>(i, socket, logger, errorHandler, socket.getOptions())); 
              }
            }
          
            public void receiveException(P2PSocket<InetSocketAddress> socket,
                Exception e) {
              deliverSocketToMe.receiveException(handle, e);
            }        
          }); 
          
        } else {
          deliverSocketToMe.receiveResult(handle, new SocketWrapperSocket<MultiInetSocketAddress, InetSocketAddress>(i, result, logger, errorHandler, result.getOptions()));            
        }
      }    
      public void receiveException(SocketRequestHandle<InetSocketAddress> c, Exception exception) {
        if (handle.getSubCancellable() != null && c != handle.getSubCancellable()) throw new RuntimeException("c != cancellable.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+handle.getSubCancellable());
        deliverSocketToMe.receiveException(handle, exception);
      }
    }, options));
    return handle;
  }

  public void incomingSocket(P2PSocket<InetSocketAddress> s) throws IOException {
    if (logger.level <= Logger.FINE) logger.log("incomingSocket("+s+"):"+sendIdentifier);

    if (sendIdentifier) {
      final SocketInputBuffer sib = new SocketInputBuffer(s,1024);
      s.register(true, false, new P2PSocketReceiver<InetSocketAddress>() {
      
        public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
            boolean canRead, boolean canWrite) throws IOException {
          if (logger.level <= Logger.FINER) logger.log("incomingSocket("+socket+"):receiveSelectResult()");
          if (canWrite || !canRead) throw new IOException("Expected to read! "+canRead+","+canWrite);
          try {
            MultiInetSocketAddress eisa = MultiInetSocketAddress.build(sib);
            if (logger.level <= Logger.FINEST) logger.log("Read "+eisa);
            callback.incomingSocket(new SocketWrapperSocket<MultiInetSocketAddress, InetSocketAddress>(eisa, socket, logger, errorHandler, socket.getOptions()));
          } catch (InsufficientBytesException ibe) {
            socket.register(true, false, this); 
          } catch (IOException e) {
            if (logger.level <= Logger.INFO) errorHandler.receivedException(new MultiInetSocketAddress(socket.getIdentifier()), e);
          }
        }
      
        public void receiveException(P2PSocket<InetSocketAddress> socket,Exception e) {
          errorHandler.receivedException(new MultiInetSocketAddress(socket.getIdentifier()), e);
        }          
      });

    } else {
      // just pass up the socket
      callback.incomingSocket(
          new SocketWrapperSocket<MultiInetSocketAddress, InetSocketAddress>(
              new MultiInetSocketAddress(s.getIdentifier()),s, logger, errorHandler, s.getOptions())); 
    }
  }

  // ************************** Messages ***************************
  public MessageRequestHandle<MultiInetSocketAddress, ByteBuffer> sendMessage(
      final MultiInetSocketAddress i, 
      final ByteBuffer m,
      final MessageCallback<MultiInetSocketAddress, ByteBuffer> deliverAckToMe,
      Map<String, Object> options) {
    
    if (logger.level <= Logger.FINE) logger.log("sendMessage("+i+","+m+")");

    final MessageRequestHandleImpl<MultiInetSocketAddress, ByteBuffer> handle 
      = new MessageRequestHandleImpl<MultiInetSocketAddress, ByteBuffer>(i, m, options);
    final ByteBuffer buf;
    if (sendIdentifier) {
      SimpleOutputBuffer sob = new SimpleOutputBuffer(m.remaining() + localAddress.getSerializedLength());
      try {
        localAddress.serialize(sob);
        sob.write(m.array(), m.position(), m.remaining());
      } catch (IOException ioe) {
        if (deliverAckToMe == null) {
          errorHandler.receivedException(i, ioe);
        } else {
          deliverAckToMe.sendFailed(handle, ioe);
        }
        return handle;
      }
      buf = ByteBuffer.wrap(sob.getBytes());
    } else {
      buf = m; 
    }
    
    handle.setSubCancellable(wire.sendMessage(
        strategy.getAddress(getLocalIdentifier(), i), 
        buf, 
        new MessageCallback<InetSocketAddress, ByteBuffer>() {
    
          public void ack(MessageRequestHandle<InetSocketAddress, ByteBuffer> msg) {
            if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
            if (deliverAckToMe != null) deliverAckToMe.ack(handle);
          }
        
          public void sendFailed(MessageRequestHandle<InetSocketAddress, ByteBuffer> msg, Exception ex) {
            if (handle.getSubCancellable() != null && msg != handle.getSubCancellable()) throw new RuntimeException("msg != cancellable.getSubCancellable() (indicates a bug in the code) msg:"+msg+" sub:"+handle.getSubCancellable());
            if (deliverAckToMe == null) {
              if (ex instanceof NodeIsFaultyException) return;
              errorHandler.receivedException(i, ex);
            } else {
              deliverAckToMe.sendFailed(handle, ex);
            }
          }
        }, 
        options));
    return handle;
  }

  public String toString() {
    return "MultiInetAddrTL{"+localAddress+"}";
  }
  
  public void messageReceived(InetSocketAddress i, ByteBuffer m, Map<String, Object> options) throws IOException {
    if (logger.level <= Logger.FINE) logger.log("messageReceived("+i+","+m+")");
    // read numAddresses
    if (sendIdentifier) {      
      int pos = m.position();
      SimpleInputBuffer sib = new SimpleInputBuffer(m.array(),pos);
      
      MultiInetSocketAddress eisa;
      try {
        eisa = MultiInetSocketAddress.build(sib);
      } catch (IOException ioe) {
        errorHandler.receivedUnexpectedData(new MultiInetSocketAddress(i), m.array(), pos, null); 
        return;
      }

      // make sure to leave m at the proper position
      m.position(m.array().length - sib.bytesRemaining());
      
//      if (!m.hasRemaining()) {
//        errorHandler.receivedUnexpectedData(new EpochInetSocketAddress(i), m.array());
//      }
//      
//      byte numAddresses = m.get();
//      if (numAddresses < 1 || numAddresses > MAX_NUM_ADDRESSES) {
//        errorHandler.receivedUnexpectedData(new EpochInetSocketAddress(i), m.array()); 
//      }
//      
//      // make sure to leave m at the proper position
//      byte[] eisaBuffer = new byte[8+1+(numAddresses*6)];            
//      eisaBuffer[0] = numAddresses;
//      m.get(eisaBuffer, 1, eisaBuffer.length-1);
//      
//      SimpleInputBuffer sib = new SimpleInputBuffer(eisaBuffer);
//      EpochInetSocketAddress eisa = EpochInetSocketAddress.build(sib);
      callback.messageReceived(eisa, m, options);
    } else {
      callback.messageReceived(new MultiInetSocketAddress(i), m, options); 
    }
  }

  // ***************************** Other ******************************
  public MultiInetSocketAddress getLocalIdentifier() {
    return localAddress;
  }

  public void acceptMessages(boolean b) {
    wire.acceptMessages(b);
  }

  public void acceptSockets(boolean b) {
    wire.acceptSockets(b);
  }

  public void destroy() {
    wire.destroy();
  }

  public void setCallback(TransportLayerCallback<MultiInetSocketAddress, ByteBuffer> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<MultiInetSocketAddress> handler) {
    this.errorHandler = handler;
  }

  /**
   * Set this to false to prevent sending/receiving the identifier at this layer for the 
   * message/socket.  Note that identity will return a guess based 
   * on the ipaddress of the message/socket, and the epoch will be UNKNOWN
   * Only use this if a higher layer will do the work of identification.
   * 
   * @param sendIdentifier
   */
  public void setSendIdentifier(boolean sendIdentifier) {
    this.sendIdentifier = sendIdentifier;
  }

  public boolean isSendIdentifier() {
    return sendIdentifier;
  }
}
