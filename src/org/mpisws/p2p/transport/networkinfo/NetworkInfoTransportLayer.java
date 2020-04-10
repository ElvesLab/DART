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
package org.mpisws.p2p.transport.networkinfo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.simpleidentity.InetSocketAddressSerializer;
import org.mpisws.p2p.transport.util.BufferReader;
import org.mpisws.p2p.transport.util.BufferWriter;
import org.mpisws.p2p.transport.util.DefaultCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.InsufficientBytesException;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketInputBuffer;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.wire.magicnumber.MagicNumberTransportLayer;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.util.AttachableCancellable;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * Can open a TCP connection to a known node, and it will return your IP address.  
 * 
 * Use getMyIpAddress()
 * 
 * header = 0; // bypass
 * header = 1; // return IP
 * 
 * Also holds a "serialized id."  This is an externally specified piece of information.  It can be a NodeHandle, a PublicKey etc, 
 * any information that any node should be able to query for and may need to do so w/o having a joined PastryNode.
 * 
 * 
 * @author Jeff Hoye
 *
 */
public class NetworkInfoTransportLayer implements 
    InetSocketAddressLookup, 
    Prober,
    TransportLayer<InetSocketAddress, ByteBuffer>, 
    TransportLayerCallback<InetSocketAddress, ByteBuffer> {
  protected Logger logger;
  protected Environment environment;
  
  protected TransportLayerCallback<InetSocketAddress, ByteBuffer> callback;
  protected ErrorHandler<InetSocketAddress> errorHandler;
  protected TransportLayer<InetSocketAddress, ByteBuffer> tl;

  protected static final byte HEADER_PASSTHROUGH_BYTE = (byte)0;
  protected static final byte HEADER_IP_ADDRESS_REQUEST_BYTE = (byte)1;
  protected static final byte HEADER_PROBE_REQUEST_BYTE = (byte)2;
  protected static final byte HEADER_PROBE_RESPONSE_BYTE = (byte)3;
  protected static final byte HEADER_NODES_REQUEST_BYTE = (byte)4;
  protected static final byte HEADER_ID_REQUEST_BYTE = (byte)5;
  protected static final byte[] HEADER_PASSTHROUGH = {HEADER_PASSTHROUGH_BYTE};
  protected static final byte[] HEADER_IP_ADDRESS_REQUEST = {HEADER_IP_ADDRESS_REQUEST_BYTE};
  protected static final byte[] HEADER_NODES_REQUEST = {HEADER_NODES_REQUEST_BYTE};

  Map<Byte, byte[]> serializedIds = new HashMap<Byte, byte[]>();
  
  public NetworkInfoTransportLayer(TransportLayer<InetSocketAddress, ByteBuffer> tl, 
      Environment env, 
      ErrorHandler<InetSocketAddress> errorHandler) {
    this.logger = env.getLogManager().getLogger(NetworkInfoTransportLayer.class, null);
    this.environment = env;
    this.tl= tl;
    
    this.errorHandler = errorHandler;
    
    if (this.errorHandler == null) {
      this.errorHandler = new DefaultErrorHandler<InetSocketAddress>(logger); 
    }
    
    tl.setCallback(this);
  }
  
  InetSocketAddressSerializer addrSerializer = new InetSocketAddressSerializer();
  
  public Cancellable getMyInetAddress(InetSocketAddress bootstrap, 
      final Continuation<InetSocketAddress, IOException> c, Map<String, Object> options) {
    AttachableCancellable ret = new AttachableCancellable();
    ret.attach(openSocket(bootstrap, HEADER_IP_ADDRESS_REQUEST, new SocketCallback<InetSocketAddress>() {
    
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable,
          P2PSocket<InetSocketAddress> sock) {
        final SocketInputBuffer sib = new SocketInputBuffer(sock);
        
        try {
          new P2PSocketReceiver<InetSocketAddress>() {
            
            public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
                boolean canRead, boolean canWrite) throws IOException {
              // read IP address
              try {
                InetSocketAddress addr = addrSerializer.deserialize(sib, null, null);
                c.receiveResult(addr);
              } catch (InsufficientBytesException ibe) {
                socket.register(true, false, this);
              } catch (IOException e) {
                c.receiveException(e);
              }
            }
          
            public void receiveException(P2PSocket<InetSocketAddress> socket,
                Exception ioe) {
              if (ioe instanceof IOException) c.receiveException((IOException)ioe);
              c.receiveException(new NetworkInfoIOException(ioe));
            }
          
          }.receiveSelectResult(sock, true, false);        
        } catch (IOException ioe) {
          c.receiveException(ioe);
        }
      }
    
      public void receiveException(SocketRequestHandle<InetSocketAddress> s,
          Exception ex) {
        if (ex instanceof IOException) c.receiveException((IOException)ex);
        c.receiveException(new NetworkInfoIOException(ex));
      }    
    }, options));
    return ret;
  }

  public void setId(byte index, byte[] value) {
    serializedIds.put(index, value);
  }
  
  public Cancellable getId(InetSocketAddress bootstrap, byte index,
      final Continuation<byte[], IOException> c, Map<String, Object> options) {
    byte[] hdr = new byte[2];
    hdr[0] = HEADER_ID_REQUEST_BYTE;
    hdr[1] = index;
    
    AttachableCancellable ret = new AttachableCancellable();
    ret.attach(openSocket(bootstrap, hdr, new SocketCallback<InetSocketAddress>() {
    
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable,
          P2PSocket<InetSocketAddress> sock) {
        final SocketInputBuffer sib = new SocketInputBuffer(sock);
        
        try {
          new P2PSocketReceiver<InetSocketAddress>() {
            
            public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
                boolean canRead, boolean canWrite) throws IOException {
              try {
                // read size
                int size = sib.readInt();
                byte[] ret = new byte[size];
                sib.read(ret);
                c.receiveResult(ret);
              } catch (InsufficientBytesException ibe) {
                socket.register(true, false, this);
              } catch (IOException e) {
                c.receiveException(e);
              }
            }
          
            public void receiveException(P2PSocket<InetSocketAddress> socket,
                Exception ioe) {
              if (ioe instanceof IOException) c.receiveException((IOException)ioe);
              c.receiveException(new NetworkInfoIOException(ioe));
            }
          
          }.receiveSelectResult(sock, true, false);        
        } catch (IOException ioe) {
          c.receiveException(ioe);
        }
      }
    
      public void receiveException(SocketRequestHandle<InetSocketAddress> s,
          Exception ex) {
        if (ex instanceof IOException) c.receiveException((IOException)ex);
        c.receiveException(new NetworkInfoIOException(ex));
      }    
    }, options));
    return ret;
  }
  
  public Cancellable getExternalNodes(InetSocketAddress bootstrap, 
      final Continuation<Collection<InetSocketAddress>, IOException> c, Map<String, Object> options) {
    AttachableCancellable ret = new AttachableCancellable();
    ret.attach(openSocket(bootstrap, HEADER_NODES_REQUEST, new SocketCallback<InetSocketAddress>() {
    
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable,
          P2PSocket<InetSocketAddress> sock) {
        final SocketInputBuffer sib = new SocketInputBuffer(sock);
        
        try {
          new P2PSocketReceiver<InetSocketAddress>() {
            
            public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
                boolean canRead, boolean canWrite) throws IOException {
              // read IP address
              try {
                ArrayList<InetSocketAddress> ret = new ArrayList<InetSocketAddress>();
                byte numAddrs = sib.readByte();
                for (int ctr = 0; ctr < numAddrs; ctr++) {
                  InetSocketAddress addr = addrSerializer.deserialize(sib, null, null);
                  ret.add(addr);
                }
                c.receiveResult(ret);
              } catch (InsufficientBytesException ibe) {
                socket.register(true, false, this);
              } catch (IOException e) {
                c.receiveException(e);
              }
            }
          
            public void receiveException(P2PSocket<InetSocketAddress> socket,
                Exception ioe) {
              if (ioe instanceof IOException) c.receiveException((IOException)ioe);
              c.receiveException(new NetworkInfoIOException(ioe));
            }
          
          }.receiveSelectResult(sock, true, false);        
        } catch (IOException ioe) {
          c.receiveException(ioe);
        }
      }
    
      public void receiveException(SocketRequestHandle<InetSocketAddress> s,
          Exception ex) {
        if (ex instanceof IOException) c.receiveException((IOException)ex);
        c.receiveException(new NetworkInfoIOException(ex));
      }    
    }, options));
    return ret;
  }
  
  public SocketRequestHandle<InetSocketAddress> openSocket(InetSocketAddress i,
      SocketCallback<InetSocketAddress> deliverSocketToMe,
      Map<String, Object> options) {
    if (logger.level <= Logger.INFO-50) logger.log("openSocket("+i+","+deliverSocketToMe+","+options+")");

    return openSocket(i,HEADER_PASSTHROUGH,deliverSocketToMe,options);
  }
  
  public SocketRequestHandle<InetSocketAddress> openSocket(final InetSocketAddress i, final byte[] header,
      final SocketCallback<InetSocketAddress> deliverSocketToMe,
      Map<String, Object> options) {
    if (logger.level <= Logger.FINEST) logger.log("openSocket("+i+","+header.length+")");

    if (deliverSocketToMe == null) throw new IllegalArgumentException("deliverSocketToMe must be non-null!");

    final SocketRequestHandleImpl<InetSocketAddress> cancellable = new SocketRequestHandleImpl<InetSocketAddress>(i, options, logger);

    cancellable.setSubCancellable(tl.openSocket(i, new SocketCallback<InetSocketAddress>(){
      public void receiveResult(SocketRequestHandle<InetSocketAddress> c, final P2PSocket<InetSocketAddress> result) {
        if (cancellable.getSubCancellable() != null && c != cancellable.getSubCancellable()) throw new RuntimeException("c != cancellable.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+cancellable.getSubCancellable());
        
        cancellable.setSubCancellable(new Cancellable() {        
          public boolean cancel() {
            result.close();
            return true;
          }        
        });
        
        result.register(false, true, new P2PSocketReceiver<InetSocketAddress>(){        
          ByteBuffer buf = ByteBuffer.wrap(header);
          public void receiveSelectResult(P2PSocket<InetSocketAddress> socket, boolean canRead, boolean canWrite) throws IOException {
            if (canRead) throw new IOException("Never asked to read!");
            if (!canWrite) throw new IOException("Can't write!");
            long ret = socket.write(buf);            
            if (ret < 0) {
              socket.close();
              return;
            }
//            notifyListenersWrite((int)ret, socket.getIdentifier(), socket.getOptions(), false, true);
            if (logger.level <= Logger.FINEST) logger.log("openSocket("+i+","+header.length+") wrote "+ret+".  Remaining:"+buf.remaining());

            if (buf.hasRemaining()) {
              socket.register(false, true, this);
            } else {
              deliverSocketToMe.receiveResult(cancellable, socket);
            }
          }        
          public void receiveException(P2PSocket<InetSocketAddress> socket, Exception e) {
            deliverSocketToMe.receiveException(cancellable, e);
          }
        });
      }    
      public void receiveException(SocketRequestHandle<InetSocketAddress> c, Exception exception) {
        if (cancellable.getSubCancellable() != null && c != cancellable.getSubCancellable()) throw new RuntimeException("c != cancellable.getSubCancellable() (indicates a bug in the code) c:"+c+" sub:"+cancellable.getSubCancellable());
        deliverSocketToMe.receiveException(cancellable, exception);
//        errorHandler.receivedException(i, exception);
      }    
    }, options));
    
    return cancellable;
  }

  public void incomingSocket(final P2PSocket<InetSocketAddress> s) throws IOException {
    if (logger.level <= Logger.FINEST) logger.log("incomingSocket("+s+")");
    new P2PSocketReceiver<InetSocketAddress>() {
      ByteBuffer bb = ByteBuffer.allocate(HEADER_PASSTHROUGH.length); 
      public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
          boolean canRead, boolean canWrite) throws IOException {
        long bytesRead = socket.read(bb);
        if (logger.level <= Logger.FINEST) logger.log("incomingSocket("+s+"): bytesRead = "+bytesRead+" remaining:"+bb.remaining());
        if (bytesRead < 0) {
          socket.close();
          return;
        }
        if(bb.hasRemaining()) {
          socket.register(true, false, this);    
          return;
        }
        
        // read the array
        byte[] ret = bb.array();
        if (ret.length > 1) throw new RuntimeException("Make this work over the array, implementation expectes header to be 1 byte.");
        if (logger.level <= Logger.FINEST) logger.log("incomingSocket("+s+"): type = "+ret[0]);
        switch (ret[0]) {
        case HEADER_PASSTHROUGH_BYTE:
          callback.incomingSocket(socket);
          return;
        case HEADER_IP_ADDRESS_REQUEST_BYTE:
          handleIpRequest(socket);
          return;
        case HEADER_NODES_REQUEST_BYTE:
          handleNodesRequest(socket);
          return;
        case HEADER_PROBE_REQUEST_BYTE:
          handleProbeRequest(socket);
          return;
        case HEADER_PROBE_RESPONSE_BYTE:
          handleProbeResponse(socket);
          return;
        case HEADER_ID_REQUEST_BYTE:
          handleIdRequest(socket);
          return;
        default:
          // header didn't match up
          errorHandler.receivedUnexpectedData(socket.getIdentifier(), ret, 0, socket.getOptions());
        }
        
      }
    
      public void receiveException(P2PSocket<InetSocketAddress> socket,
          Exception ioe) {
        errorHandler.receivedException(socket.getIdentifier(), ioe);
      }    
    }.receiveSelectResult(s, true, false);
  }

  public void handleIpRequest(final P2PSocket<InetSocketAddress> socket) throws IOException {
    // write out the caller's ip address
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    if (logger.level <= Logger.INFO) logger.log("HEADER_IP_ADDRESS_REQUEST_BYTE serializing "+socket.getIdentifier());
    addrSerializer.serialize(socket.getIdentifier(), sob);          
    final ByteBuffer writeMe = sob.getByteBuffer();
    new P2PSocketReceiver<InetSocketAddress>() {           
      public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
          boolean canRead, boolean canWrite) throws IOException {
        if (socket.write(writeMe) < 0) {
          socket.close();
        }
        if (writeMe.hasRemaining()) {
          socket.register(false, true, this);
        } else {
          socket.close();
        }
      }
    
      public void receiveException(P2PSocket<InetSocketAddress> socket,
          Exception ioe) {
        // do nothing
      }
    
    }.receiveSelectResult(socket, false, true);

  }
  
  public void handleNodesRequest(final P2PSocket<InetSocketAddress> socket) throws IOException {
    // write out the caller's ip address
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    Collection<InetSocketAddress> ret = probeStrategy.getExternalAddresses();
    if (logger.level <= Logger.INFO) logger.log("serializing "+ret.size()+" external addresses for "+socket.getIdentifier());
    
    // only take the first 20
    if (ret.size() > 20) {
      ArrayList<InetSocketAddress> temp = new ArrayList<InetSocketAddress>(20);
      int ctr = 0;
      for (InetSocketAddress foo : ret) {
        temp.add(foo);
        ctr++;
        if (ctr > 20) break;
      }
      ret = temp;
    }
    
    sob.writeByte(ret.size());
    for (InetSocketAddress foo : ret) {
      addrSerializer.serialize(foo, sob);          
    }
    final ByteBuffer writeMe = sob.getByteBuffer();
    new P2PSocketReceiver<InetSocketAddress>() {           
      public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
          boolean canRead, boolean canWrite) throws IOException {
        if (socket.write(writeMe) < 0) {
          socket.close();
        }
        if (writeMe.hasRemaining()) {
          socket.register(false, true, this);
        } else {
          socket.close();
        }
      }
    
      public void receiveException(P2PSocket<InetSocketAddress> socket,
          Exception ioe) {
        // do nothing
      }
    
    }.receiveSelectResult(socket, false, true);
  }
  
  public void handleIdRequest(final P2PSocket<InetSocketAddress> socket) throws IOException {
    // read the index
    final ByteBuffer indexBuf = ByteBuffer.allocate(1);
    new BufferReader<InetSocketAddress>(socket,new Continuation<ByteBuffer, Exception>() {
    
      public void receiveResult(ByteBuffer result) {
        byte index = result.get();
        if (serializedIds.get(index) == null) {
          // consider returning an error
          socket.close();
          return;          
        }
        new BufferWriter<InetSocketAddress>(ByteBuffer.wrap(serializedIds.get(index)),socket,null);
      }
    
      public void receiveException(Exception exception) {
        socket.close();
      }    
    },1);
  }
  
  public void handleProbeRequest(final P2PSocket<InetSocketAddress> socket) {
    // read addr, uid
    try {
      new P2PSocketReceiver<InetSocketAddress>() {
        SocketInputBuffer sib = new SocketInputBuffer(socket);
      
        public void receiveSelectResult(final P2PSocket<InetSocketAddress> socket,
            boolean canRead, boolean canWrite) throws IOException {
          // try to read the stuff until it works or fails
          try {
            MultiInetSocketAddress addr = MultiInetSocketAddress.build(sib);
            long uid = sib.readLong();
            probeStrategy.requestProbe(addr, uid, new Continuation<Boolean, Exception>() {            
              public void receiveResult(Boolean result) {
                returnResult(result.booleanValue());
              }
            
              public void receiveException(Exception exception) {
                returnResult(false);
              }
              
              public void returnResult(boolean ret) {
                final ByteBuffer writeMe = ByteBuffer.allocate(1);
                writeMe.put(ret ? (byte)1 : (byte)0);
                writeMe.flip();
                
                try {
                  new P2PSocketReceiver<InetSocketAddress>() {                
                    public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
                        boolean canRead, boolean canWrite) throws IOException {
                      long bytesWritten = socket.write(writeMe);
                      if (bytesWritten < 0) {
                        socket.close();
                        return;
                      }
                      if (writeMe.hasRemaining()) {
                        socket.register(false, true, this);
                      } else {
                        socket.close();
                      }
                    }
                  
                    public void receiveException(P2PSocket<InetSocketAddress> socket,
                        Exception ioe) {
                      socket.close();
                    }                
                  }.receiveSelectResult(socket, false, true);
                } catch (IOException ioe) {
                  socket.close();
                }
              }
            });
          } catch (InsufficientBytesException ibe) {    
            socket.register(true, false, this);
          }
        }
      
        public void receiveException(P2PSocket<InetSocketAddress> socket,
            Exception ioe) {
          socket.close();
        }
      
      }.receiveSelectResult(socket, true, false);
    } catch (IOException ioe) {
      errorHandler.receivedException(socket.getIdentifier(), ioe);
      socket.close();
    }
  }
  
  public void handleProbeResponse(final P2PSocket<InetSocketAddress> socket) {
    // read addr, uid
    try {
      new P2PSocketReceiver<InetSocketAddress>() {
        SocketInputBuffer sib = new SocketInputBuffer(socket);
      
        public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
            boolean canRead, boolean canWrite) throws IOException {
          // try to read the stuff until it works or fails
          try {
            long uid = sib.readLong();
            verifyConnectionRequests.get(uid).tcpSuccess(socket.getIdentifier(), socket.getOptions());
          } catch (InsufficientBytesException ibe) {    
            socket.register(true, false, this);
          }
        }
      
        public void receiveException(P2PSocket<InetSocketAddress> socket,
            Exception ioe) {
          // TODO Auto-generated method stub
      
        }
      
      }.receiveSelectResult(socket, true, false);
    } catch (IOException ioe) {
      errorHandler.receivedException(socket.getIdentifier(), ioe);
      socket.close();
    }
  }
  
  public void setCallback(TransportLayerCallback<InetSocketAddress, ByteBuffer> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<InetSocketAddress> handler) {
    if (handler == null) {
      this.errorHandler = new DefaultErrorHandler<InetSocketAddress>(logger);
      return;
    }
    this.errorHandler = handler;
  }

  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  public InetSocketAddress getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  /**
   * Set the PASSTHROUGH header
   */
  public MessageRequestHandle<InetSocketAddress, ByteBuffer> sendMessage(
      InetSocketAddress i, ByteBuffer m,
      final MessageCallback<InetSocketAddress, ByteBuffer> deliverAckToMe,
      Map<String, Object> options) {
    
    final MessageRequestHandleImpl<InetSocketAddress, ByteBuffer> ret = new MessageRequestHandleImpl<InetSocketAddress, ByteBuffer>(i,m,options);
    
    ByteBuffer passThrough = ByteBuffer.allocate(m.remaining()+1);
    passThrough.put(HEADER_PASSTHROUGH_BYTE);
    passThrough.put(m);
    passThrough.flip();
    
    MessageCallback<InetSocketAddress, ByteBuffer> myCallback = null;
    if (deliverAckToMe != null) {
      myCallback = new MessageCallback<InetSocketAddress, ByteBuffer>() {

        public void ack(MessageRequestHandle<InetSocketAddress, ByteBuffer> msg) {
          deliverAckToMe.ack(ret);
        }
        
        public void sendFailed(
            MessageRequestHandle<InetSocketAddress, ByteBuffer> msg,
            Exception reason) {
          deliverAckToMe.sendFailed(ret, reason);
        }
      };
    }
    ret.setSubCancellable(tl.sendMessage(i, passThrough, myCallback, options));
    return ret;
  }
  
  public void messageReceived(InetSocketAddress i, ByteBuffer m,
      Map<String, Object> options) throws IOException {
    byte header = m.get();
    switch(header) {
    case HEADER_PASSTHROUGH_BYTE:
      callback.messageReceived(i, m, options);
      return;
    case HEADER_PROBE_RESPONSE_BYTE:
      long uid = m.getLong();
      // No need to remove them from the table, this will get done in destroy()
      verifyConnectionRequests.get(uid).udpSuccess(i, null);
    }
  }

  public void destroy() {
    verifyConnectionRequests.clear();
    tl.destroy();    
  }

  /**
   * Ask this strategy to probe a requesting node, but from a 3rd party node
   */
  protected ProbeStrategy probeStrategy;
  public void setProbeStrategy(ProbeStrategy probeStrategy) {
    this.probeStrategy = probeStrategy;
  }

  Map<Long, ConnectivityResult> verifyConnectionRequests = new HashMap<Long, ConnectivityResult>();
  
  /**
   * ask probeAddress to call probeStrategy.requestProbe()
   */
  public Cancellable verifyConnectivity(final MultiInetSocketAddress local,
      final InetSocketAddress probeAddress, 
      final ConnectivityResult deliverResultToMe,
      Map<String, Object> options) {
    AttachableCancellable ret = new AttachableCancellable();

    final long uid = environment.getRandomSource().nextLong();
    
    if (logger.level <= Logger.FINE) logger.log("verifyConnectivity("+local+","+probeAddress+"):"+uid);

    synchronized(verifyConnectionRequests) {
      verifyConnectionRequests.put(uid, deliverResultToMe);
    }
    
    // header has the PROBE_REQUEST and uid
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    try {
      sob.writeByte(HEADER_PROBE_REQUEST_BYTE);
      local.serialize(sob);
      sob.writeLong(uid);
    } catch (IOException ioe) {
      // shouldn't happen
      synchronized(verifyConnectionRequests) {
        verifyConnectionRequests.remove(uid);
      }      
      deliverResultToMe.receiveException(ioe);
      return null;
    }
    
    // if they cancel, pull it from the table
    ret.attach(new Cancellable() {    
      public boolean cancel() {
        synchronized(verifyConnectionRequests) {
          verifyConnectionRequests.remove(uid);
        }
        return true;
      }    
    });
    
    ret.attach(openSocket(probeAddress, sob.getBytes(), new SocketCallback<InetSocketAddress>() {    
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable,
          P2PSocket<InetSocketAddress> sock) {
        // maybe we should read a response here, but I don't think it's important, just read to close
        
        sock.register(true, false, new P2PSocketReceiver<InetSocketAddress>() {
          ByteBuffer readMe = ByteBuffer.allocate(1);
        
          public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
              boolean canRead, boolean canWrite) throws IOException {
            // read result
            long bytesRead = socket.read(readMe);

            if (bytesRead < 0) {
              deliverResultToMe.receiveException(new ClosedChannelException("Channel closed before reporting success/failure"));
              socket.close();
              return;
            }

            if (readMe.hasRemaining()) {
              socket.register(true,false,this);
              return;
            }
            
            readMe.flip();
            byte ret = readMe.get();
            if (ret == 1) {
              // true
            } else {
              deliverResultToMe.receiveException(new CantVerifyConnectivityException(probeAddress+" can't verify our connectivity for address "+local));
              return;
            }
          }
        
          public void receiveException(P2PSocket<InetSocketAddress> socket,
              Exception ioe) {
            deliverResultToMe.receiveException(ioe);
          }        
        });
        
//        final SocketInputBuffer sib = new SocketInputBuffer(sock);
//        
//        try {
//          new P2PSocketReceiver<InetSocketAddress>() {
//            
//            public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
//                boolean canRead, boolean canWrite) throws IOException {
//              // read response
//              
//              try {
//                InetSocketAddress addr = addrSerializer.deserialize(sib, null, null);
//                c.receiveResult(addr);
//              } catch (InsufficientBytesException ibe) {
//                socket.register(true, false, this);
//              } catch (Exception e) {
//                c.receiveException(e);
//              }
//            }
//          
//            public void receiveException(P2PSocket<InetSocketAddress> socket,
//                Exception ioe) {
//              c.receiveException(ioe);
//            }
//          
//          }.receiveSelectResult(sock, true, false);        
//        } catch (IOException ioe) {
//          c.receiveException(ioe);
//        }
      }
    
      public void receiveException(SocketRequestHandle<InetSocketAddress> s,
          Exception ex) {
        deliverResultToMe.receiveException(ex);
      }    
    }, options));
    return ret;
  }

  public Cancellable probe(final InetSocketAddress addr, final long uid, final Continuation<Long, Exception> deliverResponseToMe, final Map<String, Object> options) {
    if (logger.level <= Logger.FINE) logger.log("probe("+addr+","+uid+","+deliverResponseToMe+","+options+")");
    // udp
    final AttachableCancellable ret = new AttachableCancellable();
    ByteBuffer msg = ByteBuffer.allocate(9); // header+uid
    msg.put(HEADER_PROBE_RESPONSE_BYTE);
    msg.putLong(uid);
    msg.flip();
    
    // 0 = udp 1 = tcp
    // no need to synchronize, this should all be done on the selector
    final boolean[] success = new boolean[2];
    success[0] = false;
    success[1] = false;
    
    MessageCallback<InetSocketAddress, ByteBuffer> mc = null;
    if (deliverResponseToMe != null) {
      mc = new MessageCallback<InetSocketAddress, ByteBuffer>() {
        public void ack(MessageRequestHandle<InetSocketAddress, ByteBuffer> msg) {
          if (logger.level <= Logger.FINER) logger.log("probe("+addr+","+uid+","+deliverResponseToMe+","+options+").udpSuccess()");
          success[0] = true;
          if (success[1]) {
            deliverResponseToMe.receiveResult(uid);
          }
        }
            
        public void sendFailed(
            MessageRequestHandle<InetSocketAddress, ByteBuffer> msg, Exception reason) {
          if (logger.level <= Logger.FINER) logger.log("probe("+addr+","+uid+","+deliverResponseToMe+","+options+").udpFailure()");

          ret.cancel();
          deliverResponseToMe.receiveException(reason);
        }
      
      };
    }
    
    ret.attach(tl.sendMessage(addr, msg, mc, options));
    
    
    // tcp
    final ByteBuffer writeMe = ByteBuffer.allocate(9);
    writeMe.put(HEADER_PROBE_RESPONSE_BYTE);
    writeMe.putLong(uid);
    writeMe.flip();
    ret.attach(openSocket(addr, writeMe.array(), new SocketCallback<InetSocketAddress>() {    
      public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable,
          P2PSocket<InetSocketAddress> sock) {
        // maybe we should read a response here, but I don't think it's important, just read to close
        if (logger.level <= Logger.FINER) logger.log("probe("+addr+","+uid+","+deliverResponseToMe+","+options+").receiveResult("+sock+")");
        success[1] = true;
        if (success[0]) {
          deliverResponseToMe.receiveResult(uid);
        }
        sock.close();
        
//        sock.register(true, false, new P2PSocketReceiver<InetSocketAddress>() {
//        
//          public void receiveSelectResult(P2PSocket<InetSocketAddress> socket,
//              boolean canRead, boolean canWrite) throws IOException {
//            // we just want to record the socket closing
//            long bytesRead = socket.read(ByteBuffer.allocate(1));
//            
//            if (bytesRead < 0) {
//              // what we expect
//              socket.close();
//              return;
//            }
//
//            if (bytesRead == 0) {
//              // weird, but just reregister
//              socket.register(true,false,this);
//              return;
//            }
//            
//            if (bytesRead > 0) {
//              // this shouldn't happen, it should be closed, reregister anyway
//              if (logger.level <= Logger.WARNING) logger.log("Unexpected response on REQUEST_PROBE_SOCKET reregistering.");
//              socket.register(true,false,this);              
//              return;
//            }            
//          }
//        
//          public void receiveException(P2PSocket<InetSocketAddress> socket,
//              Exception ioe) {
//            if (logger.level <= Logger.FINER) logger.log("probe("+addr+","+uid+","+deliverResponseToMe+","+options+").tcpFailure() "+ioe);
//            if (deliverResponseToMe != null) deliverResponseToMe.receiveException(ioe);
//          }        
//        });        
      }
    
      public void receiveException(SocketRequestHandle<InetSocketAddress> s,
          Exception ex) {
        if (logger.level <= Logger.FINER) logger.log("probe("+addr+","+uid+","+deliverResponseToMe+","+options+").tcpFailure2() "+ex);
        if (deliverResponseToMe != null) deliverResponseToMe.receiveException(ex);
      }    
    }, options));    
    return ret;
  }
}
