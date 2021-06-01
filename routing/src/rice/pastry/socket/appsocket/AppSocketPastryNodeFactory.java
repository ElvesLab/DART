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
package rice.pastry.socket.appsocket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
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
import org.mpisws.p2p.transport.TransportLayerListener;
import org.mpisws.p2p.transport.identity.IdentityImpl;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.OverrideLiveness;
import org.mpisws.p2p.transport.liveness.PingListener;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.util.OptionsFactory;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.wire.SocketManager;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;

import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.util.MathUtils;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.socket.SocketNodeHandleFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.socket.TransportLayerNodeHandle;
import rice.pastry.socket.nat.NATHandler;
import rice.pastry.transport.NodeHandleAdapter;
import rice.pastry.transport.SocketAdapter;
import rice.pastry.transport.TLDeserializer;

/**
 * Extends SocketPastryNodeFactory and adds getSocketFactory() to allow access to
 * a FreePastry application w/o joining the Ring.
 * 
 * @author Jeff Hoye
 *
 */
public class AppSocketPastryNodeFactory extends SocketPastryNodeFactory {

  /**
   * Used for getSocketChannel.  The AppSocketFactroyLayer stores the channel here, 
   * and then it is retrieved when the channel is opened.
   */
  protected Map<Integer, SocketManager> socketTable = new HashMap<Integer, SocketManager>();

  public AppSocketPastryNodeFactory(NodeIdFactory nf, int startPort,
      Environment env) throws IOException {
    super(nf, startPort, env);
    // TODO Auto-generated constructor stub
  }

  public AppSocketPastryNodeFactory(NodeIdFactory nf, InetAddress bindAddress,
      int startPort, Environment env) throws IOException {
    super(nf, bindAddress, startPort, env);
    // TODO Auto-generated constructor stub
  }

  public static final String STORE_SOCKET = "store_socket";
  
  class AppSocketFactoryLayer implements
    TransportLayer<InetSocketAddress, ByteBuffer>,
    TransportLayerCallback<InetSocketAddress, ByteBuffer> {
    
    protected WireTransportLayerImpl wtl;
    /**
     * Indexed by the option "STORE_SOCKET"
     */
    private TransportLayerCallback<InetSocketAddress, ByteBuffer> callback;
    
    public AppSocketFactoryLayer(WireTransportLayerImpl wtl) {
      this.wtl = wtl;
      this.wtl.setCallback(this);
    }
    
    public void acceptMessages(boolean b) {
      wtl.acceptMessages(b);
    }

    public void acceptSockets(boolean b) {
      wtl.acceptSockets(b);
    }

    public InetSocketAddress getLocalIdentifier() {      
      return wtl.getLocalIdentifier();
    }

    public SocketRequestHandle<InetSocketAddress> openSocket(
        final InetSocketAddress i,
        final SocketCallback<InetSocketAddress> deliverSocketToMe,
        final Map<String, Object> options) {
//      logger.log("openSocket("+i+","+options+")");

      return wtl.openSocket(i, new SocketCallback<InetSocketAddress>() {
      
        public void receiveResult(SocketRequestHandle<InetSocketAddress> cancellable,
            P2PSocket<InetSocketAddress> sock) {
//          logger.log("openSocket2("+i+","+options+")");

          SocketManager sa = (SocketManager)sock;
          if (options.containsKey(STORE_SOCKET)) {
            socketTable.put((Integer)options.get(STORE_SOCKET), sa);
          }
          deliverSocketToMe.receiveResult(cancellable, sock);
        }
      
        public void receiveException(SocketRequestHandle<InetSocketAddress> s,
            Exception ex) {
//          logger.logException("receiveException("+s+")",ex);
          deliverSocketToMe.receiveException(s,ex);
        }
      
      }, options);
    }

    public MessageRequestHandle<InetSocketAddress, ByteBuffer> sendMessage(
        InetSocketAddress i, ByteBuffer m,
        MessageCallback<InetSocketAddress, ByteBuffer> deliverAckToMe,
        Map<String, Object> options) {
      //logger.log("sendMessage("+m+","+options+")");
      return wtl.sendMessage(i, m, deliverAckToMe, options);
    }

    public void setCallback(
        TransportLayerCallback<InetSocketAddress, ByteBuffer> callback) {
      this.callback = callback;
    }

    public void setErrorHandler(ErrorHandler<InetSocketAddress> handler) {
      
    }

    public void incomingSocket(P2PSocket<InetSocketAddress> s)
        throws IOException {
      callback.incomingSocket(s);
    }

    public void messageReceived(InetSocketAddress i, ByteBuffer m,
        Map<String, Object> options) throws IOException {
      callback.messageReceived(i, m, options);
    }

    public void destroy() {
      // TODO Auto-generated method stub
      
    }
  }
  
  class BogusTLPastryNode extends PastryNode  {
    

    public BogusTLPastryNode(Id id, Environment e) {
      super(id, e);
    }

    public TransportLayer<InetSocketAddress, ByteBuffer> getWireTransportLayer(
        WireTransportLayerImpl wtl) {
      return new AppSocketFactoryLayer(wtl);
    }

    public void setLocalHandle(NodeHandle localhandle) {
      this.localhandle = localhandle;
    }

  }
  
  SocketFactory sf;
  public static final String SOCKET_FACTORY_UID = "appSocketFactory.uid";
  
  /**
   * @return used to open sockets to the remote node
   * @throws IOException
   */
  public synchronized SocketFactory getSocketFactory() throws IOException {
    if (sf != null) return sf;
    
    // build a bogus PastryNode for this
    Id nodeId = Id.build();
    final BogusTLPastryNode pn = new BogusTLPastryNode(nodeId, environment);    

    final SocketNodeHandleFactory handleFactory = (SocketNodeHandleFactory)getNodeHandleFactory(pn);    
    final NodeHandle localhandle = getLocalHandle(pn, handleFactory);    

    pn.setLocalHandle(localhandle);
    
    TLDeserializer deserializer = getTLDeserializer(handleFactory,pn);

    final NodeHandleAdapter nha = getNodeHandleAdapter(pn, handleFactory, deserializer);
    
    sf = new SocketFactory() {
      int uid = Integer.MIN_VALUE;
      public Cancellable getAppSocket(InetSocketAddress addr, int appid, final Continuation<AppSocket, Exception> c, Map<String, Object> options) {
        return getSocket(addr, appid, 
            new Continuation<P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>>, Exception>(){

              public void receiveException(Exception exception) {
                c.receiveException(exception);
              }

              public void receiveResult(
                  P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> result) {
                c.receiveResult(new SocketAdapter<TransportLayerNodeHandle<MultiInetSocketAddress>>(result, environment));
              }          
            }, options);
      }
      
      public Cancellable getSocketChannel(InetSocketAddress addr, int appid, final Continuation<SocketChannel, Exception> c, Map<String, Object> options) {
        // increment the uid
        final int myUid;
        synchronized(this) {
          myUid = uid++;
        }
        // add a UID in the options
        options = OptionsFactory.addOption(options, STORE_SOCKET, myUid);          

        // open the socket
        // the AppSocketFactoryLayer will intercept the SocketManager from the wtl using the UID

        return getSocket(addr, appid, 
            new Continuation<P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>>, Exception>(){

              public void receiveException(Exception exception) {
                c.receiveException(exception);
              }

              public void receiveResult(
                  P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> result) {
                // return the socket
                SocketManager sm = socketTable.remove(myUid); 
                SocketChannel ret = sm.getSocketChannel();
//                System.out.println(ret+" "+ret.isRegistered()+" "+ret.validOps());
                c.receiveResult(ret);
              }          
            }, options);

      } 
      
      
      protected Cancellable getSocket(InetSocketAddress addr, final int appid, final Continuation<P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>>, Exception> c, Map<String, Object> options) {
        TransportLayerNodeHandle<MultiInetSocketAddress> handle = getHandle(addr);
        final SocketRequestHandleImpl<TransportLayerNodeHandle<MultiInetSocketAddress>> ret = 
          new SocketRequestHandleImpl<TransportLayerNodeHandle<MultiInetSocketAddress>>(handle, options, logger);
        
        options = OptionsFactory.addOption(options, IdentityImpl.DONT_VERIFY, true);
        // open the socket
        // send the id
        ret.setSubCancellable(getTL().openSocket(handle, new SocketCallback<TransportLayerNodeHandle<MultiInetSocketAddress>>() {
        
          public void receiveResult(
              SocketRequestHandle<TransportLayerNodeHandle<MultiInetSocketAddress>> cancellable,
              final P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> sock) {
            ret.setSubCancellable(new Cancellable() {              
              public boolean cancel() {
                sock.close();
                return true;
              }
            });

            try {
              new P2PSocketReceiver<TransportLayerNodeHandle<MultiInetSocketAddress>>() {
                ByteBuffer buf;
                {
                  byte[] idBytes = MathUtils.intToByteArray(appid);
                  buf = ByteBuffer.wrap(idBytes);
                }
                
                public void receiveSelectResult(
                    P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> socket,
                    boolean canRead, boolean canWrite) throws IOException {
                  // send the id
                  if (socket.write(buf) < 0) {
                    c.receiveException(new ClosedChannelException("Socket was closed by remote host. "+socket));
                    return;
                  }
                  if (buf.hasRemaining()) {
                    socket.register(false, true, this);
                    return;
                  }
                  c.receiveResult(socket);
                }
                public void receiveException(
                    P2PSocket<TransportLayerNodeHandle<MultiInetSocketAddress>> socket,
                    Exception ioe) {
                  c.receiveException(ioe);
                }                
              }.receiveSelectResult(sock, false, true);
            } catch (IOException ioe) {
              c.receiveException(ioe);
            }
          }
        
          public void receiveException(
              SocketRequestHandle<TransportLayerNodeHandle<MultiInetSocketAddress>> s,
              Exception ex) {
            // TODO Auto-generated method stub
        
          }
        
        }, options));
        return ret;
      }

      public TransportLayerNodeHandle<MultiInetSocketAddress> getHandle(InetSocketAddress addr) {
        return handleFactory.getNodeHandle(new MultiInetSocketAddress(addr), -1, Id.build());
      }
      
      public TransportLayer<TransportLayerNodeHandle<MultiInetSocketAddress>, RawMessage> getTL() {
        return (TransportLayer<TransportLayerNodeHandle<MultiInetSocketAddress>, RawMessage>)nha.getTL();
      }
      
    };
    return sf;
  }
  
  /**
   * This code tells the WireTransportLayerImpl not to create a server-socket if we are using the BogusTLPastryNode
   */
  @Override
  protected TransportLayer<InetSocketAddress, ByteBuffer> getWireTransportLayer(InetSocketAddress innermostAddress, final PastryNode pn) throws IOException {
    Environment environment = pn.getEnvironment();    
    WireTransportLayerImpl wtl = new WireTransportLayerImpl(innermostAddress,environment, null, !(pn instanceof BogusTLPastryNode));    
    wtl.addSocketCountListener(getSocketCountListener(pn));
    
    if (pn instanceof BogusTLPastryNode) {
      return ((BogusTLPastryNode)pn).getWireTransportLayer(wtl);
    }
    
    return wtl;
  }

  /**
   * If it's for a remote node, eliminate the liveness layer, and just return bogus results
   */
  @Override
  protected TransLiveness<SourceRoute<MultiInetSocketAddress>, ByteBuffer> getLivenessTransportLayer(
      final TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> tl,
      PastryNode pn) {
    if (pn instanceof BogusTLPastryNode) {
      return new TransLiveness<SourceRoute<MultiInetSocketAddress>, ByteBuffer>(){    
        public TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> getTransportLayer() {
          return tl;
        }
        public LivenessProvider<SourceRoute<MultiInetSocketAddress>> getLivenessProvider() {
          return new LivenessProvider<SourceRoute<MultiInetSocketAddress>>(){

            public void addLivenessListener(
                LivenessListener<SourceRoute<MultiInetSocketAddress>> name) {
              // TODO Auto-generated method stub
              
            }

            public boolean checkLiveness(SourceRoute<MultiInetSocketAddress> i,
                Map<String, Object> options) {
              // TODO Auto-generated method stub
              return false;
            }

            public void clearState(SourceRoute<MultiInetSocketAddress> i) {
              // TODO Auto-generated method stub
              
            }

            public int getLiveness(SourceRoute<MultiInetSocketAddress> i,
                Map<String, Object> options) {
              // TODO Auto-generated method stub
              return LivenessListener.LIVENESS_ALIVE;
            }

            public boolean removeLivenessListener(
                LivenessListener<SourceRoute<MultiInetSocketAddress>> name) {
              // TODO Auto-generated method stub
              return false;
            }};
        }
        public OverrideLiveness<SourceRoute<MultiInetSocketAddress>> getOverrideLiveness() {
          return new OverrideLiveness<SourceRoute<MultiInetSocketAddress>>(){

            public void setLiveness(SourceRoute<MultiInetSocketAddress> i,
                int liveness, Map<String, Object> options) {
              // TODO Auto-generated method stub
              
            }};
        }
        public Pinger<SourceRoute<MultiInetSocketAddress>> getPinger() {
          return new Pinger<SourceRoute<MultiInetSocketAddress>>(){

            public void addPingListener(
                PingListener<SourceRoute<MultiInetSocketAddress>> name) {
              // TODO Auto-generated method stub
              
            }

            public boolean ping(SourceRoute<MultiInetSocketAddress> i,
                Map<String, Object> options) {
              // TODO Auto-generated method stub
              return false;
            }

            public boolean removePingListener(
                PingListener<SourceRoute<MultiInetSocketAddress>> name) {
              // TODO Auto-generated method stub
              return false;
            }};
        }    
      };
    }
  
    return super.getLivenessTransportLayer(tl, pn);
  }
  
  

}
