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
package rice.tutorial.transportlayer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

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
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.selector.TimerTask;

public class BandwidthLimitingTransportLayer<Identifier> implements 
    TransportLayer<Identifier, ByteBuffer>,
    TransportLayerCallback<Identifier, ByteBuffer> {
  
  /**
   * The lower level transport layer.
   */
  protected TransportLayer<Identifier, ByteBuffer> tl;
  
  /**
   * The environment
   */
  protected Environment environment;
  protected Logger logger;
  
  
  /**
   * The size of the bucket.
   */
  protected int BUCKET_SIZE;
  /**
   * How often the bucket is refilled.
   */
  protected int BUCKET_TIME_LIMIT;
  
  /**
   * When this goes to zero, don't send messages
   */
  protected int bucket;

  protected ErrorHandler<Identifier> errorHandler;
  
  /**
   * To send 10K/second specify use
   * 
   * 10240,1000
   * 
   * @param bucketSize bytes to send
   * @param bucketTimelimit in millis
   */
  public BandwidthLimitingTransportLayer(
      TransportLayer<Identifier, ByteBuffer> tl, 
      int bucketSize, int bucketTimelimit, 
      Environment env) {
    this.environment = env;
    this.tl = tl;
    BUCKET_SIZE = bucketSize;
    BUCKET_TIME_LIMIT = bucketTimelimit;    
    logger = env.getLogManager().getLogger(BandwidthLimitingTransportLayer.class, null);
    this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    tl.setCallback(this);
    
    environment.getSelectorManager().getTimer().schedule(new TimerTask(){    
      @Override
      public void run() {
        // always synchronize on "this" before modifying the bucket
        synchronized(this) {
          bucket = BUCKET_SIZE;
          for (BandwidthLimitingSocket s : sockets) {
            s.notifyBandwidthRefilled();
          }
        }
      }    
    }, 0, BUCKET_TIME_LIMIT);
  }  
  
  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(Identifier i, ByteBuffer m, 
      final MessageCallback<Identifier, ByteBuffer> deliverAckToMe, Map<String, Object> options) {
    
    final MessageRequestHandleImpl<Identifier, ByteBuffer> returnMe = 
      new MessageRequestHandleImpl<Identifier, ByteBuffer>(i, m, options);
    
    boolean success = true;
    synchronized(this) {
      if (m.remaining() > bucket) {
        success = false;
      } else {
        bucket-=m.remaining();
      }
    }
    if (!success) {
      if (logger.level <= Logger.FINE) logger.log("Dropping message "+m+" because not enough bandwidth:"+bucket);
      if (deliverAckToMe != null) deliverAckToMe.sendFailed(returnMe, new NotEnoughBandwidthException(bucket, m.remaining()));
      return returnMe;
    }
    
    returnMe.setSubCancellable(tl.sendMessage(i,m,new MessageCallback<Identifier, ByteBuffer>() {
      public void ack(MessageRequestHandle<Identifier, ByteBuffer> msg) {
        if (deliverAckToMe != null) deliverAckToMe.ack(returnMe);
      }

      public void sendFailed(MessageRequestHandle<Identifier, ByteBuffer> msg, Exception reason) {
        if (deliverAckToMe != null) deliverAckToMe.sendFailed(returnMe, reason);
      }    
    },options));
    return returnMe;
  }  
  
  public SocketRequestHandle<Identifier> openSocket(Identifier i, final SocketCallback<Identifier> deliverSocketToMe, Map<String, Object> options) {
    final SocketRequestHandleImpl<Identifier> returnMe = new SocketRequestHandleImpl<Identifier>(i,options, logger);
    returnMe.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>(){
      public void receiveResult(SocketRequestHandle<Identifier> cancellable, P2PSocket<Identifier> sock) {
        deliverSocketToMe.receiveResult(returnMe, new BandwidthLimitingSocket(sock));
      }
    
      public void receiveException(SocketRequestHandle<Identifier> s, Exception ex) {
        deliverSocketToMe.receiveException(returnMe, ex);
      }
    }, options));
    return returnMe;
  }

  TransportLayerCallback<Identifier, ByteBuffer> callback;
  public void setCallback(TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
    callback.incomingSocket(new BandwidthLimitingSocket(s));
  }
  
  /**
   * Keep track of all of the BandwidthLimitingSockets
   */
  Collection<BandwidthLimitingSocket> sockets = new ArrayList<BandwidthLimitingSocket>();
  
  class BandwidthLimitingSocket extends SocketWrapperSocket<Identifier, Identifier> {
    public BandwidthLimitingSocket(P2PSocket<Identifier> socket) {
      super(socket.getIdentifier(), socket, 
          BandwidthLimitingTransportLayer.this.logger, 
          BandwidthLimitingTransportLayer.this.errorHandler,
          socket.getOptions());
      synchronized(BandwidthLimitingTransportLayer.this) {
        sockets.add(this);
      }
    }

    @Override
    public long write(ByteBuffer srcs) throws IOException {            
      if (srcs.remaining() <= bucket) {
        long ret = super.write(srcs);
        if (ret >= 0) {
          // EOF is usually -1
          synchronized(this) {
            bucket -= ret;
          }
        }
        return ret;
      }

      if (logger.level <= Logger.FINE) logger.log("Limiting "+socket+" to "+bucket+" bytes.");
      
      // The user is trying to write more than is allowed.  To handle this we 
      // will copy the allowed amount into a temporary ByteBuffer and pass that
      // to the next layer.  It is critical set the proper position of scrs 
      // before returning.  First, let's record the original position.
      int originalPosition = srcs.position();
      
      // now we create temp, who's size is "bucket"
      ByteBuffer temp = ByteBuffer.wrap(srcs.array(), originalPosition, bucket);

      // try to write our temp buffer
      long ret = super.write(temp);
      
      if (ret < 0) {
        // there was a problem
        return ret;
      }
      
      // allocate the bandwidth
      synchronized(this) {
        bucket -= ret;
      }
      
      // we need to properly set the position
      srcs.position(originalPosition+(int)ret);
      return ret;
    }

    /**
     * Store the write requester.  If this variable is not null 
     * it means that the storedWriter wants to write, but there
     * wasn't enough bandwidth.
     */
    P2PSocketReceiver<Identifier> storedWriter;
    
    @Override
    public void register(boolean wantToRead, boolean wantToWrite, P2PSocketReceiver<Identifier> receiver) {
      // this variable is what we will pass to super.register()
      boolean canWrite = wantToWrite;
      
      // if the user wants to write, and the bucket is empty, set our temp variable to false
      if (wantToWrite == true && bucket == 0) {
        canWrite = false;
        storedWriter = receiver;
      }

      // only call super.register() if we have something to do
      if (wantToRead || canWrite) super.register(wantToRead, canWrite, receiver);
    }

    /**
     * Register and clear the storedWriter
     */
    public void notifyBandwidthRefilled() {
      if (storedWriter != null) {
        P2PSocketReceiver<Identifier> temp = storedWriter;
        storedWriter = null;
        super.register(false, true, temp);
      }
    }
    
    public void close() {
      super.close();
      synchronized(BandwidthLimitingTransportLayer.this) {
        sockets.remove(this);      
      }
    }
    
    public void shutdownOutput() {
      super.shutdownOutput();
      synchronized(BandwidthLimitingTransportLayer.this) {
        sockets.remove(this);
      }
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

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
    tl.setErrorHandler(handler);
  }

  public void destroy() {
    tl.destroy();
  }

  public void messageReceived(Identifier i, ByteBuffer m, Map<String, Object> options) throws IOException {
    // notify listeners
    callback.messageReceived(i, m, options);
  }
  
  public static PastryNodeFactory exampleA(int bindport, Environment env, 
      NodeIdFactory nidFactory, final int amt, final int time) throws IOException {  
    
    // anonymously extend SPNF
    PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env) {
      
      /**
       * Override getWireTransportLayer to return the BandwidthLimitingTL wrapping
       * the default wire implementation.
       */
      @Override
      protected TransportLayer<InetSocketAddress, ByteBuffer> getWireTransportLayer(
          InetSocketAddress innermostAddress, PastryNode pn) throws IOException {
        // get the default layer
        TransportLayer<InetSocketAddress, ByteBuffer> wtl = 
          super.getWireTransportLayer(innermostAddress, pn);        
        
        // wrap it with our layer
        return new BandwidthLimitingTransportLayer<InetSocketAddress>(
            wtl, amt, time, pn.getEnvironment());
      }      
    };
    return factory;
  } 
  
  public static PastryNodeFactory exampleB(int bindport, Environment env, 
      NodeIdFactory nidFactory, final int amt, final int time) throws IOException {    
    PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env) {

      @Override
      protected TransLivenessProximity<MultiInetSocketAddress, ByteBuffer> getSourceRouteManagerLayer(
          TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> ltl, 
          LivenessProvider<SourceRoute<MultiInetSocketAddress>> livenessProvider, 
          Pinger<SourceRoute<MultiInetSocketAddress>> pinger, 
          PastryNode pn, 
          MultiInetSocketAddress proxyAddress, 
          MultiAddressSourceRouteFactory esrFactory) throws IOException {
        
        // get the default layer
        final TransLivenessProximity<MultiInetSocketAddress, ByteBuffer> srm = 
          super.getSourceRouteManagerLayer(
            ltl, livenessProvider, pinger, pn, proxyAddress, esrFactory);
        
        // wrap the default layer with our layer
        final BandwidthLimitingTransportLayer<MultiInetSocketAddress> bll = 
          new BandwidthLimitingTransportLayer<MultiInetSocketAddress>(
            srm.getTransportLayer(), amt, time, pn.getEnvironment());
        
        return new TransLivenessProximity<MultiInetSocketAddress, ByteBuffer>(){
          public TransportLayer<MultiInetSocketAddress, ByteBuffer> getTransportLayer() {
            return bll;
          }        
          public LivenessProvider<MultiInetSocketAddress> getLivenessProvider() {
            return srm.getLivenessProvider();
          }
          public ProximityProvider<MultiInetSocketAddress> getProximityProvider() {
            return srm.getProximityProvider();
          }
        };
      }
    };
    return factory;    
  }
}
