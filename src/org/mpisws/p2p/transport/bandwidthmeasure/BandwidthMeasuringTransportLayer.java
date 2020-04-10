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
package org.mpisws.p2p.transport.bandwidthmeasure;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;
import org.mpisws.p2p.transport.util.SocketWrapperSocket;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.util.tuples.Tuple;
import rice.p2p.util.tuples.Tuple3;
import rice.selector.TimerTask;

/**
 * Measure the bandwidth of the connections.
 * 
 * @author Jeff Hoye
 *
 * @param <Identifier>
 */
public class BandwidthMeasuringTransportLayer<Identifier> implements 
  TransportLayer<Identifier, ByteBuffer>, 
  TransportLayerCallback<Identifier, ByteBuffer> {

  TransportLayer<Identifier, ByteBuffer> tl; 
  TransportLayerCallback<Identifier, ByteBuffer> callback;

  Logger logger;
  ErrorHandler<Identifier> errorHandler;
  
  int measurementPeriod = 5000;

  // current measured
  public static final int CUR_DOWN = 0;
  public static final int CUR_UP = 1;
  public static final int CUR_SATURATED = 2;
  
  // last measured
  public static final int LAST_DOWN = 3;
  public static final int LAST_UP = 4;
  public static final int LAST_SATURATED = 5;

  public static final int NUM_VALS = 6;
  
  public static final int SATURATED = 1;
  public static final int NOT_SATURATED = 0;
  
  /**
   * Current measure, last measure
   */
  Map<Identifier, Tuple<int[], Collection<MySocket>>> measured = new HashMap<Identifier, Tuple<int[], Collection<MySocket>>>();
  protected rice.environment.time.TimeSource time;
  
  public BandwidthMeasuringTransportLayer(int measurementPeriod, TransportLayer<Identifier, ByteBuffer> tl, Environment env) {
    this.tl = tl;
    tl.setCallback(this);
    this.measurementPeriod = measurementPeriod;
    this.logger = env.getLogManager().getLogger(BandwidthMeasuringTransportLayer.class, null);
    this.errorHandler = new DefaultErrorHandler<Identifier>(logger);
    this.time = env.getTimeSource();
    this.lastMeasure = time.currentTimeMillis();
    
    env.getSelectorManager().schedule(new TimerTask() {
    
      @Override
      public void run() {
        measure();
      }
    
    },measurementPeriod,measurementPeriod);    
  }
  
  /**
   * Last time measure was called.
   */
  long lastMeasure;
  
  protected void measure() {
    
    synchronized(measured) {
      long now = time.currentTimeMillis();
      int diff = (int)(now - lastMeasure);
      lastMeasure = now;
      for (Identifier i : measured.keySet()) {
        Tuple<int[], Collection<MySocket>> t = measured.get(i);
        int[] vals = t.a();
        vals[LAST_DOWN] = vals[CUR_DOWN]*1000/diff;
        vals[LAST_UP] = vals[CUR_UP]*1000/diff;
        vals[LAST_SATURATED] = vals[CUR_SATURATED];
        vals[CUR_DOWN] = 0;
        vals[CUR_UP] = 0;
        vals[CUR_SATURATED] = NOT_SATURATED;
        
        // see if anyone was waiting to write
        if (vals[LAST_SATURATED] == NOT_SATURATED) {
          for (MySocket s : t.b()) {
            if (s.wantsToWrite()) {
              vals[LAST_SATURATED] = SATURATED;
              break;
            }
          }
        }
      }
    }
  }

  /**
   * Should be holding the lock on measured
   * 
   * @param i
   * @return
   */
  public Tuple<int[], Collection<MySocket>> getVals(Identifier i) {
    Tuple<int[], Collection<MySocket>> ret = measured.get(i);
    if (ret == null) {
      ret = new Tuple<int[], Collection<MySocket>>(new int[NUM_VALS], new ArrayList<MySocket>());
      measured.put(i, ret);      
    }
    return ret;
  }
  
  /**
   * For all Identifiers that used any bandwidth since the measurementPeriod, returns:
   * downstream bytes/sec, upstream bytes/sec, if the upstream was saturated
   * @return
   */
  public Map<Identifier, Tuple3<Integer, Integer, Boolean>> getBandwidthUsed() {
    synchronized(measured) {
      HashMap<Identifier, Tuple3<Integer, Integer, Boolean>> ret = new HashMap<Identifier, Tuple3<Integer, Integer, Boolean>>();
      for (Identifier i : measured.keySet()) {
        Tuple<int[], Collection<MySocket>> t = measured.get(i);
        int[] vals = t.a();
        ret.put(i,new Tuple3(vals[LAST_DOWN], vals[LAST_UP], (vals[LAST_SATURATED] == SATURATED)?true:false));
      }
      return ret;
    }
  }

  public void incomingSocket(P2PSocket<Identifier> s) throws IOException {
    callback.incomingSocket(new MySocket(s.getIdentifier(),s,logger,errorHandler,s.getOptions()));
  }

  class MySocket extends SocketWrapperSocket<Identifier, Identifier> {

    @Override
    public void close() {
      synchronized(measured) {
        getVals(identifier).b().remove(this);
      }
      super.close();
    }

    @Override
    public void receiveException(P2PSocket<Identifier> socket, Exception e) {
      synchronized(measured) {
        getVals(identifier).b().remove(this);
      }
      super.receiveException(socket, e);
    }

    /**
     * true if we are waiting to write.
     * @return
     */
    public boolean wantsToWrite() {
      return (writer != null);
    }
    
    @Override
    public void register(boolean wantToRead, boolean wantToWrite,
        P2PSocketReceiver<Identifier> receiver) {
      super.register(wantToRead, wantToWrite, receiver);
    }

    public MySocket(Identifier identifier, P2PSocket<Identifier> socket,
        Logger logger, ErrorHandler<Identifier> errorHandler,
        Map<String, Object> options) {
      super(identifier, socket, logger, errorHandler, options);
      synchronized(measured) {
        getVals(identifier).b().add(this);
      }
    }
    
    @Override
    public long write(ByteBuffer srcs) throws IOException {
      long ret = super.write(srcs);
      synchronized(measured) {
        int[] vals = getVals(getIdentifier()).a();
        if (srcs.hasRemaining()) {
          vals[CUR_SATURATED] = SATURATED;
        }
//        System.out.println("adding "+ret+" to "+CUR_UP);
        vals[CUR_UP]+=ret;
      }
      return ret;
    }
    
    @Override
    public long read(ByteBuffer srcs) throws IOException {
      // TODO Auto-generated method stub
      long ret = super.read(srcs);
      synchronized(measured) {
        int[] vals = getVals(getIdentifier()).a();
        vals[CUR_DOWN]+=ret;
      }
      return ret;
    }

  }
  
  public void messageReceived(Identifier i, ByteBuffer m,
      Map<String, Object> options) throws IOException {
    synchronized (measured) {
      getVals(i).a()[CUR_DOWN]+=m.remaining();
    }
    callback.messageReceived(i, m, options);
  }  

  public MessageRequestHandle<Identifier, ByteBuffer> sendMessage(Identifier i,
      ByteBuffer m, MessageCallback<Identifier, ByteBuffer> deliverAckToMe,
      Map<String, Object> options) {
    synchronized (measured) {
      getVals(i).a()[CUR_UP]+=m.remaining();
    }
    return tl.sendMessage(i, m, deliverAckToMe, options);
  }


  public SocketRequestHandle<Identifier> openSocket(final Identifier i,
      final SocketCallback<Identifier> deliverSocketToMe, 
      final Map<String, Object> options) {
    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(i,options,logger);
    ret.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>() {

      public void receiveException(SocketRequestHandle<Identifier> s,
          Exception ex) {
        deliverSocketToMe.receiveException(s, ex);
      }

      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> sock) {
        deliverSocketToMe.receiveResult(ret, new MySocket(i,sock,logger,errorHandler,options));
      }
    
    }, options));
    return ret;
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

  public void setCallback(
      TransportLayerCallback<Identifier, ByteBuffer> callback) {
    this.callback = callback;
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  public void destroy() {
    tl.destroy();
  }

}
