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
package org.mpisws.p2p.testing.transportlayer;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.LivenessTransportLayerImpl;
import org.mpisws.p2p.transport.liveness.PingListener;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.multiaddress.MultiInetAddressTransportLayerImpl;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.proximity.MinRTTProximityProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayer;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayerImpl;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;
import org.mpisws.p2p.transport.wire.magicnumber.MagicNumberTransportLayer;

import rice.environment.Environment;
import rice.environment.logging.CloneableLogManager;
import rice.environment.params.Parameters;
import rice.selector.Timer;
import rice.selector.TimerTask;

public class LivenessTest extends SRTest {
  static TransportLayer dave; // going to be the middle hop for alice/bob
  static ProximityProvider<SourceRoute> bob_prox; // going to be the middle hop for alice/bob
  static SourceRouteFactory<MultiInetSocketAddress> srFactory;
  
  @Override
  public SourceRoute<MultiInetSocketAddress> getIdentifier(
      TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> a, 
      TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> b) {
    SourceRoute<MultiInetSocketAddress> src = a.getLocalIdentifier();
    SourceRoute<MultiInetSocketAddress> intermediate = carol_tap.getLocalIdentifier();
    SourceRoute<MultiInetSocketAddress> dest = b.getLocalIdentifier();

    if (a.equals(carol) || b.equals(carol)) {
      
      List<MultiInetSocketAddress> retArr = new ArrayList<MultiInetSocketAddress>(2);
      retArr.add(src.getFirstHop());
      retArr.add(dest.getFirstHop());
      
      return srFactory.getSourceRoute(retArr);      
    }
    
    List<MultiInetSocketAddress> retArr = new ArrayList<MultiInetSocketAddress>(3);
    retArr.add(src.getFirstHop());
    retArr.add(intermediate.getFirstHop());
    retArr.add(dest.getFirstHop());
    
    return srFactory.getSourceRoute(retArr);      
  }

  static Timer timer;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    srFactory = new MultiAddressSourceRouteFactory();
    TLTest.setUpBeforeClass();
    int startPort = 5009;
    logger = env.getLogManager().getLogger(MagicNumberTest.class, null);
    InetAddress addr = InetAddress.getLocalHost();    
//    InetAddress addr = InetAddress.getByName("10.0.0.10");
    Parameters p = env.getParameters();
    timer = env.getSelectorManager().getTimer();
    
    // this makes the liveness check super fast
      p.setInt("pastry_socket_scm_num_ping_tries", 2);
//      PING_DELAY = p.getInt("pastry_socket_scm_ping_delay");
//      PING_JITTER = p.getFloat("pastry_socket_scm_ping_jitter");
//      BACKOFF_INITIAL = p.getInt("pastry_socket_scm_backoff_initial");
//      BACKOFF_LIMIT = p.getInt("pastry_socket_scm_backoff_limit");
//      CHECK_DEAD_THROTTLE = p.getLong("pastry_socket_srm_check_dead_throttle"); // 300000
      p.setInt("pastry_socket_srm_default_rto", 400); // 3000 // 3 seconds
      p.setInt("pastry_socket_srm_rto_ubound", 500);//240000; // 240 seconds
//      RTO_LBOUND = p.getInt("pastry_socket_srm_rto_lbound");//1000;
//      gainH = p.getDouble("pastry_socket_srm_gain_h");//0.25;
//      gainG = p.getDouble("pastry_socket_srm_gain_g");//0.125;

    alice = buildTL("alice", addr, startPort, env);
    bob = buildTL("bob", addr, startPort+1, env);
    carol = buildTL("carol", addr, startPort+2, env);
    dave = buildTL("dave", addr, startPort+3, env);
    bob_prox = new MinRTTProximityProvider<SourceRoute>((LivenessTransportLayerImpl<SourceRoute>)bob, env);
  }

  private static TransportLayer buildTL(String name, InetAddress addr, int port, Environment env) throws IOException {
    Environment env_a = new Environment(
        env.getSelectorManager(), 
        env.getProcessor(), 
        env.getRandomSource(), 
        env.getTimeSource(), 
        ((CloneableLogManager) env.getLogManager()).clone(name),
        env.getParameters(), 
        env.getExceptionStrategy());    
    env.addDestructable(env_a);    
    InetSocketAddress addr_a = new InetSocketAddress(addr,port);
    SourceRouteTransportLayer srtl = 
        new SourceRouteTransportLayerImpl(srFactory,
          new MultiInetAddressTransportLayerImpl(new MultiInetSocketAddress(addr_a),
            new MagicNumberTransportLayer(
              new WireTransportLayerImpl(addr_a,env_a, null),
            env_a, null,GOOD_HDR, 2000),
          env_a, null, null),
        null, env_a, null);
    if (name.equals("carol")) carol_tap = srtl;
    return new TestLivenessTransportLayerImpl(srtl,env_a, null);
  }
  
  static class TestLivenessTransportLayerImpl extends LivenessTransportLayerImpl<SourceRoute> {    
    public TestLivenessTransportLayerImpl(TransportLayer<SourceRoute, ByteBuffer> tl, Environment env, ErrorHandler<SourceRoute> errorHandler) {
      super(tl, env, errorHandler, 5000);
    }
    
    @Override
    public void pong(final SourceRoute i, final long senderTime, final Map<String, Object> options) {
      timer.schedule(new TimerTask() {      
        @Override
        public void run() {
          TestLivenessTransportLayerImpl.super.pong(i, senderTime, options);      
        }      
      }, getDelay(getLocalIdentifier(), i));
    }
  }
  
  public static int getDelay(SourceRoute a, SourceRoute b) {
//    System.out.println("getDelay("+a+","+b+")");
    TransportLayer<SourceRoute, ByteBuffer> bob = LivenessTest.bob;
    if (b.getLastHop().equals(bob.getLocalIdentifier().getLastHop()) &&
        a.equals(alice.getLocalIdentifier())) {
//       System.out.println("here");
       return 150;
    }
//    System.out.println("here2 "+a+","+b+
//        " 1:"+b.getLastHop().equals(bob.getLocalIdentifier())+
//        " 1a:"+b.getLastHop()+
//        " 1b:"+bob.getLocalIdentifier().getLastHop()+
//        " 2:"+a.equals(alice.getLocalIdentifier()));
    return 0;
  }
  
  @Test
  public void testProximity() throws Exception {
    final Map<MultiInetSocketAddress, Tupel> pingResponse = new HashMap<MultiInetSocketAddress, Tupel>();
    final Object lock = new Object();
    
    PingListener<SourceRoute<MultiInetSocketAddress>> pl = new PingListener<SourceRoute<MultiInetSocketAddress>>() {    
      public void pingResponse(SourceRoute<MultiInetSocketAddress> i, int rtt, Map<String, Object> options) {
        synchronized(lock) {
//          System.out.println("i"+i.getLastHop());
          pingResponse.put(i.getLastHop(), new Tupel(i, rtt));
          lock.notify();
        }
      }   
      
      public void pingReceived(SourceRoute<MultiInetSocketAddress> i, Map<String, Object> options) {

      }
    };
    
    ((Pinger<SourceRoute<MultiInetSocketAddress>>)LivenessTest.bob).addPingListener(pl);

    bob_prox.proximity(getIdentifier(bob, alice), options); // initial check, causes ping if no data
    bob_prox.proximity(getIdentifier(bob, carol), options); // initial check, causes ping if no data

    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) && 
         (!pingResponse.containsKey(((SourceRoute)alice.getLocalIdentifier()).getLastHop()) ||
          !pingResponse.containsKey(((SourceRoute)carol.getLocalIdentifier()).getLastHop()))) {
        lock.wait(1000); 
      }
    }
    
    int aliceProx = bob_prox.proximity(getIdentifier(bob, alice), options);
    int carolProx = bob_prox.proximity(getIdentifier(bob, carol), options);

    assertTrue("aliceProx:"+aliceProx+" carolProx:"+carolProx,aliceProx > carolProx+100);
//    System.out.println("aliceProx"+aliceProx);
//    System.out.println("carolProx"+carolProx);
  }
  
  class Tupel {
    SourceRoute sr;
    int val;
    
    public Tupel(SourceRoute i, int val) {
      this.sr = i;
      this.val = val;
    }
  }
  
  @Test
  public void testLiveness() throws Exception {    
    LivenessProvider<SourceRoute<MultiInetSocketAddress>> alice = (LivenessProvider<SourceRoute<MultiInetSocketAddress>>)LivenessTest.alice;
    SourceRoute<MultiInetSocketAddress> aliceToDave = getIdentifier(
        (TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer>)LivenessTest.alice, dave);
    final List<Tupel> tupels = new ArrayList<Tupel>(3);
    final Object lock = new Object();
    
    alice.addLivenessListener(new LivenessListener<SourceRoute<MultiInetSocketAddress>>() {    
      public void livenessChanged(SourceRoute<MultiInetSocketAddress> i, int val, Map<String, Object> options) {
        synchronized(lock) {
          tupels.add(new Tupel(i,val));        
          lock.notify();
        }
      }    
    });
    
    // starts out suspected
    assertTrue(
        alice.getLiveness(aliceToDave, null) == 
          LivenessListener.LIVENESS_SUSPECTED);
    
    // becomes alive
    assertTrue(alice.checkLiveness(aliceToDave, null));

    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) && tupels.isEmpty()) {
        lock.wait(1000); 
      }
    }

    assertTrue(
        alice.getLiveness(aliceToDave, null) == 
        LivenessListener.LIVENESS_ALIVE);
    assertTrue(tupels.size() == 1);

    
    dave.destroy();    
    
    try { Thread.sleep(500); } catch (InterruptedException ie) {return;}

    // becomes alive
    assertTrue(alice.checkLiveness(aliceToDave, null));
    
    timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) && tupels.size() <= 2) {
        lock.wait(1000); 
      }
    }
    
    assertTrue(
        alice.getLiveness(aliceToDave, null) == 
        LivenessListener.LIVENESS_DEAD);
    // suspected/dead
    assertTrue(tupels.size() == 3);
  }
  
//  @Test
//  public void invalidNodeTest() throws Exception {
//    final ByteBuffer sentBuffer = ByteBuffer.wrap(sentBytes); 
//    
//    class Tupel<Identifier> {
//      Identifier i;
//      ByteBuffer buf;
//      public Tupel(Identifier i, ByteBuffer buf) {
//        this.i = i;
//        this.buf = buf;
//      }
//    }
//    
//    // added to every time bob receives something
//    final List<Exception> exceptionList = new ArrayList<Exception>(1);
//    final Object lock = new Object();
//    final List<ByteBuffer> sentList = new ArrayList<ByteBuffer>(1);
//    final List<ByteBuffer> failedList = new ArrayList<ByteBuffer>(1);
////    TestEnv<Identifier> tenv = getTestEnv();
//
//    alice.sendMessage(
//        getBogusIdentifier((SourceRoute)alice.getLocalIdentifier()), 
//        sentBuffer, 
//        null,
//        new MessageCallback<ByteBuffer, Exception>() {
//          
//          public void ack(ByteBuffer msg) {
//            synchronized(lock) {
//              sentList.add(msg);
//              lock.notify();
//            }        
//          }    
//          public void receiveException(ByteBuffer msg, Exception exception) {
//            synchronized(lock) {
//              exceptionList.add(exception);
//              lock.notify();
//            }
//          }
//          
//          public void sendFailed(ByteBuffer msg, FailureReason reason) {
//            synchronized(lock) {
//              failedList.add(msg);
//              lock.notify();
//            }                    
//          }    
//        });
//    
//    // block for completion
//    long timeout = env.getTimeSource().currentTimeMillis()+4000;
//    synchronized(lock) {
//      while((env.getTimeSource().currentTimeMillis()<timeout) && exceptionList.isEmpty() && sentList.isEmpty() && failedList.isEmpty()) {
//        lock.wait(1000); 
//      }
//    }
//
//    // results
//    assertTrue("sentList:"+sentList, sentList.isEmpty());  // no success
//    assertTrue(failedList.size() == 1);  // we got 1 result
//    assertTrue(failedList.get(0).equals(sentBuffer));  // result matches
//    assertTrue("exceptionList.size():"+exceptionList.size(), exceptionList.size() == 1);
//    if (!exceptionList.isEmpty()) throw exceptionList.get(0); // we got no exceptions
//    
//    alice.openSocket(getBogusIdentifier((SourceRoute)alice.getLocalIdentifier()), null, options);
//  }
}
