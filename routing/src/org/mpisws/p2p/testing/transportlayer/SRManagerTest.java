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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.LivenessTransportLayerImpl;
import org.mpisws.p2p.transport.liveness.PingListener;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.multiaddress.MultiInetAddressTransportLayer;
import org.mpisws.p2p.transport.multiaddress.MultiInetAddressTransportLayerImpl;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.proximity.MinRTTProximityProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayer;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayerImpl;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.manager.SourceRouteManagerImpl;
import org.mpisws.p2p.transport.sourceroute.manager.SourceRouteStrategy;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;
import org.mpisws.p2p.transport.wire.magicnumber.MagicNumberTransportLayer;

import rice.environment.Environment;
import rice.environment.logging.CloneableLogManager;
import rice.environment.params.Parameters;
import rice.selector.Timer;
import rice.selector.TimerTask;

public class SRManagerTest extends TLTest<MultiInetSocketAddress> {
  /**
   * Goes to Alice/Bob
   */
  public static final byte[] GOOD_HDR = {(byte)0xDE,(byte)0xAD,(byte)0xBE,(byte)0xEF};

  static TransportLayer dave; // going to be the middle hop for alice/bob
  static ProximityProvider<SourceRoute<MultiInetSocketAddress>> bob_prox; // going to be the middle hop for alice/bob
  static SourceRouteTransportLayer carol_tap; // going to be the middle hop for alice/bob
  static TransportLayer carol;
  static SourceRouteFactory<MultiInetSocketAddress> srFactory;

  static Timer timer;

  static MultiInetSocketAddress alice_addr, bob_addr;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    srFactory = new MultiAddressSourceRouteFactory();
    TLTest.setUpBeforeClass();
    int startPort = START_PORT;
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
    MultiInetAddressTransportLayer myEpochLayer = new MultiInetAddressTransportLayerImpl(new MultiInetSocketAddress(addr_a),
        new MagicNumberTransportLayer(
          new WireTransportLayerImpl(addr_a,env_a, null),
        env_a, null, GOOD_HDR, 2000),
      env_a, null, null);
    final MultiInetSocketAddress myAddr = myEpochLayer.getLocalIdentifier();
    if (name.equals("alice")) alice_addr = myAddr;
    if (name.equals("bob")) bob_addr = myAddr;
    
    // don't allow direct connections from alice <-> bob
    SourceRouteTransportLayer<MultiInetSocketAddress> srtl = 
        new SourceRouteTransportLayerImpl<MultiInetSocketAddress>(
            srFactory,
            myEpochLayer,
            null, env_a, null) {

          @Override
          public void incomingSocket(P2PSocket<MultiInetSocketAddress> socka) throws IOException {
            if (connectionAllowed(myAddr, socka.getIdentifier(), "socket")) {
              super.incomingSocket(socka);
            }
          }

          @Override
          public void messageReceived(MultiInetSocketAddress i, ByteBuffer m, Map<String, Object> options) throws IOException {
            if (connectionAllowed(myAddr, i, "message")) {
              super.messageReceived(i, m, options);
            }
          }
          
        };
    if (name.equals("carol")) carol_tap = srtl;

    TestLivenessTransportLayerImpl temp = new TestLivenessTransportLayerImpl(srtl,env_a, null);
    MinRTTProximityProvider<SourceRoute<MultiInetSocketAddress>> prox = 
      new MinRTTProximityProvider<SourceRoute<MultiInetSocketAddress>>(temp, env_a);
    if (name.equals("bob")) bob_prox = prox;
    
    return new SourceRouteManagerImpl<MultiInetSocketAddress>(srFactory, temp, temp, prox, env_a, 
        new TestSRS(temp.getLocalIdentifier().getLastHop()));
  }
  
  static boolean connectionAllowed(MultiInetSocketAddress a, MultiInetSocketAddress b, String context) {
    if (a.equals(alice_addr)) {
      if (b.equals(bob_addr)) {
//        System.out.println("Alice rejects Bob for "+context);
        return false;
      }      
    }
    if (b.equals(alice_addr)) {
      if (a.equals(bob_addr)) {
//        System.out.println("Bob rejects Alice for "+context);
        return false;
      }      
    }
//    System.out.println(b+"->"+a+":"+context);
    return true; 
  }
  
  static class TestSRS implements SourceRouteStrategy<MultiInetSocketAddress> {
    MultiInetSocketAddress local;
    
    public TestSRS(MultiInetSocketAddress local) {
      this.local = local;
    }

    public Collection<SourceRoute<MultiInetSocketAddress>> getSourceRoutes(MultiInetSocketAddress destination) {
      ArrayList<MultiInetSocketAddress> eList = new ArrayList<MultiInetSocketAddress>();
      eList.add((MultiInetSocketAddress)alice.getLocalIdentifier());
      eList.add((MultiInetSocketAddress)bob.getLocalIdentifier());
      eList.add((MultiInetSocketAddress)carol.getLocalIdentifier());
      
      eList.remove(local);
      eList.remove(destination);
      
      ArrayList<SourceRoute<MultiInetSocketAddress>> srList = new ArrayList<SourceRoute<MultiInetSocketAddress>>();
      ArrayList<MultiInetSocketAddress> path = new ArrayList<MultiInetSocketAddress>(2);
      path.add(local);
      path.add(destination);
      srList.add(srFactory.getSourceRoute(path));
      
      for (MultiInetSocketAddress eAddr : eList) {
        path = new ArrayList<MultiInetSocketAddress>(3);
        path.add(local);
        path.add(eAddr);
        path.add(destination);
        srList.add(srFactory.getSourceRoute(path));
      }
      
      return srList;
    }    
  }
  
  static class TestLivenessTransportLayerImpl extends LivenessTransportLayerImpl<SourceRoute<MultiInetSocketAddress>> {    
    public TestLivenessTransportLayerImpl(TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> tl, Environment env, ErrorHandler<SourceRoute<MultiInetSocketAddress>> errorHandler) {
      super(tl, env, errorHandler, 5000);
    }
    
    @Override
    public void pong(final SourceRoute<MultiInetSocketAddress> i, final long senderTime, final Map<String, Object> options) {
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
//    TransportLayer<SourceRoute, ByteBuffer> bob = SRManagerTest.bob;
    if (b.getLastHop().equals(bob.getLocalIdentifier()) &&
        b.getFirstHop().equals(alice.getLocalIdentifier())) {
//       System.out.println("here");
       return 150;
    }
//    System.out.println("here2 "+a+","+b+
//        " 1:"+b.getLastHop().equals(bob.getLocalIdentifier())+
//        " 1a:"+b.getLastHop()+
//        " 1b:"+bob.getLocalIdentifier().getLastHop()+
//        " 2:"+a.equals(alice.getLocalIdentifier()));
    return 2;
  }
  
  @Test
  public void testProximity() throws Exception {
    final Map<MultiInetSocketAddress, Tupel> pingResponse = new HashMap<MultiInetSocketAddress, Tupel>();
    final Object lock = new Object();
    
    PingListener<MultiInetSocketAddress> pl = new PingListener<MultiInetSocketAddress>() {    
      public void pingResponse(MultiInetSocketAddress i, int rtt, Map<String, Object> options) {
        synchronized(lock) {
//          System.out.println("i"+i.getLastHop());
          pingResponse.put(i, new Tupel(i, rtt));
          lock.notify();
        }
      }
      
      public void pingReceived(MultiInetSocketAddress i, Map<String, Object> options) {

      }
    };
    
    ((Pinger<MultiInetSocketAddress>)SRManagerTest.bob).addPingListener(pl);

    ((ProximityProvider)bob).proximity(alice.getLocalIdentifier(), options); // initial check, causes ping if no data
    ((ProximityProvider)bob).proximity(carol.getLocalIdentifier(), options); // initial check, causes ping if no data

    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) && 
         (!pingResponse.containsKey((MultiInetSocketAddress)alice.getLocalIdentifier()) ||
          !pingResponse.containsKey((MultiInetSocketAddress)carol.getLocalIdentifier()))) {
        lock.wait(1000); 
      }
    }
    
    int aliceProx = ((ProximityProvider)bob).proximity(alice.getLocalIdentifier(), options);
    int carolProx = ((ProximityProvider)bob).proximity(carol.getLocalIdentifier(), options);

    assertTrue("aliceProx:"+aliceProx+" carolProx:"+carolProx,aliceProx > carolProx+100);
//    System.out.println("aliceProx"+aliceProx);
//    System.out.println("carolProx"+carolProx);
  }
  
  class Tupel {
    MultiInetSocketAddress sr;
    int val;
    
    public Tupel(MultiInetSocketAddress i, int val) {
      this.sr = i;
      this.val = val;
    }
  }
  
  @Test
  public void testLiveness() throws Exception {    
    LivenessProvider<MultiInetSocketAddress> alice = (LivenessProvider<MultiInetSocketAddress>)SRManagerTest.alice;
    MultiInetSocketAddress daveAddress = (MultiInetSocketAddress)dave.getLocalIdentifier();
//    SourceRoute aliceToDave = getIdentifier(alice, dave);
    final List<Tupel> tupels = new ArrayList<Tupel>(3);
    final Object lock = new Object();
    
    alice.addLivenessListener(new LivenessListener<MultiInetSocketAddress>() {    
      public void livenessChanged(MultiInetSocketAddress i, int val, Map<String, Object> options) {
        synchronized(lock) {
//          System.out.println("adding("+i+","+val+")");
          tupels.add(new Tupel(i,val));        
          lock.notify();
        }
      }    
    });
    
    // starts out suspected
    assertTrue(
        alice.getLiveness(daveAddress, null) == 
        LivenessListener.LIVENESS_SUSPECTED);
    
//    System.out.println("here");
    // becomes alive
//    assertTrue(alice.checkLiveness(daveAddress));

    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) && tupels.isEmpty()) {
        lock.wait(1000); 
      }
    }

    int result = alice.getLiveness(daveAddress, null);
    assertTrue("result = "+result, result == LivenessListener.LIVENESS_ALIVE);
    assertTrue(tupels.size() == 1);

    
    dave.destroy();    
    
    try { Thread.sleep(500); } catch (InterruptedException ie) {return;}

    // becomes alive
    assertTrue(alice.checkLiveness(daveAddress, null));
    
    timeout = env.getTimeSource().currentTimeMillis()+5000;
    synchronized(lock) {
      while( (env.getTimeSource().currentTimeMillis()<timeout) && tupels.size() <= 2) {
        lock.wait(1000); 
      }
    }
    
    result = alice.getLiveness(daveAddress, null);
    assertTrue("result = "+result, 
        result == LivenessListener.LIVENESS_DEAD);
    // suspected/dead
    assertTrue(tupels.size() == 3);
  }
  
  @Override
  public MultiInetSocketAddress getBogusIdentifier(MultiInetSocketAddress local) throws IOException {
    return new MultiInetSocketAddress(new InetSocketAddress(InetAddress.getLocalHost(), START_PORT-2));
  }

}
