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
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTap;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayer;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayerImpl;
import org.mpisws.p2p.transport.sourceroute.factory.WireSourceRouteFactory;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;
import org.mpisws.p2p.transport.wire.magicnumber.MagicNumberTransportLayer;

import rice.environment.Environment;
import rice.environment.logging.CloneableLogManager;

public class WireSRTest extends TLTest<SourceRoute<InetSocketAddress>> {
  static SourceRouteTransportLayer<InetSocketAddress> carol_tap; // going to be the middle hop for alice/bob
  static TransportLayer carol;
  static SourceRouteFactory<InetSocketAddress> srFactory;

  @Override
  public SourceRoute getIdentifier(
      TransportLayer<SourceRoute<InetSocketAddress>, ByteBuffer> a, 
      TransportLayer<SourceRoute<InetSocketAddress>, ByteBuffer> b) {
    
    SourceRoute<InetSocketAddress> src = a.getLocalIdentifier();
    SourceRoute<InetSocketAddress> intermediate = (SourceRoute)carol.getLocalIdentifier();
    SourceRoute<InetSocketAddress> dest = b.getLocalIdentifier();
    
    List<InetSocketAddress> retArr = new ArrayList<InetSocketAddress>(3);
    retArr.add(src.getFirstHop());
    retArr.add(intermediate.getFirstHop());
    retArr.add(dest.getFirstHop());
    
    return srFactory.getSourceRoute(retArr);      
  }

  /**
   * Goes to Alice/Bob
   */
  public static final byte[] GOOD_HDR = {(byte)0xDE,(byte)0xAD,(byte)0xBE,(byte)0xEF};

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    srFactory = new WireSourceRouteFactory();
    TLTest.setUpBeforeClass();
    int startPort = START_PORT;
    logger = env.getLogManager().getLogger(MagicNumberTest.class, null);
    InetAddress addr = InetAddress.getLocalHost();    
//    InetAddress addr = InetAddress.getByName("10.0.0.10");
    
    alice = buildTL("alice", addr, startPort, env);
    bob = buildTL("bob", addr, startPort+1, env);
    carol_tap = (SourceRouteTransportLayer)buildTL("carol", addr, startPort+2, env);
    carol = carol_tap;
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
    return
      new SourceRouteTransportLayerImpl(srFactory,
        new MagicNumberTransportLayer(
          new WireTransportLayerImpl(addr_a,env_a, null),
        env_a, null,GOOD_HDR, 2000),
      null, env_a, null);    
  }
  
  static class Triplet {
    SourceRoute path;
    P2PSocket a,b;
    ByteBuffer m;
    
    public Triplet(SourceRoute path, P2PSocket a, P2PSocket b) {
      this.path = path;
      this.a = a;
      this.b = b;
    }
  }
  
  @Test
  @Override
  public void openTCP() throws Exception {
    final Object lock = new Object();
    final List<Triplet> opened = new ArrayList<Triplet>(1);
    final List<Triplet> closed = new ArrayList<Triplet>(1);
    final List<Triplet> received = new ArrayList<Triplet>(1);
    final List<Triplet> bad = new ArrayList<Triplet>(1);
    
    SourceRouteTap tap = new SourceRouteTap(){    
      public void socketOpened(SourceRoute path, P2PSocket a, P2PSocket b) {
        synchronized(lock) {
//          System.out.println("socketOpened("+path+","+a+","+b+")");
          opened.add(new Triplet(path, a, b));
          lock.notify();
        }
      }
    
      public void receivedMessage(ByteBuffer m, SourceRoute path) {
        bad.add(new Triplet(path, null, null));
      }
      
      public void socketClosed(SourceRoute path, P2PSocket a, P2PSocket b) {
        synchronized(lock) {
          closed.add(new Triplet(path, a, b));        
          lock.notify();
        }
      }
        
      public void receivedBytes(ByteBuffer m, SourceRoute path, P2PSocket a, P2PSocket b) {
        synchronized(lock) {
          Triplet t = new Triplet(path, a, b);
          t.m = m;
          received.add(t);                
          lock.notify();
        }
      }    
    };

    carol_tap.addSourceRouteTap(tap);
    super.openTCP();
    
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && bad.isEmpty() && (received.isEmpty() || opened.isEmpty() || opened.isEmpty())) {
        lock.wait(1000); 
      }
    }

    carol_tap.removeSourceRouteTap(tap);
    
    
  }

  @Test
  @Override
  public void sendUDP() throws Exception {
    class TapTupel {
      ByteBuffer m;
      SourceRoute path;
      public TapTupel(ByteBuffer m, SourceRoute path) {
        this.m = m;
        this.path = path;
      }
    }
    final Object lock = new Object();    
    final List<TapTupel> taptupels = new ArrayList<TapTupel>(1);
    
    SourceRouteTap tap = new SourceRouteTap(){    
      public void socketOpened(SourceRoute path, P2PSocket a, P2PSocket b) {
      }
    
      public void receivedBytes(ByteBuffer m, SourceRoute path, P2PSocket a, P2PSocket b) {
        
      }    
      
      public void socketClosed(SourceRoute path, P2PSocket a, P2PSocket b) {
        
      }
    
      public void receivedMessage(ByteBuffer m, SourceRoute path) {
//        System.out.println("receivedMessage("+m+","+path+"):"+m.remaining());        
        synchronized(lock) {
          taptupels.add(new TapTupel(m,path));
          lock.notify(); 
        }
      }    
    };

    carol_tap.addSourceRouteTap(tap);
    
    super.sendUDP();
    
    long timeout = env.getTimeSource().currentTimeMillis()+4000;
    synchronized(lock) {
      while((env.getTimeSource().currentTimeMillis()<timeout) && taptupels.isEmpty()) {
        lock.wait(1000); 
      }
    }
    
    carol_tap.removeSourceRouteTap(tap);
    
    // verify that we got the tap info, and it is all correct
    assertTrue(taptupels.size() == 1);
//    ByteBuffer m = taptupels.get(0).m;
//    assertTrue("m.remaining() = "+m.remaining()+" expected "+sentBytes.length, m.remaining() == sentBytes.length);
//    byte[] result = new byte[m.remaining()];
//    m.get(result);
//    assertTrue(Arrays.equals(result, sentBytes));
    assertTrue(taptupels.get(0).path.equals(getIdentifier(alice, bob)));
  }

  @Override
  public SourceRoute<InetSocketAddress> getBogusIdentifier(SourceRoute<InetSocketAddress> local) throws IOException {
    return srFactory.getSourceRoute(local.getFirstHop(), 
        new InetSocketAddress(InetAddress.getLocalHost(), START_PORT-2));
  }

}
