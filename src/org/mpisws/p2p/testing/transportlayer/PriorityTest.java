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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.liveness.LivenessTransportLayerImpl;
import org.mpisws.p2p.transport.multiaddress.MultiInetAddressTransportLayerImpl;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.priority.PriorityTransportLayerImpl;
import org.mpisws.p2p.transport.priority.QueueOverflowException;
import org.mpisws.p2p.transport.proximity.MinRTTProximityProvider;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;

import rice.environment.Environment;
import rice.environment.logging.CloneableLogManager;

public class PriorityTest extends TLTest<InetSocketAddress> {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TLTest.setUpBeforeClass();
    logger = env.getLogManager().getLogger(PriorityTest.class, null);
    
    int startPort = START_PORT;
    InetAddress addr = InetAddress.getLocalHost();    
//    InetAddress addr = InetAddress.getByName("10.0.0.10");
    alice = buildTL("alice", addr, startPort, env);
    bob = buildTL("bob", addr, startPort+1, env);
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
    
    LivenessTransportLayerImpl<MultiInetSocketAddress> ltli = new LivenessTransportLayerImpl<MultiInetSocketAddress>(
        new MultiInetAddressTransportLayerImpl(new MultiInetSocketAddress(addr_a),
          new WireTransportLayerImpl(addr_a,env_a, null), 
        env_a, null, null),
      env_a, null, 5000);
    
    ProximityProvider<MultiInetSocketAddress> prox = new MinRTTProximityProvider<MultiInetSocketAddress>(ltli, env_a);

    
    return new PriorityTransportLayerImpl<MultiInetSocketAddress>(ltli, ltli, prox,
           env_a, 1024, 30, null);
  }

  @Test
  public void bogus() {} 
  
  @Test
  public void queueOverflow() throws IOException {
    MultiInetSocketAddress bogus = new MultiInetSocketAddress(getBogusIdentifier(null));
    
    final ArrayList<MessageRequestHandle> dropped = new ArrayList<MessageRequestHandle>(10);
    
    for (int ctr = 0; ctr < 40; ctr++) {
      HashMap<String, Integer> options = new HashMap<String, Integer>();
      options.put(PriorityTransportLayer.OPTION_PRIORITY, -ctr); // keep adding higher priority messages, to verify 
      // the early, low priority messages are dropped
      
      alice.sendMessage(bogus, ByteBuffer.wrap(new byte[ctr]), new MessageCallback<MultiInetSocketAddress, ByteBuffer>(){
        public void sendFailed(MessageRequestHandle<MultiInetSocketAddress, ByteBuffer> msg, Exception reason) {
//          System.out.println("sendFailed");
          if (reason instanceof NodeIsFaultyException) {
          } else {
            if (reason instanceof QueueOverflowException) {
              synchronized(dropped) {
                System.out.println("Dropped "+msg);
                dropped.add(msg);
                if (dropped.size() >= 10) dropped.notify();
              }            
            } else {
              reason.printStackTrace();              
            }
          }
        }
        public void ack(MessageRequestHandle<MultiInetSocketAddress, ByteBuffer> msg) {
          System.out.println("ack");
        }
      }, options);
    }
    
    System.out.println("sleeping");
    synchronized(dropped) {
      if (dropped.size() < 10) 
        try { dropped.wait(10000); } catch (InterruptedException ie) {}
    }
    System.out.println("done sleeping"+dropped.size());
    
    
  } 
  
  @Override
  public InetSocketAddress getBogusIdentifier(InetSocketAddress local) throws IOException {
    return new InetSocketAddress(InetAddress.getLocalHost(), START_PORT-2);
  }

}
