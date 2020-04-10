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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.bandwidthmeasure.BandwidthMeasuringTransportLayer;

import rice.environment.Environment;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.util.tuples.Tuple3;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.commonapi.PastryEndpoint;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.selector.TimerTask;
import rice.tutorial.transportlayer.BandwidthLimitingTransportLayer;

public class BandwidthMeasuringTLTest implements Application {

  static class MyMsg implements Message {

    public int getPriority() {
      return 0;
    }
    
  }
  
  public void deliver(Id id, Message message) {
//    System.out.println("Deliver. "+message);
  }

  public boolean forward(RouteMessage message) {
    // TODO Auto-generated method stub
    return true;
  }

  public void update(NodeHandle handle, boolean joined) {
    // TODO Auto-generated method stub
    
  }

  

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int bindPort = 9001;
    Environment env = new Environment();
    
    // Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);
    
    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    SocketPastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindPort, env) {

      @Override
      protected TransportLayer<InetSocketAddress, ByteBuffer> getWireTransportLayer(
          InetSocketAddress innermostAddress, PastryNode pn) throws IOException {
        TransportLayer<InetSocketAddress, ByteBuffer> wtl = super.getWireTransportLayer(innermostAddress, pn);
        
        final BandwidthLimitingTransportLayer<InetSocketAddress> ret = new BandwidthLimitingTransportLayer<InetSocketAddress>(wtl,40000,1000,pn.getEnvironment());
        
        final BandwidthMeasuringTransportLayer<InetSocketAddress> ret2 = new BandwidthMeasuringTransportLayer<InetSocketAddress>(5000,ret,pn.getEnvironment());
        
        pn.getEnvironment().getSelectorManager().schedule(new TimerTask() {
        
          @Override
          public void run() {
            Map<InetSocketAddress, Tuple3<Integer, Integer, Boolean>> map = ret2.getBandwidthUsed();
            for (InetSocketAddress addr : map.keySet()) {
              Tuple3<Integer, Integer, Boolean> t = map.get(addr);
              System.out.println("Bandwidth for "+addr+" d:"+t.a()+" u:"+t.b()+" saturated:"+t.c());
            }
            //ret.getVals(i)
          }        
        }, 3000, 3000);
        
        
        return ret2;
      }      
    };

    PastryNode node1 = factory.newNode();
    BandwidthMeasuringTLTest app1 = new BandwidthMeasuringTLTest();
    PastryEndpoint ep1 = (PastryEndpoint)node1.buildEndpoint(app1, null);
    ep1.register();
    node1.boot((Object)null);
    
    PastryNode node2 = factory.newNode();
    BandwidthMeasuringTLTest app2 = new BandwidthMeasuringTLTest();
    PastryEndpoint ep2 = (PastryEndpoint)node2.buildEndpoint(app2, null);
    ep2.register();
    node2.boot(new InetSocketAddress(factory.getBindAddress(), bindPort));
    
//    System.out.println(ep1.getAddress()+" "+ep2.getAddress());
    
    Thread.sleep(5000);
    System.out.println("Spamming messages.");
    
    long startTime = System.currentTimeMillis();
    while(System.currentTimeMillis() < startTime+5000) {
      for (int ctr = 0; ctr < 100; ctr++) {
        ep1.route(node2.getNodeId(), new MyMsg(), null);
      }
      Thread.sleep(10);
    }
    
    System.out.println("Done spamming messages.");
  }

}
