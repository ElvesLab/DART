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
package rice.tutorial.ssl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.Security;
import java.util.Map;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.identity.BindStrategy;
import org.mpisws.p2p.transport.identity.IdentityImpl;
import org.mpisws.p2p.transport.identity.LowerIdentity;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayer;
import org.mpisws.p2p.transport.sourceroute.SourceRouteTransportLayerImpl;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.ssl.SSLTransportLayer;
import org.mpisws.p2p.transport.ssl.SSLTransportLayerImpl;

import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandleSet;
import rice.pastry.NodeHandle;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.leafset.LeafSet;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.socket.TransportLayerNodeHandle;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * This tutorial shows how to setup a FreePastry node using the Socket Protocol.
 * 
 * @author Jeff Hoye
 */
public class DistTutorial {

  /**
   * This constructor sets up a PastryNode.  It will bootstrap to an 
   * existing ring if it can find one at the specified location, otherwise
   * it will start a new ring.
   * 
   * @param bindport the local port to bind to 
   * @param bootaddress the IP:port of the node to boot from
   * @param env the environment for these nodes
   */
  public DistTutorial(int bindport, InetSocketAddress bootaddress, Environment env, File keyStoreFile) throws Exception {
    
    // Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);

    // Add the bouncycastle security provider
    Security.addProvider(new BouncyCastleProvider());
    
    // create the keystore    
    final KeyStore store = KeyStore.getInstance("UBER", "BC");
    store.load(new FileInputStream(keyStoreFile), "".toCharArray());        
    
    // create the id from the file name
    rice.pastry.Id id = rice.pastry.Id.build(keyStoreFile.getName().split("\\.")[0]);
    
    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env) {
      @Override
      protected TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> getSourceRouteTransportLayer(
          TransportLayer<MultiInetSocketAddress, ByteBuffer> etl, 
          PastryNode pn, 
          MultiAddressSourceRouteFactory esrFactory) {

        // get the default layer by calling super
        TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> sourceRoutingTransportLayer = super.getSourceRouteTransportLayer(etl, pn, esrFactory);
        
        try {
          // return our layer
          return new SSLTransportLayerImpl<SourceRoute<MultiInetSocketAddress>, ByteBuffer>(sourceRoutingTransportLayer,store,store,pn.getEnvironment());
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }

      @Override
      protected BindStrategy<TransportLayerNodeHandle<MultiInetSocketAddress>, SourceRoute<MultiInetSocketAddress>> getBindStrategy() {
        return new BindStrategy<TransportLayerNodeHandle<MultiInetSocketAddress>, SourceRoute<MultiInetSocketAddress>>() {        
          public boolean accept(TransportLayerNodeHandle<MultiInetSocketAddress> u,
              SourceRoute<MultiInetSocketAddress> l, Map<String, Object> options) {
            
            // get the id from the certificate
            String idName = (String)options.get(SSLTransportLayer.OPTION_CERT_SUBJECT);
            
            // if it's not there, it could be because this is a UDP message, just accept
            if (idName != null) {              
              // compare the name to the id
              if (u.getId().toStringFull().equals(idName)) {
                // accept
                return true;
              } else {
                // reject
                System.out.println("Rejecting id:"+u+" which does not match the certificate entry:"+idName);
                return false;
              }
            }
            return true;
          }        
        };
      }
    };

    // construct a node with the id this time
    PastryNode node = factory.newNode(id);
      
    // construct a new MyApp
    MyApp app = new MyApp(node);    
    
    // boot the node
    node.boot(bootaddress);
    
    // the node may require sending several messages to fully boot into the ring
    synchronized(node) {
      while(!node.isReady() && !node.joinFailed()) {
        // delay so we don't busy-wait
        node.wait(500);
        
        // abort if can't join
        if (node.joinFailed()) {
          throw new IOException("Could not join the FreePastry ring.  Reason:"+node.joinFailedReason()); 
        }
      }       
    }
    
    System.out.println("Finished creating new node "+node);
    

    
    
    // wait 10 seconds
    env.getTimeSource().sleep(10000);
    
      
    // route 10 messages
    for (int i = 0; i < 10; i++) {
      // pick a key at random
      Id randId = nidFactory.generateNodeId();
      
      // send to that key
      app.routeMyMsg(randId);
      
      // wait a sec
      env.getTimeSource().sleep(1000);
    }

    // wait 10 seconds
    env.getTimeSource().sleep(10000);
    
    // send directly to my leafset
    LeafSet leafSet = node.getLeafSet();
    
    // this is a typical loop to cover your leafset.  Note that if the leafset
    // overlaps, then duplicate nodes will be sent to twice
    for (int i=-leafSet.ccwSize(); i<=leafSet.cwSize(); i++) {
      if (i != 0) { // don't send to self
        // select the item
        NodeHandle nh = leafSet.get(i);
        
        // send the message directly to the node
        app.routeMyMsgDirect(nh);   
        
        // wait a sec
        env.getTimeSource().sleep(1000);
      }
    }
  }

  /**
   * Usage: 
   * java [-cp FreePastry-<version>.jar] rice.tutorial.lesson3.DistTutorial localbindport bootIP bootPort
   * example java rice.tutorial.DistTutorial 9001 pokey.cs.almamater.edu 9001
   */
  public static void main(String[] args) throws Exception {
    // Loads pastry settings
    Environment env = new Environment();

    // disable the UPnP setting (in case you are testing this on a NATted LAN)
    env.getParameters().setString("nat_search_policy","never");
    
    try {
      // the port to use locally
      int bindport = Integer.parseInt(args[0]);
      
      // build the bootaddress from the command line args
      InetAddress bootaddr = InetAddress.getByName(args[1]);
      int bootport = Integer.parseInt(args[2]);
      InetSocketAddress bootaddress = new InetSocketAddress(bootaddr,bootport);
      
      // get the keystore file
      String keystoreFileName = args[3];
      File keystoreFile = new File(keystoreFileName);
      if (!keystoreFile.exists()) throw new IllegalArgumentException("The file: "+keystoreFileName+" was not found.");
      
      // launch our node!
      DistTutorial dt = new DistTutorial(bindport, bootaddress, env, keystoreFile);
    } catch (Exception e) {
      // remind user how to use
      System.out.println("Usage:"); 
      System.out.println("java [-cp FreePastry-<version>.jar] rice.tutorial.lesson3.DistTutorial localbindport bootIP bootPort");
      System.out.println("example java rice.tutorial.DistTutorial 9001 pokey.cs.almamater.edu 9001");
      throw e; 
    }
  }
}
