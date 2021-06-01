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
/*
 * Created on Jun 24, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package rice.p2p.past.testing;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;

import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.past.ContentHashPastContent;
import rice.p2p.past.Past;
import rice.p2p.past.PastContent;
import rice.p2p.past.PastContentHandle;
import rice.p2p.past.PastException;
import rice.p2p.past.PastImpl;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.leafset.LeafSet;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.persistence.LRUCache;
import rice.persistence.MemoryStorage;
import rice.persistence.PersistentStorage;
import rice.persistence.Storage;
import rice.persistence.StorageManagerImpl;

/**
 * @author jstewart
 */
@SuppressWarnings("unchecked")
public class DistPastTest {
  
    public DistPastTest(int bindport, InetSocketAddress bootaddress, Environment env, int numNodes) throws Exception {
      
      // Generate the NodeIds Randomly
      NodeIdFactory nidFactory = new RandomNodeIdFactory(env);
      Past p = null;
      Storage stor = null;

      // used for generating PastContent object Ids.
      // this implements the "hash function" for our DHT
      PastryIdFactory idf = new rice.pastry.commonapi.PastryIdFactory(env);
      
      // construct the PastryNodeFactory, this is how we use rice.pastry.socket
      PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env);

      // loop to construct the nodes/apps
      for (int curNode = 0; curNode < numNodes; curNode++) {
        // This will return null if we there is no node at that location
        NodeHandle bootHandle = ((SocketPastryNodeFactory)factory).getNodeHandle(bootaddress);
    
        // construct a node, passing the null boothandle on the first loop will cause the node to start its own ring
        PastryNode node = factory.newNode((rice.pastry.NodeHandle)bootHandle);
          
        // the node may require sending several messages to fully boot into the ring
        while(!node.isReady()) {
          // delay so we don't busy-wait
          Thread.sleep(100);
        }
        
        System.out.println("Finished creating new node "+node);
        
        
        stor = new PersistentStorage(idf,".",4*1024*1024,node.getEnvironment());
          p = new PastImpl(node, new StorageManagerImpl(idf,stor,new LRUCache(new MemoryStorage(idf),512*1024,node.getEnvironment())), 3, "");
      }
      Thread.sleep(5000);
        
      String s = "test" + env.getRandomSource().nextInt();
      PastContent dptc = new DistPastTestContent(env,idf,s);
      System.out.println("Inserting "+dptc);
      p.insert(dptc, new Continuation() {
        public void receiveResult(Object result) {
          Boolean[] results = ((Boolean[])result);
          for(int ctr = 0; ctr < results.length;ctr++) {
            System.out.println("got "+results[ctr].booleanValue());
          }
          
//          System.out.println("got: "+result);
//          System.out.println(result.getClass().getName());
        }

        public void receiveException(Exception result) {
          result.printStackTrace();
        }
      });

      Thread.sleep(5000);
      
      p.lookup(dptc.getId(), new Continuation() {
        public void receiveResult(Object result) {
          System.out.println("Got a "+result);
        }

        public void receiveException(Exception result) {
          result.printStackTrace();
        }
      });
    }

    /**
     * Usage: 
     * java [-cp FreePastry-<version>.jar] rice.tutorial.lesson4.DistTutorial localbindport bootIP bootPort numNodes
     * example java rice.tutorial.DistTutorial 9001 pokey.cs.almamater.edu 9001 10
     */
    public static void main(String[] args) throws Exception {
      // Loads pastry settings
      Environment env = new Environment();
      
      try {
        // the port to use locally
        int bindport = Integer.parseInt(args[0]);
        
        // build the bootaddress from the command line args
        InetAddress bootaddr = InetAddress.getByName(args[1]);
        int bootport = Integer.parseInt(args[2]);
        InetSocketAddress bootaddress = new InetSocketAddress(bootaddr,bootport);
    
        // the port to use locally
        int numNodes = Integer.parseInt(args[3]);    
        
        // launch our node!
        DistPastTest dt = new DistPastTest(bindport, bootaddress, env, numNodes);
      } catch (Exception e) {
        // remind user how to use
        System.out.println("Usage:"); 
        System.out.println("java [-cp FreePastry-<version>.jar] rice.tutorial.lesson4.DistTutorial localbindport bootIP bootPort numNodes");
        System.out.println("example java rice.tutorial.DistTutorial 9001 pokey.cs.almamater.edu 9001 10");
        throw e; 
      }
    }
  }

