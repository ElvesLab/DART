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
 */
package rice.tutorial.past;

import java.io.IOException;
import java.net.*;
import java.util.Vector;

import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.past.*;
import rice.pastry.*;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.persistence.*;

/**
 * This tutorial shows how to use Past.
 * 
 * @author Jeff Hoye, Jim Stewart, Ansley Post
 */
public class PastTutorial {
  /**
   * this will keep track of our Past applications
   */
  Vector<Past> apps = new Vector<Past>();

  /**
   * Based on the rice.tutorial.scribe.ScribeTutorial
   * 
   * This constructor launches numNodes PastryNodes. They will bootstrap to an
   * existing ring if one exists at the specified location, otherwise it will
   * start a new ring.
   * 
   * @param bindport the local port to bind to
   * @param bootaddress the IP:port of the node to boot from
   * @param numNodes the number of nodes to create in this JVM
   * @param env the Environment
   */
  public PastTutorial(int bindport, InetSocketAddress bootaddress,
      int numNodes, final Environment env) throws Exception {
    
    // Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);

    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory,
        bindport, env);

    // loop to construct the nodes/apps
    for (int curNode = 0; curNode < numNodes; curNode++) {      
      // construct a node, passing the null boothandle on the first loop will
      // cause the node to start its own ring
      PastryNode node = factory.newNode();

      // used for generating PastContent object Ids.
      // this implements the "hash function" for our DHT
      PastryIdFactory idf = new rice.pastry.commonapi.PastryIdFactory(env);
      
      // create a different storage root for each node
      String storageDirectory = "./storage"+node.getId().hashCode();

      // create the persistent part
//      Storage stor = new PersistentStorage(idf, storageDirectory, 4 * 1024 * 1024, node
//          .getEnvironment());
      Storage stor = new MemoryStorage(idf);
      Past app = new PastImpl(node, new StorageManagerImpl(idf, stor, new LRUCache(
          new MemoryStorage(idf), 512 * 1024, node.getEnvironment())), 0, "");
      
      apps.add(app);      

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
      
      System.out.println("Finished creating new node " + node);      
    }
    
    // wait 5 seconds
    env.getTimeSource().sleep(5000);

    // We could cache the idf from whichever app we use, but it doesn't matter
    PastryIdFactory localFactory = new rice.pastry.commonapi.PastryIdFactory(env);

    // Store 5 keys
    // let's do the "put" operation
    System.out.println("Storing 5 keys");
    Id[] storedKey = new Id[5];
    for(int ctr = 0; ctr < storedKey.length; ctr++) {
      // these variables are final so that the continuation can access them
      final String s = "test" + env.getRandomSource().nextInt();
      
      // build the past content
      final PastContent myContent = new MyPastContent(localFactory.buildId(s), s);
    
      // store the key for a lookup at a later point
      storedKey[ctr] = myContent.getId();
      
      // pick a random past appl on a random node
      Past p = (Past)apps.get(env.getRandomSource().nextInt(numNodes));
      System.out.println("Inserting " + myContent + " at node "+p.getLocalNodeHandle());
      
      // insert the data
      p.insert(myContent, new Continuation<Boolean[], Exception>() {
        // the result is an Array of Booleans for each insert
        public void receiveResult(Boolean[] results) {          
          int numSuccessfulStores = 0;
          for (int ctr = 0; ctr < results.length; ctr++) {
            if (results[ctr].booleanValue()) 
              numSuccessfulStores++;
          }
          System.out.println(myContent + " successfully stored at " + 
              numSuccessfulStores + " locations.");
        }
  
        public void receiveException(Exception result) {
          System.out.println("Error storing "+myContent);
          result.printStackTrace();
        }
      });
    }
    
    // wait 5 seconds
    env.getTimeSource().sleep(5000);
    
    // let's do the "get" operation
    System.out.println("Looking up the 5 keys");
    
    // for each stored key
    for (int ctr = 0; ctr < storedKey.length; ctr++) {
      final Id lookupKey = storedKey[ctr];
      
      // pick a random past appl on a random node
      Past p = (Past)apps.get(env.getRandomSource().nextInt(numNodes));

      System.out.println("Looking up " + lookupKey + " at node "+p.getLocalNodeHandle());
      p.lookup(lookupKey, new Continuation<PastContent, Exception>() {
        public void receiveResult(PastContent result) {
          System.out.println("Successfully looked up " + result + " for key "+lookupKey+".");
        }
  
        public void receiveException(Exception result) {
          System.out.println("Error looking up "+lookupKey);
          result.printStackTrace();
        }
      });
    }
    
    // wait 5 seconds
    env.getTimeSource().sleep(5000);
    
    // now lets see what happens when we do a "get" when there is nothing at the key
    System.out.println("Looking up a bogus key");
    final Id bogusKey = localFactory.buildId("bogus");
    
    // pick a random past appl on a random node
    Past p = (Past)apps.get(env.getRandomSource().nextInt(numNodes));

    System.out.println("Looking up bogus key " + bogusKey + " at node "+p.getLocalNodeHandle());
    p.lookup(bogusKey, new Continuation<PastContent, Exception>() {
      public void receiveResult(PastContent result) {
        System.out.println("Successfully looked up " + result + " for key "+bogusKey+".  Notice that the result is null.");
        env.destroy();
      }

      public void receiveException(Exception result) {
        System.out.println("Error looking up "+bogusKey);
        result.printStackTrace();
        env.destroy();
      }
    });
  }

  /**
   * Usage: java [-cp FreePastry- <version>.jar]
   * rice.tutorial.past.PastTutorial localbindport bootIP bootPort numNodes
   * example java rice.tutorial.past.PastTutorial 9001 pokey.cs.almamater.edu 9001 10
   */
  public static void main(String[] args) throws Exception {
    // Loads pastry configurations
    Environment env = new Environment();

    // disable the UPnP setting (in case you are testing this on a NATted LAN)
    env.getParameters().setString("nat_search_policy","never");
    
    try {
      // the port to use locally
      int bindport = Integer.parseInt(args[0]);

      // build the bootaddress from the command line args
      InetAddress bootaddr = InetAddress.getByName(args[1]);
      int bootport = Integer.parseInt(args[2]);
      InetSocketAddress bootaddress = new InetSocketAddress(bootaddr, bootport);

      // the port to use locally
      int numNodes = Integer.parseInt(args[3]);

      // launch our node!
      PastTutorial dt = new PastTutorial(bindport, bootaddress, numNodes, env);
    } catch (Exception e) {
      // remind user how to use
      System.out.println("Usage:");
      System.out
          .println("java [-cp FreePastry-<version>.jar] rice.tutorial.past.PastTutorial localbindport bootIP bootPort numNodes");
      System.out
          .println("example java rice.tutorial.past.PastTutorial 9001 pokey.cs.almamater.edu 9001 10");
      throw e;
    }
  }
}

