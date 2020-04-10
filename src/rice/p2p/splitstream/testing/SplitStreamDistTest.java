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
 * Created on Jul 13, 2005
 */
package rice.p2p.splitstream.testing;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.util.Random;

import rice.environment.Environment;
import rice.p2p.splitstream.ChannelId;
import rice.pastry.*;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * @author Jeff Hoye
 */
public class SplitStreamDistTest {
  public static final int DEFAULT_PORT = 53245; //13245;
  public static final int WAIT_TO_JOIN_DELAY = 3000; // 300000;
  public static final int WAIT_TO_SUBSCRIBE_DELAY = 3000; // 60000;
  public static final int IM_ALIVE_PERIOD = 5000;
    
  public static String INSTANCE = "DistSplitStreamTest";

//  public static final String BOOTNODE = "ricepl-3.cs.rice.edu";
  public static final String BOOTNODE = "swsao9509";
  
  public static final boolean nameSelf = true;
  
  /**
   * Usage java rice.p2p.splitstream.testing.SplitStreamDistTest [artificialchurn] [bootstrap] [port] [bootport]
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    
    String suffix = "";
    
    if (nameSelf) {
      suffix+="."+InetAddress.getLocalHost().getHostName();
    }
    String outfileString = "ss.txt"+suffix;
    
    // setup output
    PrintStream ps = new PrintStream(new FileOutputStream(outfileString, true));
    System.setErr(ps);
    System.setOut(ps);

    // setup environment
    final Environment env = new Environment();
    System.out.println("BOOTUP:"+env.getTimeSource().currentTimeMillis());
    
    
    // **************** parse args ***************
    // artificial churn
    boolean artificialChurn = false;
    if (args.length > 0) {
//      artificialChurn = Boolean.getBoolean(args[0]); 
    }
    
    // parse non automatic bootstrap
    String bootNode = BOOTNODE;
    if (args.length > 1) {
      bootNode = args[1]; 
    }

    // parse non automatic port
    int port = DEFAULT_PORT;
    if (args.length > 2) {
      port = Integer.parseInt(args[2]); 
    }
            
    boolean isBootNode = false;
    InetAddress localAddress = InetAddress.getLocalHost();
    if (localAddress.getHostName().startsWith(bootNode)) {
      isBootNode = true;
    }
    System.out.println("isBootNode:"+isBootNode);
        
    if (IM_ALIVE_PERIOD > 0) {
      new Thread(new Runnable() {
        public void run() {
          while(true) {
            System.out.println("ImALIVE:"+env.getTimeSource().currentTimeMillis());
            try {
              Thread.sleep(IM_ALIVE_PERIOD);
            } catch (Exception e) {}
          } 
        }
      },"ImALIVE").start();
    }
    
    if (!isBootNode) {
      int waitTime = env.getRandomSource().nextInt(WAIT_TO_JOIN_DELAY); 
      System.out.println("Waiting for "+waitTime+" millis before continuing..."+env.getTimeSource().currentTimeMillis());
      Thread.sleep(waitTime);
      System.out.println("Starting connection process "+env.getTimeSource().currentTimeMillis());
    }
    
    // test port bindings before proceeding
    boolean success = false;
    while(!success) {
      try {
        InetSocketAddress bindAddress = new InetSocketAddress(InetAddress.getLocalHost(),port);
        
        // udp test
        DatagramChannel channel = DatagramChannel.open();
        channel.configureBlocking(false);
        channel.socket().bind(bindAddress);
        channel.close();
        
        ServerSocketChannel channel1 = ServerSocketChannel.open();
        channel1.configureBlocking(false);
        channel1.socket().bind(bindAddress);
        channel1.close();
        
        success = true;
      } catch (Exception e) {
        System.out.println("Couldn't bind on port "+port+" trying "+(port+1));
        port++; 
        
      }
    }
    InetAddress bootaddr = InetAddress.getByName(bootNode);

    // make bootport
    int bootport = port;
    if (args.length > 3) {
      bootport = Integer.parseInt(args[3]);
    }
    InetSocketAddress bootaddress = new InetSocketAddress(bootaddr,bootport);

    // Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);
    
    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, port, env);

    // This will return null if we there is no node at that location
    NodeHandle bootHandle = ((SocketPastryNodeFactory)factory).getNodeHandle(bootaddress);
    
    if (bootHandle == null) {
      if (isBootNode) {
        // go ahead and start a new ring
      } else {
        // don't boot your own ring unless you are ricepl-1
        System.out.println("Couldn't find bootstrap... exiting.");        
        System.exit(23); 
      }
    }
    
    // construct a node, passing the null boothandle on the first loop will cause the node to start its own ring
    final PastryNode node = factory.newNode(bootHandle);
    System.out.println("Node "+node+" created.");
    
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() { System.out.println("SHUTDOWN "+env.getTimeSource().currentTimeMillis()+" "+node); }
    });
    
    synchronized(node) {
      while(!node.isReady()) {
        System.out.println("Waiting for node to go ready.  "+env.getTimeSource().currentTimeMillis());
        node.wait(5000); 
      }
    }

//    if (!isBootNode) {
//      System.out.println("Sleeping a minute at "+env.getTimeSource().currentTimeMillis());
//      Thread.sleep(1*60*1000);
//      System.out.println("Done sleeping at "+env.getTimeSource().currentTimeMillis());
//    }          
    MySplitStreamClient app = new MySplitStreamClient(node, INSTANCE);      
    ChannelId CHANNEL_ID = new ChannelId(generateId());    
    app.attachChannel(CHANNEL_ID);
    
    if (!isBootNode) {
      System.out.println("Sleeping(2) for "+WAIT_TO_SUBSCRIBE_DELAY+" at "+env.getTimeSource().currentTimeMillis());
      Thread.sleep(WAIT_TO_SUBSCRIBE_DELAY);
      System.out.println("Done(2) sleeping at "+env.getTimeSource().currentTimeMillis());
    }   
    
    app.subscribeToAllChannels();    
//    if (isBootNode) {
      app.startPublishTask(); 
//    }
  
    // this is to cause different connections to open
    if (artificialChurn) {
      while(true) {
        Thread.sleep(1*60*1000);
        if (!isBootNode) {
          if (env.getRandomSource().nextInt(60) == 0) {
            System.out.println("Killing self to cause churn. "+env.getTimeSource().currentTimeMillis()+":"+node);
            System.exit(25);
          }
        }
      }
    } 
  }
  
  private static Id generateId() {
    byte[] data = new byte[20];
    new Random(100).nextBytes(data);
    return rice.pastry.Id.build(data);
  }

  
}
