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
 * Created on Jul 5, 2005
 */
package rice.pastry.testing;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;

import rice.environment.Environment;
import rice.pastry.*;
import rice.pastry.dist.DistPastryNodeFactory;
import rice.pastry.leafset.LeafSet;
import rice.pastry.socket.*;

/**
 * Pass in a certificate which contains bootstrap nodes.  
 * 
 * Output is an ordered list of independent rings, and which bootstrap nodes belong 
 * to each.
 * 
 * @author Jeff Hoye
 */
public class PartitionChecker {

  protected final int MAX_THREADS = 100;

  int numThreads = 0;

  /**
   * Set of InetSocketAddress 
   * 
   * The set of bootstrap nodes that need to be checked.  
   * Gained from the certificate.
   * 
   * This list is reduced each time a node from this ring is identified
   */
  HashSet<InetSocketAddress> unmatchedBootstraps;
  
  /**
   * of InetSocketAddress.  This list is kept around.
   */
  HashSet<InetSocketAddress> bootstraps;

  final PrintStream ps = new PrintStream(new FileOutputStream("response.txt"));

  
  /**
   * A list of Ring.  Increased whenever a new one is found.
   */
  ArrayList<Ring> rings;
  
  Environment environment;

  HashSet<NodeHandle> dead = new HashSet<NodeHandle>();    

  public PartitionChecker(String ringIdString) throws Exception {
    environment = new Environment();
    buildBootstrapSetFromCert(ringIdString); 
    rings = new ArrayList<Ring>();
    
    SocketPastryNodeFactory factory = new SocketPastryNodeFactory(null, 1, environment);
    
    
    while(unmatchedBootstraps.size() > 0) {
      rings.add(buildRing(factory, (InetSocketAddress)(unmatchedBootstraps.iterator().next()))); 
    }
    Collections.sort(rings);
    Iterator<Ring> i = rings.iterator();
    while(i.hasNext()) {
      System.out.println(i.next()); 
    }
  }
  
  
  
  protected void buildBootstrapSetFromCert(String ringIdString) throws Exception {
    unmatchedBootstraps = new HashSet<InetSocketAddress>(); 
    bootstraps = new HashSet<InetSocketAddress>(); 
    
//    byte[] ringIdbytes = ringIdString.getBytes();
    
    
       
//    InetSocketAddress[] addr = RingCertificate.getCertificate(null).getBootstraps();
  
    BufferedReader in = new BufferedReader(new FileReader(ringIdString));

    String line;
    
    while(in.ready()) {
      line = in.readLine();
      String[] parts = line.split(":");
      
      int port = 10003;
      if (parts.length > 1) {
        port = Integer.parseInt(parts[1]);
      }
      InetSocketAddress addr = new InetSocketAddress(parts[0], port);
      
      unmatchedBootstraps.add(addr);
      bootstraps.add(addr);
    }
  }
  
  protected LeafSet getLeafSet(NodeHandle nh) throws IOException {
    // TODO: implement, need to make this an application and have a message to fetch this info
    
    return null;
  }
  
  protected Ring buildRing(final SocketPastryNodeFactory factory, InetSocketAddress bootstrap) throws Exception {
//    System.out.println("buildRing("+bootstrap+")");
    unmatchedBootstraps.remove(bootstrap);
    numThreads = 0;
    final Ring ring = new Ring(bootstrap);

//    final HashMap leafsets = new HashMap();
    final HashSet<NodeHandle> unseen = new HashSet<NodeHandle>();
    
    
    unseen.add(factory.getNodeHandle(bootstrap, 20000));

    synchronized (unseen) {
      while (true) {
        if (numThreads >= MAX_THREADS) 
          unseen.wait();
        
        if (unseen.size() > 0) {
          numThreads++;
          
          final SocketNodeHandle handle = (SocketNodeHandle) unseen.iterator().next();          
          if (handle == null) {
            break;
          }
            unseen.remove(handle);
            ring.addNode(handle);
//            System.out.println("Fetching leafset of " + handle + " (thread " + numThreads + " of "+MAX_THREADS+")");
            
            Thread t = new Thread() {
              public void run() {  
                try {
                  LeafSet ls = getLeafSet(handle);
//                  System.out.println("Response:"+handle+" "+ring.getName()+" "+ls);
                  
                  ps.println(handle.getInetSocketAddress().getAddress().getHostAddress()+":"+
                      handle.getInetSocketAddress().getPort());
          //        SourceRoute[] routes = factory.getRoutes(handle);
                  
          //        for (int i=0; i<routes.length; i++) 
          //          System.out.println("ROUTE:\t" + routes[i].prepend(handle.getEpochAddress()));
                  
                  
                  NodeSet ns = ls.neighborSet(Integer.MAX_VALUE);
                  
                  if (! ns.get(0).equals(handle)) {
                    ring.addFailure(handle, new Exception("Node is now "+ns.get(0))); 
                  }
                  
                  synchronized (unseen) {
                    for (int i=1; i<ns.size(); i++) 
                      if ((! ring.contains(ns.get(i))) && (! dead.contains(ns.get(i))))
                        unseen.add(ns.get(i));
                  }
                
                } catch (java.net.ConnectException e) {
                  ring.addFailure(handle,e);
                  dead.add(handle);
                } catch (java.net.SocketTimeoutException e) {
                  ring.addFailure(handle,e);
                  dead.add(handle);
                } catch (IOException e) {
//                  environment.getLogManager().getLogger(PastryNetworkTest.class, null).log(Logger.WARNING,"GOT OTHER ERROR CONNECTING TO " + handle + " - " + e);
                } finally {
                  synchronized (unseen) {
                    numThreads--;
                    unseen.notifyAll();
                  }
                }
              }
            };
            
            t.start();
          } else if (numThreads > 0) {
            unseen.wait();
          } else {
            break;
          }
        }
      }
        
//    System.out.println("Fetched all leafsets - return...  Found " + nodes.size() + " nodes.");

    System.out.println("buildRing() complete:"+ring);
    return ring;
  }

  
  class Ring implements Comparable<Ring> {
    /**
     * of InetSocketAddress
     */
    HashSet<InetSocketAddress> myBootstraps;
    /**
     * of NodeHandle
     */
    HashSet<NodeHandle> nodes;
    
    String name;
    
    public Ring(InetSocketAddress bootAddr) {
      name = bootAddr.toString();
      myBootstraps = new HashSet<InetSocketAddress>();
      myBootstraps.add(bootAddr);
      nodes = new HashSet<NodeHandle>();
    }
    
    /**
     * @param handle
     * @return
     */
    public boolean contains(NodeHandle handle) {
      return nodes.contains(handle);
    }

    /**
     * @return
     */
    public String getName() {
      return name;
    }

    /**
     * @param handle
     * @param e
     */
    public void addFailure(SocketNodeHandle handle, Exception e) {
//      System.err.println("Failure:"+handle);
//      e.printStackTrace(System.err);
    }

    public synchronized void addNode(SocketNodeHandle snh) {
      
      InetSocketAddress newAddr = snh.getInetSocketAddress();
      synchronized(unmatchedBootstraps) {
        if (unmatchedBootstraps.contains(newAddr)) {
          unmatchedBootstraps.remove(newAddr);
        }
      }      

      if (bootstraps.contains(newAddr)) {
        myBootstraps.add(newAddr); 
      }
      nodes.add(snh);
    }
    
    public String toString() {
      String s = nodes.size()+":"+myBootstraps.size()+": boots:";
      synchronized(myBootstraps) {
        Iterator<InetSocketAddress> i = myBootstraps.iterator();
        s+=i.next();
        while(i.hasNext()) {
          s+=","+i.next();
        }
      }
      s+=" non-boots:";
      synchronized(nodes) {
        Iterator<NodeHandle> i = nodes.iterator();
        while(i.hasNext()) {
          NodeHandle nxt = i.next();
          if (!bootstraps.contains(nxt))
            s+=","+nxt;
        }
      }      
      return s;
    }

    public int compareTo(Ring arg0) {
      Ring that = (Ring)arg0;
      return this.size() - that.size();
//      return that.size() - this.size();
    }
    
    public int size() {
      return nodes.size(); 
    }
  }
  
  public static void main(String[] args) throws Exception {
    new PartitionChecker(args[0]);
    System.exit(0);
  }
}
