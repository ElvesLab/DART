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

package rice.pastry.dist;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.UnresolvedAddressException;
import java.util.*;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.CancellableTask;
import rice.pastry.*;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.socket.nat.rendezvous.RendezvousSocketPastryNodeFactory;

/**
 * An abstraction of the nodeId factory for distributed nodes. In order to
 * obtain a nodeId factory, a client should use the getFactory method, passing
 * in either PROTOCOL_RMI or PROTOCOL_WIRE as the protocol, and the port number
 * the factory should use. In the wire protocol, the port number is the starting
 * port number that the nodes are constructed on, and in the rmi protocol, the
 * port number is the location of the local RMI registry.
 *
 * @version $Id: DistPastryNodeFactory.java,v 1.8 2003/12/22 03:24:46 amislove
 *      Exp $
 * @author Alan Mislove
 */
public class DistPastryNodeFactory {

//  // choices of protocols
//  /**
//   * DESCRIBE THE FIELD
//   */
  public static int PROTOCOL_SOCKET = 2;
  public static int PROTOCOL_RENDEZVOUS = 3;

  public static int PROTOCOL_DEFAULT = PROTOCOL_SOCKET;
//  
//  /**
//   * Constructor. Protected - one should use the getFactory method.
//   */
//  protected DistPastryNodeFactory(Environment env) {
//    super(env);
//  }
//
//  /**
//   * Method which a client should use in order to get a bootstrap node from the
//   * factory. In the wire protocol, this method will generate a node handle
//   * corresponding to the pastry node at location address. In the rmi protocol,
//   * this method will generate a node handle for the pastry node bound to
//   * address.
//   *
//   * @param address The address of the remote node.
//   * @return The NodeHandle value
//   */
//  public final NodeHandle getNodeHandle(InetSocketAddress address) {
//    return generateNodeHandle(address, 0);
//  }
//  
//  public final NodeHandle getNodeHandle(InetSocketAddress address, int timeout) {
//    return generateNodeHandle(address, timeout);
//  }
//  
//  public final CancellableTask getNodeHandle(InetSocketAddress address, Continuation c) {
//    return generateNodeHandle(address, c);
//  }
//  
//  /**
//   * Method which a client should use in order to get a bootstrap node from the
//   * factory. In the wire protocol, this method will generate a node handle
//   * corresponding to the pastry node at location address. In the rmi protocol,
//   * this method will generate a node handle for the pastry node bound to
//   * address.
//   *
//   * @param address The address of the remote node.
//   * @return The NodeHandle value
//   */
//  public final NodeHandle getNodeHandle(InetSocketAddress[] addresses) {
//    return getNodeHandle(addresses, 0);
//  }
//  public final NodeHandle getNodeHandle(InetSocketAddress[] addresses, int timeoutMillis) {
//    // first, randomize the addresses
//    Random r = new Random();
//    for (int i=0; i<addresses.length; i++) {
//      int j = r.nextInt(addresses.length);
//      InetSocketAddress tmp = addresses[j];
//      addresses[j] = addresses[i];
//      addresses[i] = tmp;
//    } 
//    
//    // then boot
//    for (int i=0; i<addresses.length; i++) {
//      NodeHandle result = getNodeHandle(addresses[i], timeoutMillis);
//      if (result != null)
//        return result;
//      // try re-resolving the address before giving up
//      try {
//        InetSocketAddress newAddress = new InetSocketAddress(addresses[i].getAddress().getHostName(), addresses[i].getPort());
//        if (!newAddress.getAddress().equals(addresses[i].getAddress())) {
//          // re-resolving actually gave us a  new address
//          result = getNodeHandle(newAddress, timeoutMillis);
//        }
//      } catch (UnresolvedAddressException uae) {
//        if (logger.level <= Logger.INFO) logger.log("getNodeHandle: Could not resolve hostname "+addresses[i]);
//      }
//      if (result != null)
//        return result;
//    }
//    
//    return null;
//  }
//
//  /**
//   * Implements non-blocking multiple address selection, in parallel.  
//   *  
//   * @author Jeff Hoye
//   */
//  class GNHContinuation implements Continuation<Object, Exception>, CancellableTask {
//    /**
//     * Points to next address to try
//     */
//    int index;
//    int numInParallel;
//    int outstandingRequests;
//    InetSocketAddress[] addresses;
//    Continuation<Object, Exception> subContinuation;
//    
//    /**
//     * Stored list of sub CancellableTasks to make the whole parallel process cancellable.
//     */
//    ArrayList<Cancellable> outstandingTasks = new ArrayList<Cancellable>();
//    /**
//     * Set to true when complete.
//     */
//    boolean done = false;
//    
//    public GNHContinuation(InetSocketAddress[] addresses, Continuation subContinuation, int numInParallel) {
//      this.addresses = addresses;
//      this.subContinuation = subContinuation;
//      // must request at least one in parallel (this is serial)
//      if (numInParallel < 1) numInParallel = 1;
//      this.numInParallel = numInParallel;      
//      index = 0;
//    }
//    
//    
//    public synchronized void receiveResult(Object result) {
//      if (done) return;
//      outstandingRequests--;
//      if (result != null) {
//        cancel();
//        subContinuation.receiveResult(result);
//        return;
//      } else {
//        tryNext();
//      }
//    }
//
//    public synchronized void receiveException(Exception result) {
//      if (done) return;
//      if (logger.level <= Logger.WARNING) logger.logException(
//          "Received exception while booting, trying next bootstap address",result);
//      outstandingRequests--;
//      tryNext();      
//    }
//    
//    private synchronized void tryNext() {
//      if (done) return;
//      while(outstandingRequests < numInParallel &&
//          index < addresses.length) {
//        outstandingRequests++; 
//        index++; // do this in case call comes back on same thread, in same stack
//        outstandingTasks.add(getNodeHandle(addresses[index-1], this));
//      }
//      if (outstandingRequests == 0) {
//        subContinuation.receiveResult(null);
//        // cant be any tasks to cancel at this point
//        done = true;
//      }
//    }
//
//
//    public void run() {    }
//
//
//    public synchronized boolean cancel() {
//      if (done) return false;
//      Iterator i = outstandingTasks.iterator();
//      while(i.hasNext()) {
//        CancellableTask ct = (CancellableTask)i.next(); 
//        ct.cancel();
//      }
//      done = true;
//      return true;
//    }
//
//
//    public long scheduledExecutionTime() {
//      return 0;
//    }    
//  }
//  
//  public final CancellableTask getNodeHandle(InetSocketAddress[] addresses, Continuation c) {
//    // first, randomize the addresses
//    Random r = new Random();
//    for (int i=0; i<addresses.length; i++) {
//      int j = r.nextInt(addresses.length);
//      InetSocketAddress tmp = addresses[j];
//      addresses[j] = addresses[i];
//      addresses[i] = tmp;
//    }     
//
//    GNHContinuation gnh = new GNHContinuation(addresses,c,environment.getParameters().getInt("pastry_factory_bootsInParallel"));
//    gnh.tryNext();
//    return gnh;
//  }
//
//  /**
//   * Method which all subclasses should implement allowing the client to
//   * generate a node handle given the address of a node. This is designed to
//   * allow the client to get their hands on a bootstrap node during the
//   * initialization phase of the client application.
//   *
//   * @param address DESCRIBE THE PARAMETER
//   * @param timeout maximum time in milliseconds to return the result
//   * @return DESCRIBE THE RETURN VALUE
//   */
//  public abstract NodeHandle generateNodeHandle(InetSocketAddress address, int timeout);
//  public abstract CancellableTask generateNodeHandle(InetSocketAddress address, Continuation c);
//  
//  /**
//   * Generates a new pastry node with a random NodeId using the bootstrap
//   * bootstrap.
//   *
//   * @param bootstrap Node handle to bootstrap from.
//   * @return DESCRIBE THE RETURN VALUE
//   */
//  public abstract PastryNode newNode(NodeHandle bootstrap);
//
//  /**
//   * Generates a new pastry node with the specified NodeId using the bootstrap
//   * bootstrap.
//   *
//   * @param bootstrap Node handle to bootstrap from.
//   * @param nodeId DESCRIBE THE PARAMETER
//   * @return DESCRIBE THE RETURN VALUE
//   */
//  public abstract PastryNode newNode(NodeHandle bootstrap, Id nodeId);
//  
//  /**
//   * Generates a new pastry node with the specified NodeId using the bootstrap
//   * bootstrap.
//   *
//   * @param bootstrap Node handle to bootstrap from.
//   * @param nodeId DESCRIBE THE PARAMETER
//   * @return DESCRIBE THE RETURN VALUE
//   */
//  public abstract PastryNode newNode(NodeHandle bootstrap, Id nodeId, InetSocketAddress proxy);
//  
//  /** 
//   * Generates a new pastry node with the specified NodeId using the bootstrap
//   * bootstrap.
//   *
//   * @param bootstrap Node handle to bootstrap from.
//   * @param nodeId DESCRIBE THE PARAMETER
//   * @return DESCRIBE THE RETURN VALUE
//   */
//  public abstract PastryNode newNode(NodeHandle bootstrap, InetSocketAddress proxy);

  /**
   * Static method which is designed to be used by clients needing a distrubuted
   * pastry node factory. The protocol should be one of PROTOCOL_RMI or
   * PROTOCOL_WIRE. The port is protocol-dependent, and is the port number of
   * the RMI registry if using RMI, or is the starting port number the nodes
   * should be created on if using wire.
   *
   * @param protocol The protocol to use (PROTOCOL_RMI or PROTOCOL_WIRE)
   * @param port The RMI registry port if RMI, or the starting port if wire.
   * @param nf DESCRIBE THE PARAMETER
   * @return A DistPastryNodeFactory using the given protocol and port.
   * @throws IllegalArgumentException If protocol is an unsupported port.
   */
  public static SocketPastryNodeFactory getFactory(NodeIdFactory nf, int protocol, int port, Environment env) throws IOException {
    if (protocol == PROTOCOL_SOCKET) {
      return new SocketPastryNodeFactory(nf, port, env);
    }
    if (protocol == PROTOCOL_RENDEZVOUS) {
      return new RendezvousSocketPastryNodeFactory(nf, port, env, false);
    }
    
    throw new IllegalArgumentException("Unsupported Protocol " + protocol);
  }
}

