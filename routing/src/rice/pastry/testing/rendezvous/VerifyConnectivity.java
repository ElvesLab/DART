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
package rice.pastry.testing.rendezvous;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.networkinfo.ConnectivityResult;

import rice.Continuation;
import rice.environment.Environment;
import rice.pastry.NodeIdFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.socket.nat.connectivityverifiier.ConnectivityVerifier;
import rice.pastry.socket.nat.connectivityverifiier.ConnectivityVerifierImpl;
import rice.pastry.standard.RandomNodeIdFactory;

public class VerifyConnectivity {
  
  /**
   * @param args bindport bootstrap bootstrapport externalport
   */
  public static void main(String[] args) throws IOException {    
    // the port to use locally
    int bindport = Integer.parseInt(args[0]);
    final int externalPort = Integer.parseInt(args[3]);
    
    // build the bootaddress from the command line args
    InetAddress bootaddr = InetAddress.getByName(args[1]);
    int bootport = Integer.parseInt(args[2]);
    final InetSocketAddress bootaddress = new InetSocketAddress(bootaddr,bootport);

    final Environment env = new Environment();
    
    // Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);
    
    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    final SocketPastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory, bindport, env);
    
    final ConnectivityVerifier verifier = new ConnectivityVerifierImpl(factory);
    verifier.findExternalAddress(factory.getNextInetSocketAddress(), Collections.singleton(bootaddress), new Continuation<InetAddress, IOException>() {
      
      public void receiveResult(final InetAddress result) {
        System.out.println("My external address is "+result);

        // for some reason, can't do this immeadiately... don't know why yet
        Runnable r = new Runnable() {        
          public void run() {        
            InetSocketAddress[] addrs = new InetSocketAddress[2];
            addrs[0] = new InetSocketAddress(result, externalPort);
            addrs[1] = factory.getNextInetSocketAddress();
            
            MultiInetSocketAddress local = new MultiInetSocketAddress(addrs);
            
            verifier.verifyConnectivity(local, Collections.singleton(bootaddress), new ConnectivityResult() {
            
              public void udpSuccess(InetSocketAddress from, Map<String, Object> options) {
                System.out.println("UDP works. "+from);
              }
            
              public void tcpSuccess(InetSocketAddress from, Map<String, Object> options) {
                System.out.println("TCP works. "+from);
              }
            
              public void receiveException(Exception exception) {
                exception.printStackTrace();
              }    
            });        
          }
          
        }; 
        env.getSelectorManager().invoke(r);        
      }
    
      public void receiveException(IOException exception) {
        exception.printStackTrace();
      }    
    });

  }

}
