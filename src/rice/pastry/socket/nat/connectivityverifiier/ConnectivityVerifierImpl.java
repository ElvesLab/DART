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
package rice.pastry.socket.nat.connectivityverifiier;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.networkinfo.ConnectivityResult;
import org.mpisws.p2p.transport.networkinfo.InetSocketAddressLookup;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.util.AttachableCancellable;
import rice.pastry.Id;
import rice.pastry.PastryNode;
import rice.pastry.socket.SocketPastryNodeFactory;

public class ConnectivityVerifierImpl implements ConnectivityVerifier {
  SocketPastryNodeFactory spnf;
  Environment environment;
  Logger logger;
  
  public ConnectivityVerifierImpl(SocketPastryNodeFactory spnf) {
    this.spnf = spnf;
    this.environment = spnf.getEnvironment();
    this.logger = environment.getLogManager().getLogger(ConnectivityVerifierImpl.class, null);
  }
  
  /**
   * Get the address from the transport layer.  Used by the two public methods.  Does the execution on the selector, 
   * calls back on the continuation on the selector.
   */
  protected Cancellable getInetSocketAddressLookup(final InetSocketAddress bindAddress, final Continuation<InetSocketAddressLookup, IOException> deliverResultToMe) {
    final AttachableCancellable ret = new AttachableCancellable();
    
    Runnable r = new Runnable() {
      public void run() {
        if (ret.isCancelled()) return;
        PastryNode pn = new PastryNode(Id.build(), spnf.getEnvironment());
    
        try {
          InetSocketAddressLookup lookup = (InetSocketAddressLookup)spnf.getBottomLayers(pn, new MultiInetSocketAddress(bindAddress));      
          deliverResultToMe.receiveResult(lookup);
        } catch (IOException ioe) {
          deliverResultToMe.receiveException(ioe);
        }
      }
    };
    // only do this on the selector thread, b/c binding on the non-selector makes java cranky on some versions of linux
    if (spnf.getEnvironment().getSelectorManager().isSelectorThread()) {
      r.run();
    } else {
      spnf.getEnvironment().getSelectorManager().invoke(r);
    }
    return ret;    
  }
  
  /**
   * Call this to find some nodes outside your firewall.
   */
  public Cancellable findExternalNodes(final InetSocketAddress local,
      final Collection<InetSocketAddress> probeAddresses,
      final Continuation<Collection<InetSocketAddress>, IOException> deliverResultToMe) {
    if (logger.level <= Logger.FINER) logger.log("findExternalAddress("+local+","+probeAddresses+","+deliverResultToMe+")");
    // TODO: make sure the addresses are Internet routable?
    // TODO: Timeout (can be in parallel, so timeout ~every second, and try the next one, then cancel everything if one comes through)

    final ArrayList<InetSocketAddress> probeList = new ArrayList<InetSocketAddress>(probeAddresses);
    final AttachableCancellable ret = new AttachableCancellable();

    // getInetSocketAddressLookup verifyies that we are on the selector
    ret.attach(getInetSocketAddressLookup(local, new Continuation<InetSocketAddressLookup, IOException>() {
    
      public void receiveResult(InetSocketAddressLookup lookup) {
        // we're on the selector now, and we have our TL
        findExternalNodesHelper(lookup, ret, local, probeList, deliverResultToMe);
      }
      
      public void receiveException(IOException exception) {
        // we couldn't even get a transport layer, DOA        
        if (logger.level <= Logger.INFO) logger.log("findExternalAddress("+local+","+probeAddresses+","+deliverResultToMe+").receiveException("+exception+")");
        deliverResultToMe.receiveException(exception);
      }      
    }));
    
    return ret;
  }
  
  /**
   * Called recursively.
   * 
   * @param lookup
   * @param ret
   * @param local
   * @param probeList
   * @param deliverResultToMe
   */
  public void findExternalNodesHelper(final InetSocketAddressLookup lookup, final AttachableCancellable ret, final InetSocketAddress local,
      final List<InetSocketAddress> probeList,
      final Continuation<Collection<InetSocketAddress>, IOException> deliverResultToMe) {
    if (logger.level <= Logger.FINER) logger.log("findExternalNodesHelper("+lookup+","+local+","+probeList+")");
    // we're on the selector now, and we have our TL        
    // pull a random node off the list, and try it, we do this so the recursion works
    InetSocketAddress target = probeList.remove(spnf.getEnvironment().getRandomSource().nextInt(probeList.size())); 
    
    ret.attach(lookup.getExternalNodes(target, new Continuation<Collection<InetSocketAddress>, IOException>() {          
      public void receiveResult(final Collection<InetSocketAddress> result) {              
        if (logger.level <= Logger.INFO) logger.log("findExternalNodesHelper("+lookup+","+local+","+probeList+").success:"+result);

        // success!
        ret.cancel(); // kill any recursive tries
        lookup.destroy();
        
        // lookup.destroy() uses an invoke
        environment.getSelectorManager().invoke(new Runnable() {        
          public void run() {
            //if (logger.level <= Logger.INFO) 
//              logger.log("findExternalNodesHelper("+lookup+","+local+","+probeList+").success:"+result);
            deliverResultToMe.receiveResult(result);
          }        
        });
      }
    
      public void receiveException(final IOException exception) {
        if (logger.level <= Logger.INFO) logger.log("findExternalNodesHelper("+lookup+","+local+","+probeList+").receiveException("+exception+")");
        
        // see if we can try anyone else
        if (probeList.isEmpty()) {
          lookup.destroy();

          // lookup.destroy() uses an invoke
          environment.getSelectorManager().invoke(new Runnable() {        
            public void run() {
              deliverResultToMe.receiveException(exception);
            }        
          });
          return;
        }
        
        // retry (recursive)
        findExternalNodesHelper(lookup, ret, local, probeList, deliverResultToMe);
      }      
    }, null));    
  }

  /**
   * Call this to determine your external address.
   */
  public Cancellable findExternalAddress(final InetSocketAddress local,
      final Collection<InetSocketAddress> probeAddresses,
      final Continuation<InetAddress, IOException> deliverResultToMe) {
    if (logger.level <= Logger.FINER) logger.log("findExternalAddress("+local+","+probeAddresses+","+deliverResultToMe+")");
    // TODO: make sure the addresses are Internet routable?
    // TODO: Timeout (can be in parallel, so timeout ~every second, and try the next one, then cancel everything if one comes through)

    final ArrayList<InetSocketAddress> probeList = new ArrayList<InetSocketAddress>(probeAddresses);
    final AttachableCancellable ret = new AttachableCancellable();

    // getInetSocketAddressLookup verifyies that we are on the selector
    ret.attach(getInetSocketAddressLookup(local, new Continuation<InetSocketAddressLookup, IOException>() {
    
      public void receiveResult(InetSocketAddressLookup lookup) {
        // we're on the selector now, and we have our TL
        findExternalAddressHelper(lookup, ret, local, probeList, deliverResultToMe);
      }
      
      public void receiveException(IOException exception) {
        // we couldn't even get a transport layer, DOA        
        if (logger.level <= Logger.INFO) logger.log("findExternalAddress("+local+","+probeAddresses+","+deliverResultToMe+").receiveException("+exception+")");
        deliverResultToMe.receiveException(exception);
      }      
    }));
    
    return ret;
  }
  
  /**
   * Called recursively.
   * 
   * @param lookup
   * @param ret
   * @param local
   * @param probeList
   * @param deliverResultToMe
   */
  public void findExternalAddressHelper(final InetSocketAddressLookup lookup, final AttachableCancellable ret, final InetSocketAddress local,
      final List<InetSocketAddress> probeList,
      final Continuation<InetAddress, IOException> deliverResultToMe) {
    if (logger.level <= Logger.FINER) logger.log("findExternalAddressHelper("+lookup+","+local+","+probeList+")");
    // we're on the selector now, and we have our TL        
    // pull a random node off the list, and try it, we do this so the recursion works
    InetSocketAddress target = probeList.remove(spnf.getEnvironment().getRandomSource().nextInt(probeList.size())); 
    
    ret.attach(lookup.getMyInetAddress(target, new Continuation<InetSocketAddress, IOException>() {          
      public void receiveResult(final InetSocketAddress result) {              
        if (logger.level <= Logger.INFO) logger.log("findExternalAddressHelper("+lookup+","+local+","+probeList+").success:"+result);

        // success!
        ret.cancel(); // kill any recursive tries
        lookup.destroy();
        
        // lookup.destroy() uses an invoke
        environment.getSelectorManager().invoke(new Runnable() {        
          public void run() {
            deliverResultToMe.receiveResult(result.getAddress());
          }        
        });
      }
    
      public void receiveException(final IOException exception) {
        if (logger.level <= Logger.INFO) logger.log("findExternalAddressHelper("+lookup+","+local+","+probeList+").receiveException("+exception+")");
        
        // see if we can try anyone else
        if (probeList.isEmpty()) {
          lookup.destroy();

          // lookup.destroy() uses an invoke
          environment.getSelectorManager().invoke(new Runnable() {        
            public void run() {
              deliverResultToMe.receiveException(exception);
            }        
          });
          return;
        }
        
        // retry (recursive)
        findExternalAddressHelper(lookup, ret, local, probeList, deliverResultToMe);
      }      
    }, null));    
  }


  /**
   * Call this to determine if your connectivity is good.
   */
  public Cancellable verifyConnectivity(final MultiInetSocketAddress local,
      final Collection<InetSocketAddress> probeAddresses,
      final ConnectivityResult deliverResultToMe) {
    final ArrayList<InetSocketAddress> probeList = new ArrayList<InetSocketAddress>(probeAddresses);
//    logger.logException("verifyConnectivity("+local+","+probeAddresses+")", new Exception("Stack Trace"));
//    logger.log("verifyConnectivity("+local+","+probeAddresses+")");
    if (logger.level <= Logger.FINER) logger.log("verifyConnectivity("+local+","+probeAddresses+")");

    // don't probe self
    for (int ctr = 0; ctr < local.getNumAddresses(); ctr++) {
      probeList.remove(local.getAddress(ctr));
    }
    
    if (probeList.isEmpty()) {
      if (logger.level <= Logger.FINER) logger.log("verifyConnectivity("+local+","+probeAddresses+"). no valid addresses");
      deliverResultToMe.receiveException(new IllegalStateException("No valid probe addresses. "+probeAddresses+" local:"+local));
      return null;
    }
    
    final AttachableCancellable ret = new AttachableCancellable();

    // getInetSocketAddressLookup verifies that we are on the selector
    ret.attach(getInetSocketAddressLookup(local.getInnermostAddress(), new Continuation<InetSocketAddressLookup, IOException>() {
    
      public void receiveResult(final InetSocketAddressLookup lookup) {
        // we're on the selector now, and we have our TL
        ret.attach(new Cancellable() {
        
          public boolean cancel() {
            lookup.destroy();            
            return true;
          }
        
        });
        

        verifyConnectivityHelper(lookup, ret, local, probeList, new ConnectivityResult() {
          boolean udpSuccess = false;
          boolean tcpSuccess = false;
          
          public void udpSuccess(final InetSocketAddress from, final Map<String, Object> options) {
            udpSuccess = true;
            if (tcpSuccess) {
              ret.cancel();
              lookup.destroy();
            }
            if (logger.level <= Logger.INFO) logger.log("verifyConnectivity("+local+","+probeAddresses+"). udpSuccess("+from+")");
            
            // lookup.destroy() uses an invoke
            environment.getSelectorManager().invoke(new Runnable() {        
              public void run() {
                deliverResultToMe.udpSuccess(from, options);
              }        
            });
          }
        
          public void tcpSuccess(final InetSocketAddress from, final Map<String, Object> options) {
            tcpSuccess = true;
            if (udpSuccess) {
              ret.cancel();
              lookup.destroy();
            }
            if (logger.level <= Logger.INFO) logger.log("verifyConnectivity("+local+","+probeAddresses+"). tcpSuccess("+from+")");
            
            // lookup.destroy() uses an invoke
            environment.getSelectorManager().invoke(new Runnable() {        
              public void run() {
                deliverResultToMe.tcpSuccess(from, options);
              }        
            });

          }
                
          public void receiveException(final Exception exception) {
            // see if we can try anyone else
            if (probeList.isEmpty()) {
              lookup.destroy();
              if (logger.level <= Logger.INFO) logger.log("verifyConnectivity("+local+","+probeAddresses+"). failure no more addresses "+exception);
              
              // lookup.destroy() uses an invoke
              environment.getSelectorManager().invoke(new Runnable() {        
                public void run() {
                  deliverResultToMe.receiveException(exception);
                }        
              });

              return;
            }
            
            // retry (recursive)
            verifyConnectivityHelper(lookup, ret, local, probeList, deliverResultToMe);
          }      
        });
      }
      
      public void receiveException(IOException exception) {
        // we couldn't even get a transport layer, DOA        
        if (logger.level <= Logger.INFO) logger.log("verifyConnectivity("+local+","+probeAddresses+"). couldn't get tl "+exception);
        deliverResultToMe.receiveException(exception);
      }      
    }));
    
    return ret;
  }

  public void verifyConnectivityHelper(final InetSocketAddressLookup lookup, final AttachableCancellable ret, 
      final MultiInetSocketAddress local,
      final List<InetSocketAddress> probeList,
      final ConnectivityResult deliverResultToMe) {
      if (logger.level <= Logger.INFO) logger.log("verifyConnectivityHelper("+local+","+probeList+")");
      
      // we're on the selector now, and we have our TL        
      // pull a random node off the list, and try it, we do this so the recursion works
      InetSocketAddress target = probeList.remove(spnf.getEnvironment().getRandomSource().nextInt(probeList.size())); 
      
      ret.attach(lookup.verifyConnectivity(local, target, deliverResultToMe, null));
  }

}
