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
package rice.pastry.socket.nat.rendezvous;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.commonapi.CommonAPITransportLayerImpl;
import org.mpisws.p2p.transport.identity.IdentityImpl;
import org.mpisws.p2p.transport.identity.IdentitySerializer;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.liveness.Pinger;
import org.mpisws.p2p.transport.multiaddress.AddressStrategy;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.multiaddress.SimpleAddressStrategy;
import org.mpisws.p2p.transport.nat.FirewallTLImpl;
import org.mpisws.p2p.transport.priority.PriorityTransportLayer;
import org.mpisws.p2p.transport.proximity.ProximityProvider;
import org.mpisws.p2p.transport.rendezvous.ContactDeserializer;
import org.mpisws.p2p.transport.rendezvous.ContactDirectStrategy;
import org.mpisws.p2p.transport.rendezvous.PilotFinder;
import org.mpisws.p2p.transport.rendezvous.PilotManager;
import org.mpisws.p2p.transport.rendezvous.RendezvousContact;
import org.mpisws.p2p.transport.rendezvous.RendezvousGenerationStrategy;
import org.mpisws.p2p.transport.rendezvous.RendezvousStrategy;
import org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl;
import org.mpisws.p2p.transport.rendezvous.ResponseStrategy;
import org.mpisws.p2p.transport.rendezvous.TimeoutResponseStrategy;
import org.mpisws.p2p.transport.sourceroute.SourceRoute;
import org.mpisws.p2p.transport.sourceroute.factory.MultiAddressSourceRouteFactory;
import org.mpisws.p2p.transport.sourceroute.manager.simple.NextHopStrategy;
import org.mpisws.p2p.transport.util.OptionsFactory;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.environment.random.RandomSource;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.p2p.util.tuples.MutableTuple;
import rice.p2p.util.tuples.Tuple;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.ReadyStrategy;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.join.JoinProtocol;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.LeafSetProtocol;
import rice.pastry.pns.PNSApplication;
import rice.pastry.routing.RouterStrategy;
import rice.pastry.routing.RoutingTable;
import rice.pastry.socket.SocketNodeHandle;
import rice.pastry.socket.SocketNodeHandleFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.socket.TransportLayerNodeHandle;
import rice.pastry.socket.SocketPastryNodeFactory.TLBootstrapper;
import rice.pastry.socket.nat.NATHandler;
import rice.pastry.standard.ConsistentJoinProtocol;
import rice.pastry.standard.PeriodicLeafSetProtocol;
import rice.pastry.standard.ProximityNeighborSelector;
import rice.pastry.standard.StandardRouter;
import rice.pastry.transport.LeafSetNHStrategy;
import rice.pastry.transport.NodeHandleAdapter;


/**
 * This class assembles the rendezvous layer with the rendezvous app.
 * 
 * Need to think about where this best goes, but for now, we'll put it just above the magic number layer.
 * 
 * @author Jeff Hoye
 *
 */
public class RendezvousSocketPastryNodeFactory extends SocketPastryNodeFactory {
  /**
   * Maps to a byte contactState
   */
  protected String CONTACT_STATE = "RendezvousSocketPastryNodeFactory.CONTACT_STATE";
  
  protected RandomSource random;

  /**
   * maps to a RendezvousStrategy<RendezvousSocketNodeHandle>
   */
  public static final String RENDEZVOUS_STRATEGY = "RendezvousSocketPastryNodeFactory.RENDEZVOUS_STRATEGY";
  
  /**
   * maps to a RendezvousTransportLayerImpl<InetSocketAddress, RendezvousSocketNodeHandle>
   */
  public static final String RENDEZVOUS_TL = "RendezvousSocketPastryNodeFactory.RENDEZVOUS_TL";

  /**
   * maps to a boolean, if true then simulates a firewall
   */
  public static final String SIMULATE_FIREWALL = "rendezvous_simulate_firewall";
  
  /**
   * getVars() maps to a ContactDirectStrategy<RendezvousSocketNodeHandle>
   */
  public static final String RENDEZVOUS_CONTACT_DIRECT_STRATEGY = "RendezvousSocketPastryNodeFactory.ContactDirectStrategy";
  
  /**
   * The local node's contact state.  
   * 
   * TODO: Configure this
   */
  byte localContactState = RendezvousSocketNodeHandle.CONTACT_DIRECT;
  
  public RendezvousSocketPastryNodeFactory(NodeIdFactory nf, InetAddress bindAddress, int startPort, Environment env, boolean firewalled) throws IOException {
    super(nf, bindAddress, startPort, env);
    init(firewalled);
  }

  public RendezvousSocketPastryNodeFactory(NodeIdFactory nf, int startPort, Environment env, boolean firewalled) throws IOException {
    super(nf, startPort, env);
    init(firewalled);
  }
  
  private void init(boolean firewalled) {
    random = environment.getRandomSource();
    if (firewalled) setContactState(RendezvousSocketNodeHandle.CONTACT_FIREWALLED);
  }
  
  public void setContactState(byte contactState) {
    this.localContactState = contactState;
  }

  /**
   * Can override the contactState on a per-node basis
   * 
   * @param nodeId
   * @param proxyAddress
   * @param deliverResultToMe
   * @param initialVars
   * @param firewalled
   */
  protected void newNodeSelector(Id nodeId,
      MultiInetSocketAddress proxyAddress,
      Continuation<PastryNode, IOException> deliverResultToMe,
      Map<String, Object> initialVars, boolean firewalled) {
    
    byte contactState = localContactState;
    if (firewalled) {
      contactState = RendezvousSocketNodeHandle.CONTACT_FIREWALLED;
    } else {
      contactState = RendezvousSocketNodeHandle.CONTACT_DIRECT;      
    }
    newNodeSelector(nodeId, proxyAddress, deliverResultToMe, initialVars, contactState);
  }

  
  
  /**
   * Can override the contactState on a per-node basis
   * 
   * @param nodeId
   * @param proxyAddress
   * @param deliverResultToMe
   * @param initialVars
   * @param contactState
   */
  protected void newNodeSelector(Id nodeId,
      MultiInetSocketAddress proxyAddress,
      Continuation<PastryNode, IOException> deliverResultToMe,
      Map<String, Object> initialVars, byte contactState) {
    initialVars = OptionsFactory.addOption(initialVars, CONTACT_STATE, contactState);
    super.newNodeSelector(nodeId, proxyAddress, deliverResultToMe, initialVars);
  }
  
  @Override
  protected JoinProtocol getJoinProtocol(PastryNode pn, LeafSet leafSet,
      RoutingTable routeTable, ReadyStrategy lsProtocol) {
    RendezvousJoinProtocol jProtocol = new RendezvousJoinProtocol(pn,
        pn.getLocalHandle(), routeTable, leafSet, lsProtocol, (PilotManager<RendezvousSocketNodeHandle>)pn.getVars().get(RENDEZVOUS_TL));
    jProtocol.register();
    return jProtocol;    
  }


  @Override
  protected TransportLayer<InetSocketAddress, ByteBuffer> getIpServiceTransportLayer(TransportLayer<InetSocketAddress, ByteBuffer> wtl, PastryNode pn) throws IOException {
    TransportLayer<InetSocketAddress, ByteBuffer> mtl = super.getIpServiceTransportLayer(wtl, pn);
    
    if (pn.getLocalHandle() == null) return mtl;
    
    return getRendezvousTransportLayer(mtl, pn);
  }

  @Override
  protected IdentitySerializer<TransportLayerNodeHandle<MultiInetSocketAddress>, MultiInetSocketAddress, SourceRoute<MultiInetSocketAddress>> getIdentiySerializer(PastryNode pn, SocketNodeHandleFactory handleFactory) {
    return new RendezvousSPNFIdentitySerializer(pn, handleFactory);
  }

  protected TransportLayer<InetSocketAddress, ByteBuffer> getRendezvousTransportLayer(
      TransportLayer<InetSocketAddress, ByteBuffer> mtl, PastryNode pn) {
    
    RendezvousTransportLayerImpl<InetSocketAddress, RendezvousSocketNodeHandle> ret = 
      new RendezvousTransportLayerImpl<InetSocketAddress, RendezvousSocketNodeHandle>(
        mtl, 
        IdentityImpl.NODE_HANDLE_FROM_INDEX,
//        CommonAPITransportLayerImpl.DESTINATION_IDENTITY, 
        (RendezvousSocketNodeHandle)pn.getLocalHandle(), 
        getContactDeserializer(pn),
        getRendezvousGenerator(pn), 
        getPilotFinder(pn),
        getRendezvousStrategyHelper(pn),
        getResponseStrategy(pn),
        getContactDirectStrategy(pn),
        pn.getEnvironment());
    
    pn.getVars().put(RENDEZVOUS_TL, ret);
    ((RendezvousStrategy<RendezvousSocketNodeHandle>)pn.getVars().get(RENDEZVOUS_STRATEGY)).setTransportLayer(ret);
    
    generatePilotStrategy(pn, ret);
    return ret;
  }

  @Override
  protected NextHopStrategy<MultiInetSocketAddress> getNextHopStrategy(      
      TransportLayer<SourceRoute<MultiInetSocketAddress>, ByteBuffer> ltl, 
      LivenessProvider<SourceRoute<MultiInetSocketAddress>> livenessProvider, 
      Pinger<SourceRoute<MultiInetSocketAddress>> pinger, 
      PastryNode pn, 
      MultiInetSocketAddress proxyAddress, 
      MultiAddressSourceRouteFactory esrFactory) throws IOException {

    return new RendezvousLeafSetNHStrategy(pn.getLeafSet());    
  }
  
  protected ResponseStrategy<InetSocketAddress> getResponseStrategy(PastryNode pn) {
//  return new NeverResponseStrategy<InetSocketAddress>();
  return new TimeoutResponseStrategy<InetSocketAddress>(3000, pn.getEnvironment());
}

  protected ContactDirectStrategy<RendezvousSocketNodeHandle> getContactDirectStrategy(PastryNode pn) {
    AddressStrategy adrStrat = (AddressStrategy)pn.getVars().get(MULTI_ADDRESS_STRATEGY);    
    if (adrStrat == null) {
      adrStrat = new SimpleAddressStrategy();
    }
    
    ContactDirectStrategy<RendezvousSocketNodeHandle> ret = new RendezvousContactDirectStrategy((RendezvousSocketNodeHandle)pn.getLocalNodeHandle(), adrStrat, pn.getEnvironment());
    pn.getVars().put(RENDEZVOUS_CONTACT_DIRECT_STRATEGY, ret);
    
    return ret;
}

  protected PilotFinder<RendezvousSocketNodeHandle> getPilotFinder(PastryNode pn) {
    return new LeafSetPilotFinder(pn);
  }
  
  protected void generatePilotStrategy(PastryNode pn, RendezvousTransportLayerImpl<InetSocketAddress, RendezvousSocketNodeHandle> rendezvousLayer) {
    // only do this if firewalled
    RendezvousSocketNodeHandle handle = (RendezvousSocketNodeHandle)pn.getLocalHandle();
    if (handle != null && !handle.canContactDirect())
      new LeafSetPilotStrategy<RendezvousSocketNodeHandle>(pn.getLeafSet(),rendezvousLayer, pn.getEnvironment());    
  }

  protected ContactDeserializer<InetSocketAddress, RendezvousSocketNodeHandle> getContactDeserializer(final PastryNode pn) {
    return new ContactDeserializer<InetSocketAddress, RendezvousSocketNodeHandle>(){
    
      public Map<String, Object> getOptions(RendezvousSocketNodeHandle high) {
        return OptionsFactory.addOption(null, IdentityImpl.NODE_HANDLE_FROM_INDEX, high);
      }
    
      public RendezvousSocketNodeHandle deserialize(InputBuffer sib) throws IOException {
        return (RendezvousSocketNodeHandle)pn.readNodeHandle(sib);
      }
    
      public InetSocketAddress convert(RendezvousSocketNodeHandle high) {
        // TODO: is this the correct one?
        return high.eaddress.getAddress(0);
      }

      public void serialize(RendezvousSocketNodeHandle i, OutputBuffer buf) throws IOException {
        i.serialize(buf);
      }
      
      public ByteBuffer serialize(RendezvousSocketNodeHandle i)
          throws IOException {
        SimpleOutputBuffer sob = new SimpleOutputBuffer();
        serialize(i,sob);
        return sob.getByteBuffer();
      }
    
    };
  }

  protected RendezvousGenerationStrategy<RendezvousSocketNodeHandle> getRendezvousGenerator(PastryNode pn) {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  protected ProximityNeighborSelector getProximityNeighborSelector(PastryNode pn) {    
    if (environment.getParameters().getBoolean("transport_use_pns")) {
      RendezvousPNSApplication pns = new RendezvousPNSApplication(pn, (ContactDirectStrategy<RendezvousSocketNodeHandle>)pn.getVars().get(RENDEZVOUS_CONTACT_DIRECT_STRATEGY));
      pns.register();
      return pns;
    }
  
    // do nothing
    return new ProximityNeighborSelector(){    
      public Cancellable getNearHandles(Collection<NodeHandle> bootHandles, Continuation<Collection<NodeHandle>, Exception> deliverResultToMe) {
        deliverResultToMe.receiveResult(bootHandles);
        return null;
      }    
    };
  }


  /**
   * This is an annoying hack.  We can't register the RendezvousApp until registerApps(), but we need it here.
   * 
   * This table temporarily holds the rendezvousApps until they are needed, then it is deleted.
   */
//  Map<PastryNode, MutableTuple<RendezvousStrategy<RendezvousSocketNodeHandle>, RendezvousTransportLayerImpl<InetSocketAddress, RendezvousSocketNodeHandle>>> rendezvousApps = 
//    new HashMap<PastryNode, MutableTuple<RendezvousStrategy<RendezvousSocketNodeHandle>, RendezvousTransportLayerImpl<InetSocketAddress, RendezvousSocketNodeHandle>>>();
  protected RendezvousStrategy<RendezvousSocketNodeHandle> getRendezvousStrategyHelper(PastryNode pn) {
    RendezvousStrategy<RendezvousSocketNodeHandle>  app = getRendezvousStrategy(pn);
    pn.getVars().put(RENDEZVOUS_STRATEGY, app);
    return app;
  }

  protected RendezvousStrategy<RendezvousSocketNodeHandle> getRendezvousStrategy(PastryNode pn) {
    RendezvousApp app = new RendezvousApp(pn);
    return app;
  }

  @Override
  protected void registerApps(PastryNode pn, LeafSet leafSet, RoutingTable routeTable, NodeHandleAdapter nha, NodeHandleFactory handleFactory) {
    super.registerApps(pn, leafSet, routeTable, nha, handleFactory);
    RendezvousStrategy<RendezvousSocketNodeHandle> app = (RendezvousStrategy<RendezvousSocketNodeHandle>)pn.getVars().get(RENDEZVOUS_STRATEGY);
    if (app instanceof RendezvousApp) {
      ((RendezvousApp)app).register();
    }
  }
  
  @Override
  public NodeHandleFactory getNodeHandleFactory(PastryNode pn) {
    return new RendezvousSNHFactory(pn);
  }
  
  /**
   * Used with getWireTL to make sure to return the bootstrap as not firewalled.
   */
  boolean firstNode = true;
  
  @Override
  public NodeHandle getLocalHandle(PastryNode pn, NodeHandleFactory nhf) {
    byte contactState = localContactState;    
    
    if (pn.getVars().containsKey(CONTACT_STATE)) {
      contactState = (Byte)pn.getVars().get(CONTACT_STATE);
    }
    
    // this code is for testing
    Parameters p = environment.getParameters();    
    if (firstNode && p.getBoolean("rendezvous_test_makes_bootstrap")) {
      firstNode = false;
      // this just guards the next part
    } else if (p.getBoolean("rendezvous_test_firewall")) {
      if (random.nextFloat() <= p.getFloat("rendezvous_test_num_firewalled")) {
        pn.getVars().put(SIMULATE_FIREWALL, true);
        contactState = RendezvousSocketNodeHandle.CONTACT_FIREWALLED;
      }
    }
    
    RendezvousSNHFactory pnhf = (RendezvousSNHFactory)nhf;
    MultiInetSocketAddress proxyAddress = (MultiInetSocketAddress)pn.getVars().get(PROXY_ADDRESS);
    SocketNodeHandle ret = pnhf.getNodeHandle(proxyAddress, pn.getEnvironment().getTimeSource().currentTimeMillis(), pn.getNodeId(), contactState);
    
    // this code is for logging    
    if (logger.level <= Logger.FINE || (contactState != localContactState && logger.level <= Logger.INFO)) {
      switch(contactState) {
      case RendezvousSocketNodeHandle.CONTACT_DIRECT:
        logger.log(ret+" is not firewalled.");
        break;
      case RendezvousSocketNodeHandle.CONTACT_FIREWALLED:
        logger.log(ret+" is firewalled.");
        break;
      }
    }
    
    return ret;
  }

  /**
   * For testing, may return a FirewallTL impl for testing.
   */
  @Override
  protected TransportLayer<InetSocketAddress, ByteBuffer> getWireTransportLayer(InetSocketAddress innermostAddress, PastryNode pn) throws IOException {
    TransportLayer<InetSocketAddress, ByteBuffer> baseTl = super.getWireTransportLayer(innermostAddress, pn);
    Parameters p = pn.getEnvironment().getParameters();
    
    if ((pn.getVars().containsKey(SIMULATE_FIREWALL) && ((Boolean)pn.getVars().get(SIMULATE_FIREWALL)).booleanValue()) ||
        (p.contains(SIMULATE_FIREWALL) && p.getBoolean(SIMULATE_FIREWALL))) {
      return new FirewallTLImpl<InetSocketAddress, ByteBuffer>(baseTl,5000,pn.getEnvironment());
    }
    
    // do the normal thing
    return baseTl; 
  }
  
  @Override
  protected PriorityTransportLayer<MultiInetSocketAddress> getPriorityTransportLayer(
      TransportLayer<MultiInetSocketAddress, ByteBuffer> trans,
      LivenessProvider<MultiInetSocketAddress> liveness,
      ProximityProvider<MultiInetSocketAddress> prox, PastryNode pn) {
    PriorityTransportLayer<MultiInetSocketAddress> ret = super.getPriorityTransportLayer(trans, liveness, prox, pn);
    ((StandardRouter)pn.getRouter()).setRouterStrategy(new RendezvousRouterStrategy(ret, pn.getEnvironment()));
    return ret;
  }
  
  /**
   * This code opens a pilot to our bootstrap node before proceeding.  This is necessary to allow the liveness
   * checks to be sent back to me without the bootstrap node remembering the address that I sent the liveness
   * check on.
   * 
   * When the node goes live, close all of the opened pilots, to not blow out the bootstrap node.
   */
  @Override
  @SuppressWarnings("unchecked")
  protected Bootstrapper getBootstrapper(final PastryNode pn, 
      NodeHandleAdapter tl, 
      NodeHandleFactory handleFactory,
      ProximityNeighborSelector pns) {
    final PilotManager<RendezvousSocketNodeHandle> manager = (PilotManager<RendezvousSocketNodeHandle>)pn.getVars().get(RENDEZVOUS_TL);
    
    // only do the special step if we're NATted
    if (((RendezvousSocketNodeHandle)pn.getLocalHandle()).canContactDirect()) return super.getBootstrapper(pn, tl, handleFactory, pns);

    // set the contents true when booted
    final boolean[] booted = new boolean[1];
    booted[0] = false;
    final ArrayList<RendezvousSocketNodeHandle> closeMeWhenReady = new ArrayList<RendezvousSocketNodeHandle>();    
    Observer obs = new Observer() {    
      public void update(Observable o, Object arg) {
        if (arg instanceof Boolean) {
          if (((Boolean)arg).booleanValue()) {
            // node is ready
            List<RendezvousSocketNodeHandle> temp;
            synchronized(closeMeWhenReady) {
              booted[0] = true;
            }
            for (RendezvousSocketNodeHandle nh : closeMeWhenReady) {
              manager.closePilot(nh);
            }
            closeMeWhenReady.clear();
            o.deleteObserver(this);
          }
        }
      }    
    };
    pn.addObserver(obs);
    
    TLBootstrapper bootstrapper = new TLBootstrapper(pn, tl.getTL(), (SocketNodeHandleFactory)handleFactory, pns) {

      /**
       * Used as a helper to TLBootstrapper.boot() to generate a temporary node handle
       *  
       * @param addr
       * @return
       */
//      @Override
//      protected SocketNodeHandle getTempNodeHandle(InetSocketAddress addr) {
//        RendezvousSocketNodeHandle local = (RendezvousSocketNodeHandle)pn.getLocalNodeHandle();
//        if (!isInternetRoutablePrefix(addr.getAddress()) && !local.canContactDirect() && local.getAddress().getNumAddresses() > 1) {
//          // assume we have the same external address
//          return handleFactory.getNodeHandle(new MultiInetSocketAddress(local.getAddress().getOutermostAddress(), addr), -1, Id.build());          
//        }
//        return handleFactory.getNodeHandle(new MultiInetSocketAddress(addr), -1, Id.build());
//      }
      

//      @Override
//      protected void bootAsBootstrap() {
//        if (!((RendezvousSocketNodeHandle)pn.getLocalHandle()).canContactDirect()) {
//          throw new RuntimeException("Local node is believed to be firewalled and can't be used as a bootstrap node. "+pn.getLocalHandle());
//        }
//        super.bootAsBootstrap();
//      }
      
      @Override
      protected void checkLiveness(final SocketNodeHandle h, Map<String, Object> options) {
        // open pilot first, then call checkliveness, but it's gonna fail the first time, because the NH is bogus.
        // so, open it the first time and watch it fail, then open it again
        ContactDirectStrategy<RendezvousSocketNodeHandle> contactStrat = 
          (ContactDirectStrategy<RendezvousSocketNodeHandle>)pn.getVars().get(RENDEZVOUS_CONTACT_DIRECT_STRATEGY);
        RendezvousSocketNodeHandle rsnh = (RendezvousSocketNodeHandle)h;
        if (!rsnh.canContactDirect() && contactStrat.canContactDirect(rsnh)) {
//          logger.log("not opening pilot to "+rsnh);
          super.checkLiveness(h, options);
          return;
        }
        
//        logger.log("opening pilot to "+rsnh);
        manager.openPilot((RendezvousSocketNodeHandle)h, new Continuation<SocketRequestHandle<RendezvousSocketNodeHandle>, Exception>() {
        
          public void receiveResult(
              SocketRequestHandle<RendezvousSocketNodeHandle> result) {

            // don't hold the lock when calling closePilot()
            boolean close = false;
            
            // see if we already joined
            synchronized(closeMeWhenReady) {
              if (booted[0]) {
                close = true;
              } else {
                closeMeWhenReady.add(result.getIdentifier());
              }
            }
            if (close) {
              logger.log("closing pilot");
              manager.closePilot(result.getIdentifier());
              return;
            }
            
            // remember to close the pilot once we're joined
            pn.getLivenessProvider().checkLiveness(h, null);
          }
        
          public void receiveException(Exception exception) {
            // TODO Auto-generated method stub
            logger.logException("In Rendezvous Bootstrapper.checkLiveness("+h+")", exception);
          }        
        });        
      }      
    };
    return bootstrapper;
  }

  /**
   * @return true if ip address matches firewall prefix
   */
  protected boolean isInternetRoutablePrefix(InetAddress address) {    
    String ip = address.getHostAddress();
    String nattedNetworkPrefixes = environment.getParameters().getString(
        "nat_network_prefixes");

    String[] nattedNetworkPrefix = nattedNetworkPrefixes.split(";");
    for (int i = 0; i < nattedNetworkPrefix.length; i++) {
      if (ip.startsWith(nattedNetworkPrefix[i])) {
        return false;
      }
    }
    return true;
  }


}
