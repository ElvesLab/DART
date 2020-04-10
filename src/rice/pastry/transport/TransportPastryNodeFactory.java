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
package rice.pastry.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.environment.random.RandomSource;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.CancellableTask;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.ReadyStrategy;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.join.JoinProtocol;
import rice.pastry.leafset.LeafSet;
import rice.pastry.leafset.LeafSetProtocol;
import rice.pastry.messaging.MessageDispatch;
import rice.pastry.pns.PNSApplication;
import rice.pastry.routing.RouteSet;
import rice.pastry.routing.RouteSetProtocol;
import rice.pastry.routing.RouterStrategy;
import rice.pastry.routing.RoutingTable;
import rice.pastry.standard.ConsistentJoinProtocol;
import rice.pastry.standard.PeriodicLeafSetProtocol;
import rice.pastry.standard.ProximityNeighborSelector;
import rice.pastry.standard.RapidRerouter;
import rice.pastry.standard.StandardJoinProtocol;
import rice.pastry.standard.StandardLeafSetProtocol;
import rice.pastry.standard.StandardRouteSetProtocol;
import rice.pastry.standard.StandardRouter;

@SuppressWarnings("unchecked")
public abstract class TransportPastryNodeFactory extends PastryNodeFactory {

  /**
   * Large period (in seconds) means infrequent, 0 means never.
   */
  protected int leafSetMaintFreq;

  protected int routeSetMaintFreq;


  /**
   * Constructor.
   * 
   * Here is order for bind address 1) bindAddress parameter 2) if bindAddress
   * is null, then parameter: socket_bindAddress (if it exists) 3) if
   * socket_bindAddress doesn't exist, then InetAddress.getLocalHost()
   * 
   * @param nf The factory for building node ids
   * @param bindAddress which address to bind to
   * @param startPort The port to start creating nodes on
   * @param env The environment.
   */
  public TransportPastryNodeFactory(Environment env) {
    super(env);
    Parameters params = env.getParameters();
    leafSetMaintFreq = params.getInt("pastry_leafSetMaintFreq");
    routeSetMaintFreq = params.getInt("pastry_routeSetMaintFreq");
  }
    
  public PastryNode nodeHandleHelper(PastryNode pn) throws IOException {
//    final Object lock = new Object();
//    final ArrayList<IOException> retException = new ArrayList<IOException>();
//    Object nhaPart1 = getnodeHandleAdapterPart1(pn);
    
    NodeHandleFactory handleFactory = getNodeHandleFactory(pn);
    NodeHandle localhandle = getLocalHandle(pn, handleFactory);    
    
    TLDeserializer deserializer = getTLDeserializer(handleFactory,pn);
  
    MessageDispatch msgDisp = new MessageDispatch(pn, deserializer);
    RoutingTable routeTable = new RoutingTable(localhandle, rtMax, rtBase,
        pn);
    LeafSet leafSet = new LeafSet(localhandle, lSetSize, routeTable);
//            StandardRouter router = new RapidRerouter(pn, msgDisp);
    StandardRouter router = new RapidRerouter(pn, msgDisp, getRouterStrategy(pn));
    pn.setElements(localhandle, msgDisp, leafSet, routeTable, router);
  
    
    NodeHandleAdapter nha = getNodeHandleAdapter(pn, handleFactory, deserializer);

    pn.setSocketElements(leafSetMaintFreq, routeSetMaintFreq, 
        nha, nha, nha, handleFactory);
    
    router.register();
    
    registerApps(pn, leafSet, routeTable, nha, handleFactory);
    
    return pn;
  }

  protected RouterStrategy getRouterStrategy(PastryNode pn) {
    return null; // use the default one
  }
  
  protected void registerApps(PastryNode pn, LeafSet leafSet, RoutingTable routeTable, NodeHandleAdapter nha, NodeHandleFactory handleFactory) {
    ProximityNeighborSelector pns = getProximityNeighborSelector(pn);
    
    Bootstrapper bootstrapper = getBootstrapper(pn, nha, handleFactory, pns);          

    RouteSetProtocol rsProtocol = getRouteSetProtocol(pn, leafSet, routeTable);
      
    LeafSetProtocol lsProtocol = getLeafSetProtocol(pn, leafSet, routeTable);
    
    ReadyStrategy readyStrategy;
    if (lsProtocol instanceof ReadyStrategy) {
      readyStrategy = (ReadyStrategy)lsProtocol;
    } else {
      readyStrategy = pn.getDefaultReadyStrategy();
    }
    
    JoinProtocol jProtocol = getJoinProtocol(pn, leafSet, routeTable, readyStrategy);
    
    pn.setJoinProtocols(bootstrapper, jProtocol, lsProtocol, rsProtocol);    
  }
  
  protected RouteSetProtocol getRouteSetProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable) {
    StandardRouteSetProtocol rsProtocol = new StandardRouteSetProtocol(pn, routeTable);    
    rsProtocol.register();
    return rsProtocol;
  }
  
  protected LeafSetProtocol getLeafSetProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable) {
    PeriodicLeafSetProtocol lsProtocol = new PeriodicLeafSetProtocol(pn,
        pn.getLocalHandle(), leafSet, routeTable);    
//    StandardLeafSetProtocol lsProtocol = new StandardLeafSetProtocol(pn,pn.getLocalHandle(),leafSet,routeTable);
    lsProtocol.register();
    return lsProtocol;
    
  }
  
  protected JoinProtocol getJoinProtocol(PastryNode pn, LeafSet leafSet, RoutingTable routeTable, ReadyStrategy lsProtocol) {
    ConsistentJoinProtocol jProtocol = new ConsistentJoinProtocol(pn,
        pn.getLocalHandle(), routeTable, leafSet, lsProtocol);
//    StandardJoinProtocol jProtocol = new StandardJoinProtocol(pn,pn.getLocalHandle(), routeTable, leafSet);
    jProtocol.register();
    return jProtocol;    
  }
  
  protected TLDeserializer getTLDeserializer(NodeHandleFactory handleFactory, PastryNode pn) {
    TLDeserializer deserializer = new TLDeserializer(handleFactory, pn.getEnvironment());
    return deserializer;
  }

  /**
   * Can be overridden.
   * @param pn
   * @return
   */
  protected ProximityNeighborSelector getProximityNeighborSelector(PastryNode pn) {    
    if (environment.getParameters().getBoolean("transport_use_pns")) {
      PNSApplication pns = new PNSApplication(pn);
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
    
  protected abstract NodeHandle getLocalHandle(PastryNode pn, 
      NodeHandleFactory handleFactory) throws IOException;
  protected abstract NodeHandleAdapter getNodeHandleAdapter(PastryNode pn, 
      NodeHandleFactory handleFactory, TLDeserializer deserializer) throws IOException;
  protected abstract NodeHandleFactory getNodeHandleFactory(PastryNode pn) throws IOException;
  protected abstract Bootstrapper getBootstrapper(PastryNode pn, 
      NodeHandleAdapter tl, 
      NodeHandleFactory handleFactory,
      ProximityNeighborSelector pns);  
}
