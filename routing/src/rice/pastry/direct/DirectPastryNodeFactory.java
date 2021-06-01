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

package rice.pastry.direct;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.*;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.*;
import rice.pastry.messaging.*;
import rice.pastry.standard.*;
import rice.pastry.transport.NodeHandleAdapter;
import rice.pastry.transport.TLDeserializer;
import rice.pastry.transport.TransportPastryNodeFactory;
import rice.pastry.routing.*;
import rice.pastry.boot.Bootstrapper;
import rice.pastry.leafset.*;

import java.io.*;
import java.util.*;

import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.direct.DirectTransportLayer;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.liveness.LivenessProvider;
import org.mpisws.p2p.transport.proximity.ProximityListener;
import org.mpisws.p2p.transport.proximity.ProximityProvider;

/**
 * Pastry node factory for direct connections between nodes (local instances).
 *
 * @version $Id: DirectPastryNodeFactory.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Andrew Ladd
 * @author Sitaram Iyer
 * @author Rongmei Zhang/Y. Charlie Hu
 */
@SuppressWarnings("unchecked")
public class DirectPastryNodeFactory extends TransportPastryNodeFactory {
  protected NodeIdFactory nidFactory;
//  protected RandomSource random;

  protected NetworkSimulator simulator;
  
  protected Collection<NodeHandleFactoryListener<NodeHandle>> listeners = 
    new ArrayList<NodeHandleFactoryListener<NodeHandle>>();

  
  /**
   * Main constructor.
   * 
   * @param nf the NodeIdFactory
   * @param sim the NetworkSimulator
   * @param e the Enviornment
   */
  public DirectPastryNodeFactory(NodeIdFactory nf, NetworkSimulator<DirectNodeHandle,RawMessage> sim, Environment env) {    
    super(env);
    env.getParameters().setInt("pastry_protocol_consistentJoin_max_time_to_be_scheduled",120000);
    nidFactory = nf;
    simulator = sim;
  }

  @Override
  protected LeafSetProtocol getLeafSetProtocol(PastryNode pn, LeafSet leafSet,
      RoutingTable routeTable) {
    if (pn.getEnvironment().getParameters().getBoolean("pastry_direct_guarantee_consistency")) { // true
      return super.getLeafSetProtocol(pn, leafSet, routeTable);
    } else {
//      System.out.println("using standard ls protocol");
      StandardLeafSetProtocol ret = new StandardLeafSetProtocol(pn,pn.getLocalHandle(),leafSet, routeTable);
      ret.register();
      return ret;
    }
  }

  /**
   * Getter for the NetworkSimulator.
   * 
   * @return the NetworkSimulator we are using.
   */
  public NetworkSimulator<DirectNodeHandle,RawMessage> getNetworkSimulator() { return simulator; }
  
  /**
   * Manufacture a new Pastry node.
   *
   * @return a new PastryNode
   */
  public PastryNode newNode(NodeHandle bootstrap) {
    return newNode(bootstrap, nidFactory.generateNodeId());
  }
  public PastryNode newNode() throws IOException {
    return newNode(nidFactory.generateNodeId());
  }

  HashMap<Id, NodeRecord> recordTable = new HashMap<Id, NodeRecord>();

  public PastryNode newNode(NodeHandle bootstrap, Id nodeId) {
    try {
      if (bootstrap == null)
        if (logger.level <= Logger.WARNING) logger.log(
            "No bootstrap node provided, starting a new ring...");
      PastryNode pn = newNode(nodeId);
      if (bootstrap == null) {
        pn.getBootstrapper().boot(Collections.EMPTY_LIST);
      } else {
        pn.getBootstrapper().boot(Collections.singleton(bootstrap));
      }
      return pn;
    } catch (IOException ioe) {
      logger.logException("Couldn't construct node.", ioe);
      return null;
    }
  }
  
  /**
   * Manufacture a new Pastry node.
   *
   * @return a new PastryNode
   */
  public PastryNode newNode(Id nodeId) throws IOException {
      
      // this code builds a different environment for each PastryNode
      Environment environment = this.environment;
      if (this.environment.getParameters().getBoolean("pastry_factory_multipleNodes")) {
        if (this.environment.getLogManager() instanceof CloneableLogManager) {
          environment = new Environment(
              this.environment.getSelectorManager(),
              this.environment.getProcessor(),
            this.environment.getRandomSource(),
            this.environment.getTimeSource(),
            ((CloneableLogManager)this.environment.getLogManager()).clone("0x"+nodeId.toStringBare()),
            this.environment.getParameters(), 
            this.environment.getExceptionStrategy());
        }
      }    
      PastryNode pn = new PastryNode(nodeId, environment);
      nodeHandleHelper(pn);
//      ((DirectPastryNode)pn).doneNode(getNearest(pn.getLocalHandle(), bootstrap));
        
      return pn;
  }

  /**
   * This method returns the remote leafset of the provided handle
   * to the caller, in a protocol-dependent fashion.  Note that this method
   * may block while sending the message across the wire.
   *
   * @param handle The node to connect to
   * @return The leafset of the remote node
   */
  public LeafSet getLeafSet(NodeHandle handle) throws IOException {
    DirectNodeHandle dHandle = (DirectNodeHandle) handle;

    return dHandle.getRemote().getLeafSet();
  }

  /**
   * The non-blocking versions here all execute immeadiately.  
   * This CancellableTask is just a placeholder.
   * @author Jeff Hoye
   */
  static class NullCancellableTask implements CancellableTask {
    public void run() {
    }

    public boolean cancel() {
      return false;
    }

    public long scheduledExecutionTime() {
      return 0;
    }
  }

  
  public CancellableTask getLeafSet(NodeHandle handle, Continuation<LeafSet, Exception> c) {
    DirectNodeHandle dHandle = (DirectNodeHandle) handle;
    c.receiveResult(dHandle.getRemote().getLeafSet());
    return new NullCancellableTask();  
  }

  /**
   * This method returns the remote route row of the provided handle
   * to the caller, in a protocol-dependent fashion.  Note that this method
   * may block while sending the message across the wire.
   *
   * @param handle The node to connect to
   * @param row The row number to retrieve
   * @return The route row of the remote node
   */
  public RouteSet[] getRouteRow(NodeHandle handle, int row) throws IOException {
    DirectNodeHandle dHandle = (DirectNodeHandle) handle;

    return dHandle.getRemote().getRoutingTable().getRow(row);
  }

  public CancellableTask getRouteRow(NodeHandle handle, int row, Continuation<RouteSet[], Exception> c) {
    DirectNodeHandle dHandle = (DirectNodeHandle) handle;
    c.receiveResult(dHandle.getRemote().getRoutingTable().getRow(row));
    return new NullCancellableTask();  
  }
  
  /**
   * This method determines and returns the proximity of the current local
   * node the provided NodeHandle.  This will need to be done in a protocol-
   * dependent fashion and may need to be done in a special way.
   *
   * @param handle The handle to determine the proximity of
   * @return The proximity of the provided handle
   */
  public int getProximity(NodeHandle local, NodeHandle remote) {
    return (int)simulator.proximity((DirectNodeHandle)local, (DirectNodeHandle)remote);
  }

  @Override
  protected NodeHandle getLocalHandle(PastryNode pn, NodeHandleFactory handleFactory) throws IOException {
    DirectNodeHandle localhandle = new DirectNodeHandle(pn, simulator);
    return localhandle;
  }

  @Override
  protected NodeHandleFactory getNodeHandleFactory(PastryNode pn) throws IOException {
    // TODO: Make this work
    return new NodeHandleFactory<NodeHandle>(){
    
      public NodeHandle readNodeHandle(InputBuffer buf) throws IOException {
        // TODO Auto-generated method stub
        return null;
      }
    
      public NodeHandle coalesce(NodeHandle handle) {
        // TODO Auto-generated method stub
        notifyListeners(handle);
        return null;
      }

      /**
       * Notify the listeners that this new handle has come along.
       */
      protected void notifyListeners(NodeHandle nh) {
        Collection<NodeHandleFactoryListener<NodeHandle>> temp = listeners;
        synchronized (listeners) {      
          temp = new ArrayList<NodeHandleFactoryListener<NodeHandle>>(listeners);      
        }
        for (NodeHandleFactoryListener<NodeHandle> foo:temp) {
          foo.nodeHandleFound(nh);
        }
      }

      public void addNodeHandleFactoryListener(
          NodeHandleFactoryListener<NodeHandle> listener) {
        synchronized(listeners) {
          listeners.add(listener);
        }
      }


      public void removeNodeHandleFactoryListener(
          NodeHandleFactoryListener<NodeHandle> listener) {
        synchronized(listeners) {
          listeners.remove(listener);
        }
      }
    };
  }

  @Override
  protected NodeHandleAdapter getNodeHandleAdapter(final PastryNode pn, NodeHandleFactory handleFactory, TLDeserializer deserializer) throws IOException {
    NodeRecord nr = (NodeRecord)recordTable.get(pn.getId());
    if (nr == null) {
      nr = simulator.generateNodeRecord();
      recordTable.put(pn.getNodeId(),nr);
    }
    TransportLayer<NodeHandle, RawMessage> tl = getDirectTransportLayer(pn, nr);
    // new DirectTransportLayer<NodeHandle, RawMessage>(pn.getLocalHandle(), simulator, nr, pn.getEnvironment());
        
    NodeHandleAdapter nha = new NodeHandleAdapter(tl,simulator.getLivenessProvider(),new ProximityProvider<NodeHandle>(){          
      public int proximity(NodeHandle i, Map<String, Object> options) {
        return (int)simulator.proximity((DirectNodeHandle)pn.getLocalHandle(), (DirectNodeHandle)i);
      }
    
      // proximity won't change, so don't worry about it
      List<ProximityListener<NodeHandle>> proxListeners = new ArrayList<ProximityListener<NodeHandle>>();
      public void addProximityListener(ProximityListener<NodeHandle> name) {
        proxListeners.add(name);
      }

      public boolean removeProximityListener(ProximityListener<NodeHandle> name) {
        return proxListeners.remove(name);
      }

      public void clearState(NodeHandle i) {

      }    
    });
    
    return nha;
  }

  /**
   * Override me
   * @param pn
   * @param nr
   * @return
   */
  protected TransportLayer<NodeHandle, RawMessage> getDirectTransportLayer(PastryNode pn, NodeRecord nr) {
    DirectTransportLayer<NodeHandle, RawMessage> tl = new DirectTransportLayer<NodeHandle, RawMessage>(pn.getLocalHandle(), simulator, nr, pn.getEnvironment());    
    return tl;
  }
  
  @Override
  protected Bootstrapper getBootstrapper(final PastryNode pn, NodeHandleAdapter tl, NodeHandleFactory handleFactory, final ProximityNeighborSelector pns) {
    return new Bootstrapper<NodeHandle>() {
    
      public void boot(Collection<NodeHandle> bootaddresses) {
        pns.getNearHandles(bootaddresses, new Continuation<Collection<NodeHandle>, Exception>() {        
          public void receiveResult(Collection<NodeHandle> result) {
            if (logger.level <= Logger.INFO) logger.log("boot() calling pn.doneNode("+result+")");
            pn.doneNode(result);
          }        
          public void receiveException(Exception exception) {
            // TODO Auto-generated method stub        
          }        
        });
      }    
    };
  }
}
