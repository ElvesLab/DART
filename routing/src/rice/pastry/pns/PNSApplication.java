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
package rice.pastry.pns;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.mpisws.p2p.transport.proximity.ProximityListener;
import org.mpisws.p2p.transport.proximity.ProximityProvider;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.exception.TimeoutException;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.util.AttachableCancellable;
import rice.p2p.util.tuples.MutableTuple;
import rice.p2p.util.tuples.Tuple;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.client.PastryAppl;
import rice.pastry.leafset.LeafSet;
import rice.pastry.messaging.Message;
import rice.pastry.pns.messages.LeafSetRequest;
import rice.pastry.pns.messages.LeafSetResponse;
import rice.pastry.pns.messages.RouteRowRequest;
import rice.pastry.pns.messages.RouteRowResponse;
import rice.pastry.routing.RouteSet;
import rice.pastry.standard.ProximityNeighborSelector;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;
import rice.selector.Timer;
import rice.selector.TimerTask;

/**
 * Can request LeafSet, RouteRow, Proximity of nodes, implements the PNS algorithm.
 * 
 * Flow:
 *   call getLeafSet(...)
 *     addToWaitingForLeafSet()
 *   
 *   cancellable.cancel()
 *     removeFromWaitingForLeafSet()
 * 
 * 
 * TODO: Make use the environment's clock for the wait() calls.
 * 
 * @author Jeff Hoye
 *
 */
public class PNSApplication extends PastryAppl implements ProximityNeighborSelector, ProximityListener<NodeHandle> {
  public static final int DEFAULT_PROXIMITY = ProximityProvider.DEFAULT_PROXIMITY;
  
  /**
   * Hashtable which keeps track of temporary ping values, which are
   * only used during the getNearest() method
   */
  protected Map<NodeHandle,Integer> pingCache = new HashMap<NodeHandle,Integer>();
  
  protected final byte rtBase;
  
  protected Environment environment;

  protected Timer timer;

  final short depth; // = (Id.IdBitLength / rtBase);

  public PNSApplication(PastryNode pn) {
    this(pn, pn.getEnvironment().getLogManager().getLogger(PNSApplication.class, null));
  }
  
  public PNSApplication(PastryNode pn, Logger logger) {
    super(pn, null, 0, null, logger);
    setDeserializer(new PNSDeserializer());
    this.environment = pn.getEnvironment();
    rtBase = (byte)environment.getParameters().getInt("pastry_rtBaseBitLength");
    depth = (short)(Id.IdBitLength / rtBase);
  }

  /**
   * We always want to receive messages.
   */
  public boolean deliverWhenNotReady() {
    return true;
  }

  @Override
  public void messageForAppl(Message msg) {
//    logger.log("messageForAppl("+msg+")");
    if (logger.level <= Logger.FINER) logger.log("messageForAppl("+msg+")");

    if (msg instanceof LeafSetRequest) {
      LeafSetRequest req = (LeafSetRequest)msg;
      thePastryNode.send(req.getSender(), new LeafSetResponse(thePastryNode.getLeafSet(), getAddress()), null, null);
      return;
    }
    
    if (msg instanceof LeafSetResponse) {
      LeafSetResponse response = (LeafSetResponse)msg;
      synchronized (waitingForLeafSet) {
        LeafSet ls = response.leafset;
        Collection<Tuple<Continuation<LeafSet, Exception>, Cancellable>> waiters = waitingForLeafSet.remove(ls.get(0));
        if (waiters != null) {
          for (Tuple<Continuation<LeafSet, Exception>, Cancellable> w : waiters) {
            w.b().cancel();
            w.a().receiveResult(ls);            
          }
        }
      }
      return;
    }
    
    if (msg instanceof RouteRowRequest) {
      RouteRowRequest req = (RouteRowRequest)msg;
      thePastryNode.send(req.getSender(), 
          new RouteRowResponse(
              thePastryNode.getLocalHandle(), 
              req.index, 
              thePastryNode.getRoutingTable().getRow(req.index), getAddress()), null, null);
      return;
    }
    
    if (msg instanceof RouteRowResponse) {
      RouteRowResponse response = (RouteRowResponse)msg;
      synchronized (waitingForRouteRow) {
        Collection<Tuple<Continuation<RouteSet[], Exception>, Cancellable>>[] waiters = waitingForRouteRow.get(response.getSender());
        if (waiters != null) {
          if (waiters[response.index] != null) {
            for (Tuple<Continuation<RouteSet[], Exception>, Cancellable> w : new ArrayList<Tuple<Continuation<RouteSet[], Exception>, Cancellable>>(waiters[response.index])) {
              w.b().cancel();
              w.a().receiveResult(response.row);            
            }
            waiters[response.index].clear();
            waiters[response.index] = null;
            
            // remove the entry if all rows empty
            boolean deleteIt = true;
            for (int i = 0; i < depth; i++) {
              if (waiters[i] != null) {
                deleteIt = false;
                break;
              }
            }
          
            if (deleteIt) waitingForRouteRow.remove(response.getSender());
          }
        }
      }
      return;
    }
    

    if (logger.level <= Logger.WARNING) logger.log("unrecognized message in messageForAppl("+msg+")");
  }
  
  public Cancellable getNearHandles(final Collection<NodeHandle> bootHandles, final Continuation<Collection<NodeHandle>, Exception> deliverResultToMe) {
    if (bootHandles == null || bootHandles.size() == 0 || bootHandles.iterator().next() == null) {
      deliverResultToMe.receiveResult(bootHandles);
      return null;
    }

    final AttachableCancellable ret = new AttachableCancellable();
    
    // when this goes empty, return what we have
    final Collection<NodeHandle> remaining = new HashSet<NodeHandle>(bootHandles);
    
    thePastryNode.addProximityListener(this);
    
    // our best candidate so far, initiall null
    final MutableTuple<NodeHandle, Cancellable> best = new MutableTuple<NodeHandle, Cancellable>();
    
    // get the proximity of everyone in the list
    for (NodeHandle nh : bootHandles) {
      final NodeHandle handle = nh;
      Continuation<Integer, IOException> c = new Continuation<Integer, IOException>() {
                
        public void receiveResult(Integer result) {
          if (logger.level <= Logger.FINE) logger.log("got proximity for "+handle+" in getNearHandles()");
          if ((best.a() != null) && 
              (pingCache.get(best.a()).intValue() < result.intValue())) {
            // we're not the best, just return
            return;
          }
          
          // we are the best
          
          // cancel the last task
          if (best.b() != null) best.b().cancel();
          
          // set ourself as best
          Cancellable cancellable = getNearest(handle, new Continuation<Collection<NodeHandle>, Exception>() {
    
            public void receiveResult(Collection<NodeHandle> result) {
              if (logger.level <= Logger.FINE) logger.log("receiveResult("+result+") in getNearHandles()");
              ret.cancel(); // go ahead and cancel everything
              finish();
            }
    
            public void receiveException(Exception exception) {        
              logger.logException("PNS got an exception in getNearHandles() returning what we got.",exception);
              finish();
            }    
      
            public void finish() {
              thePastryNode.removeProximityListener(PNSApplication.this);
              List<NodeHandle> ret = sortedProximityCache();
              purgeProximityCache();
              if (logger.level <= Logger.INFO) logger.log("getNearHandles("+bootHandles+"):"+ret.size()+ret);
              deliverResultToMe.receiveResult(getNearHandlesHelper(ret));        
            }
          });

          ret.attach(cancellable);
          best.set(handle, cancellable);
        }
      
        public void receiveException(IOException exception) {
          remaining.remove(handle);
        }      
      };
      getProximity(handle, c, 10000); // TODO: Make configurable
    }
    return ret;
  }
  
  /**
   * Helper for getNearHandles
   * 
   * Can be overridden to select out any handles that shouldn't be returned.
   * 
   * @param handles
   * @return
   */
  protected List<NodeHandle> getNearHandlesHelper(List<NodeHandle> handles) {
    return handles;
  }
  
  /**
   * This method returns the remote leafset of the provided handle
   * to the caller, in a protocol-dependent fashion.  Note that this method
   * may block while sending the message across the wire.
   *
   * Non-blocking version.
   * 
   * @param handle The node to connect to
   * @param c Continuation to return the LeafSet to
   * @return
   * @throws IOException
   */
  public Cancellable getLeafSet(final NodeHandle handle, final Continuation<LeafSet, Exception> c) {
    final AttachableCancellable cancellable = new AttachableCancellable(){          
      public boolean cancel() {        
        removeFromWaitingForLeafSet(handle, c);
        super.cancel();
        
        // it was cancelled if it was removed from the list, and if not then it was already cancelled
        return true;
      }    
    };
    
    final TimerTask task = new TimerTask(){        
      @Override
      public void run() {
        cancellable.cancel();
        c.receiveException(new TimeoutException("Ping to "+handle+" timed out."));
      }        
    };

    addToWaitingForLeafSet(handle, c, task);
    cancellable.attach(task);
    
    cancellable.attach(thePastryNode.send(handle, new LeafSetRequest(this.getNodeHandle(), this.getAddress()), new PMessageNotification() {
      public void sent(PMessageReceipt msg) {        
      }
      public void sendFailed(PMessageReceipt msg, Exception reason) {
        cancellable.cancel();
//        removeFromWaitingForLeafSet(handle, c);
        c.receiveException(reason);
      }    
    }, null));
    
    return cancellable;
  }

  Map<NodeHandle, Collection<Tuple<Continuation<LeafSet, Exception>, Cancellable>>> waitingForLeafSet = 
    new HashMap<NodeHandle, Collection<Tuple<Continuation<LeafSet, Exception>, Cancellable>>>();
  
  protected void addToWaitingForLeafSet(NodeHandle handle, Continuation<LeafSet, Exception> c, Cancellable cancelMeWhenSuccess) {
    synchronized (waitingForLeafSet) {
      Collection<Tuple<Continuation<LeafSet, Exception>, Cancellable>> waiters = waitingForLeafSet.get(handle);
      if (waiters == null) {
        waiters = new ArrayList<Tuple<Continuation<LeafSet,Exception>,Cancellable>>();
        waitingForLeafSet.put(handle, waiters);
      }
      waiters.add(new Tuple<Continuation<LeafSet, Exception>, Cancellable>(c,cancelMeWhenSuccess));
    }    
  }
  
  protected boolean removeFromWaitingForLeafSet(NodeHandle handle, Continuation<LeafSet, Exception> c) {
    synchronized (waitingForLeafSet) {
      Collection<Tuple<Continuation<LeafSet, Exception>, Cancellable>> waiters = waitingForLeafSet.get(handle);
      if (waiters == null) return false;          
      
      // look for and remove c in waiters
      boolean ret = false;
      Iterator<Tuple<Continuation<LeafSet, Exception>, Cancellable>> i = waiters.iterator();
      while(i.hasNext()) {
        Tuple<Continuation<LeafSet, Exception>, Cancellable> foo = i.next();
        if (foo.a().equals(c)) {
          ret = true;
          foo.b().cancel();
          i.remove();
        }
      }
      
      if (waiters.isEmpty()) waitingForLeafSet.remove(handle);
      return ret;
    }    
  }
  
  /**
   * Non-blocking version.
   * 
   * @param handle
   * @param row
   * @param c
   * @return
   * @throws IOException
   */
  public Cancellable getRouteRow(final NodeHandle handle, final short row, final Continuation<RouteSet[], Exception> c) {
    final AttachableCancellable cancellable = new AttachableCancellable(){          
      public boolean cancel() {        
        removeFromWaitingForRouteRow(handle, row, c);
        super.cancel();
        
        // it was cancelled if it was removed from the list, and if not then it was already cancelled
        return true;
      }    
    };
    
    final TimerTask task = new TimerTask(){        
      @Override
      public void run() {
        cancellable.cancel();
        c.receiveException(new TimeoutException("Ping to "+handle+" timed out."));
      }        
    };

    addToWaitingForRouteRow(handle, row, c, task);
    cancellable.attach(task);
    
    cancellable.attach(thePastryNode.send(handle, new RouteRowRequest(this.getNodeHandle(), row, this.getAddress()), new PMessageNotification() {
      public void sent(PMessageReceipt msg) {        
      }
      public void sendFailed(PMessageReceipt msg, Exception reason) {
//        removeFromWaitingForRouteRow(handle, row, c);
        cancellable.cancel();
        c.receiveException(reason);
      }    
    }, null));
    
    return cancellable;
  }

  Map<NodeHandle, Collection<Tuple<Continuation<RouteSet[], Exception>, Cancellable>>[]> waitingForRouteRow = 
    new HashMap<NodeHandle, Collection<Tuple<Continuation<RouteSet[], Exception>, Cancellable>>[]>();
  
  protected void addToWaitingForRouteRow(NodeHandle handle, int row, Continuation<RouteSet[], Exception> c, Cancellable cancelMeWhenSuccess) {
    synchronized (waitingForRouteRow) {
      Collection<Tuple<Continuation<RouteSet[], Exception>, Cancellable>>[] waiters = waitingForRouteRow.get(handle);
      if (waiters == null) {
        waiters = new Collection[depth];
        waitingForRouteRow.put(handle, waiters);
      }
      if (waiters[row] == null) {
        waiters[row] = new ArrayList<Tuple<Continuation<RouteSet[], Exception>, Cancellable>>();        
      }
      waiters[row].add(new Tuple<Continuation<RouteSet[], Exception>, Cancellable>(c, cancelMeWhenSuccess));
    }    
  }
  
  protected boolean removeFromWaitingForRouteRow(NodeHandle handle, int row, Continuation<RouteSet[], Exception> c) {
    synchronized (waitingForRouteRow) {
      Collection<Tuple<Continuation<RouteSet[], Exception>, Cancellable>>[] waiters = waitingForRouteRow.get(handle);
      if (waiters == null) return false;
      if (waiters[row] == null) return false;
      boolean ret = false;
      Iterator<Tuple<Continuation<RouteSet[], Exception>, Cancellable>> it = waiters[row].iterator();
      while(it.hasNext()) {
        Tuple<Continuation<RouteSet[], Exception>, Cancellable> foo = it.next();
        if (foo.a().equals(c)) {
          ret = true;
          foo.b().cancel();
          it.remove();
        }
      }
      
      // remove the row if empty
      if (waiters[row].isEmpty()) {
        waiters[row] = null;
      }
      
      // remove the entry if all rows empty
      boolean deleteIt = true;
      for (int i = 0; i < depth; i++) {
        if (waiters[i] != null) {
          deleteIt = false;
          break;
        }
      }
      
      if (deleteIt) waitingForRouteRow.remove(handle);

      return ret;
    }    
  }
  
  
  /**
   * This method determines and returns the proximity of the current local
   * node the provided NodeHandle.  This will need to be done in a protocol-
   * dependent fashion and may need to be done in a special way.
   *
   * Timeout of 5 seconds.
   *
   * @param handle The handle to determine the proximity of
   * @return The proximity of the provided handle
   */
//  public int getProximity(NodeHandle handle) {    
//    final int[] container = new int[1];
//    container[0] = DEFAULT_PROXIMITY;
//    synchronized (container) {
//      getProximity(handle, new Continuation<Integer, IOException>() {      
//        public void receiveResult(Integer result) {
//          synchronized(container) {
//            container[0] = result.intValue();
//            container.notify();
//          }
//        }
//      
//        public void receiveException(IOException exception) {
//          synchronized(container) {
//            container.notify();
//          }
//        }      
//      });
//      
//      if (container[0] == DEFAULT_PROXIMITY) {
//        try {
//          container.wait(5000);
//        } catch(InterruptedException ie) {
//          // continue to return        
//        }
//      }
//    }
//    if (logger.level <= Logger.FINE) logger.log("getProximity("+handle+") returning "+container[0]);
//    return container[0];
//  }

  /**
   * Non-blocking version, no timeout.
   * 
   * TODO: Make this fail early if faulty.
   * 
   * @param handle
   * @param c
   */
  public Cancellable getProximity(final NodeHandle handle, final Continuation<Integer, IOException> c, int timeout) {
    if (logger.level <= Logger.FINEST) logger.log("getProximity("+handle+")");
    int prox;
    
    // acquire a lock that will block proximityChanged()
    synchronized(waitingForPing) {
      // see what we have for proximity (will initiate a checkLiveness => proximityChanged() if DEFAULT)
      prox = thePastryNode.proximity(handle);
      if (prox == DEFAULT_PROXIMITY) {
        if (logger.level <= Logger.FINE) logger.log("getProximity("+handle+"): waiting for proximity update");
        // need to find it
        Collection<Continuation<Integer, IOException>> waiters = waitingForPing.get(handle);
        if (waiters == null) {          
          waiters = new ArrayList<Continuation<Integer,IOException>>();
          waitingForPing.put(handle, waiters);
        }
        waiters.add(c);
        
        final AttachableCancellable cancellable = new AttachableCancellable(){          
          public boolean cancel() {        
            synchronized(waitingForPing) {
              Collection<Continuation<Integer, IOException>> waiters = waitingForPing.get(handle);
              if (waiters != null) {          
                waiters.remove(c);
                if (waiters.isEmpty()) {
                  waitingForPing.remove(handle);
                }
              }              
            }
            super.cancel();
            
            // it was cancelled if it was removed from the list, and if not then it was already cancelled
            return true;
          }    
        };
        
        TimerTask task = new TimerTask(){        
          @Override
          public void run() {
            cancellable.cancel();
            c.receiveException(new TimeoutException("Ping to "+handle+" timed out."));
          }        
        };

        cancellable.attach(task);
        environment.getSelectorManager().schedule(task, timeout);
        return cancellable;
      } 
      
      // else we have the proximity, no need to wait for it, just return the correct result
    }
    
    synchronized(pingCache) {
      pingCache.put(handle,prox);
    }
    
    // we already had the right proximity
    c.receiveResult(prox);    
    return null;
  }


  Map<NodeHandle, Collection<Continuation<Integer, IOException>>> waitingForPing = 
    new HashMap<NodeHandle, Collection<Continuation<Integer, IOException>>>();
  public void proximityChanged(NodeHandle i, int newProximity, Map<String, Object> options) {
    if (logger.level <= Logger.FINE) logger.log("proximityChanged("+i+","+newProximity+")");
    synchronized(pingCache) {
      pingCache.put(i, newProximity);
    }
    synchronized(waitingForPing) {      
      if (waitingForPing.containsKey(i)) {
        Collection<Continuation<Integer, IOException>> waiting = waitingForPing.remove(i);
        for (Continuation<Integer, IOException>c : waiting) {
          c.receiveResult(newProximity);
        }
      }
    }    
  }

  
  /**
   * Method which checks to see if we have a cached value of the remote ping, and
   * if not, initiates a ping and then caches the value
   *
   * @param handle The handle to ping
   * @return The proximity of the handle
   */
//  protected void proximity(NodeHandle handle, Continuation<Integer, Exception>) {
//    Hashtable<NodeHandle,Integer> localTable = pingCache;
//    
//    Integer i = localTable.get(handle);
//    
//    if (i == null) {
//      int value = thePastryNode.proximity(handle);
//      synchronized(localTable) {
//        localTable.put(handle, value);
//      }
//      return value;
//    } else {
//      return i.intValue();
//    }
//  }
  
  private void purgeProximityCache() {
    pingCache.clear(); 
  }
  
  public List<NodeHandle> sortedProximityCache() {
    ArrayList<NodeHandle> handles = new ArrayList<NodeHandle>(pingCache.keySet());
    Collections.sort(handles,new Comparator<NodeHandle>() {
    
      public int compare(NodeHandle a, NodeHandle b) {
        return pingCache.get(a).intValue()-pingCache.get(b).intValue();
      }    
    });
    
//    Iterator<NodeHandle> i = handles.iterator();
//    while(i.hasNext()) {
//      NodeHandle nh = i.next();
//      System.out.println(nh+":"+localTable.get(nh));
//    }
    
    return handles;
  }
  
  /**
   * This method implements the algorithm in the Pastry locality paper
   * for finding a close node the the current node through iterative
   * leafset and route row requests.  The seed node provided is any
   * node in the network which is a member of the pastry ring.  This
   * algorithm is designed to work in a protocol-independent manner, using
   * the getResponse(Message) method provided by subclasses.
   *
   * @param seed Any member of the pastry ring
   * @return A node suitable to boot off of (which is close the this node)
   */
  public Cancellable getNearest(final NodeHandle seed, final Continuation<Collection<NodeHandle>, Exception> retToMe) {    
//    try {
    if (logger.level <= Logger.FINE) logger.log("getNearest("+seed+")");
      // if the seed is null, we can't do anything
    if (seed == null) {
      if (logger.level <= Logger.WARNING) logger.logException("getNearest("+seed+")", new Exception("Stack Trace"));
      environment.getSelectorManager().invoke(new Runnable() {        
        public void run() {
          retToMe.receiveResult(null);
        }        
      });
      return null;
    }
    
    final AttachableCancellable ret = new AttachableCancellable();
    
    // get closest node in leafset
    ret.attach(getLeafSet(seed, 
      new Continuation<LeafSet, Exception>() {
    
        public void receiveResult(LeafSet result) {
          // ping everyone in the leafset:
          if (logger.level <= Logger.FINE) logger.log("getNearest("+seed+") got "+result);
          
          // seed is the bootstrap node that we use to enter the pastry ring
          NodeHandle nearNode = seed;
          NodeHandle currentClosest = seed;
          
          ret.attach(closestToMe(nearNode, result, new Continuation<NodeHandle, Exception>() {
          
            /**
             * Now we have the closest node in the leafset.
             */
            public void receiveResult(NodeHandle result) {
              NodeHandle nearNode = result;
              // get the number of rows in a routing table
              // -- Here, we're going to be a little inefficient now.  It doesn't
              // -- impact correctness, but we're going to walk up from the bottom
              // -- of the routing table, even through some of the rows are probably
              // -- unfilled.  We'll optimize this in a later iteration.
              short i = 0;
              
              // make "ALL" work
              if (!environment.getParameters().getString("pns_num_rows_to_use").equalsIgnoreCase("all")) {
                i = (short)(depth-(short)(environment.getParameters().getInt("pns_num_rows_to_use")));
              }
              
              // fix it up to not throw an error if the number is too big
              if (i < 0) i = 0;                                              
              
              ret.attach(seekThroughRouteRows(i,depth,nearNode,new Continuation<NodeHandle, Exception>(){
              
                public void receiveResult(NodeHandle result) {
                  NodeHandle nearNode = result;
                  retToMe.receiveResult(sortedProximityCache());
                }
              
                public void receiveException(Exception exception) {
                  retToMe.receiveResult(sortedProximityCache());                    
                }                
              }));                
            }
          
            public void receiveException(Exception exception) {
              retToMe.receiveResult(sortedProximityCache());                    
            }            
          }));
        }
      
        public void receiveException(Exception exception) {
          retToMe.receiveException(exception);
        }        
      }));
    
//    } catch (IOException e) {
//      if (logger.level <= Logger.WARNING) logger.logException(
//        "ERROR occured while finding best bootstrap.", e);
//      return new NodeHandle[]{seed};
//    } finally {
//      purgeProximityCache(); 
//    }
    return ret;
  }

  
  /**
   * This method recursively seeks through the routing tables of the other nodes, always taking the closest node.  
   * When it gets to the top, it keeps taking the top row of the cloest node.  Tail recursion with the continuation.
   * 
   *                 // now, iteratively walk up the routing table, picking the closest node
                // each time for the next request
//                while (i < depth) {
//                  nearNode = closestToMe(nearNode, getRouteRow(nearNode, i));
//                  i++;
//                }
//                // finally, recursively examine the top level routing row of the nodes
//                // until no more progress can be made
//                do {
//                  currentClosest = nearNode;
//                  nearNode = closestToMe(nearNode, getRouteRow(nearNode, (short)(depth-1)));
//                } while (! currentClosest.equals(nearNode));
//                

   * @param i
   * @param depth
   * @param nearNode
   * @param name
   * @return
   */
  private Cancellable seekThroughRouteRows(final short i, final short depth, final NodeHandle currentClosest, 
      final Continuation<NodeHandle, Exception> returnToMe) {
    final AttachableCancellable ret = new AttachableCancellable();
    
    // base case
    ret.attach(getRouteRow(currentClosest, i, new Continuation<RouteSet[], Exception>(){    
      public void receiveResult(RouteSet[] result) {
        ret.attach(closestToMe(currentClosest,result,new Continuation<NodeHandle, Exception>() {        
          public void receiveResult(NodeHandle nearNode) {
            if ((i >= depth-1) && (currentClosest.equals(nearNode))) {
              // base case
              // i == depth and we didn't find a closer node
              returnToMe.receiveResult(nearNode);
            } else {
              // recursive case
              short newIndex = (short)(i+1);
              if (newIndex > depth-1) newIndex = (short)(depth-1);
              seekThroughRouteRows(newIndex, depth, nearNode, returnToMe);
            }
          }
        
          public void receiveException(Exception exception) {
            returnToMe.receiveException(exception);
          }        
        }));
        
      }
    
      public void receiveException(Exception exception) {
        returnToMe.receiveException(exception);
      }
    
    }));
    
    return ret;
  }


  /**
   * This method returns the closest node to the current node out of
   * the union of the provided handle and the node handles in the
   * leafset
   *
   * @param handle The handle to include
   * @param leafSet The leafset to include
   * @return The closest node out of handle union leafset
   */
  private Cancellable closestToMe(NodeHandle handle, LeafSet leafSet, Continuation<NodeHandle, Exception> c)  {
    if (leafSet == null) {
      c.receiveResult(handle);
      return null;
    }
    HashSet<NodeHandle> handles = new HashSet<NodeHandle>();

    for (int i = 1; i <= leafSet.cwSize() ; i++)
      handles.add(leafSet.get(i));

    for (int i = -leafSet.ccwSize(); i < 0; i++)
      handles.add(leafSet.get(i));

    return closestToMe(handle, handles, c);
  }

  /**
   * This method returns the closest node to the current node out of
   * the union of the provided handle and the node handles in the
   * routeset
   *
   * @param handle The handle to include
   * @param routeSet The routeset to include
   * @return The closest node out of handle union routeset
   */
  private Cancellable closestToMe(NodeHandle handle, RouteSet[] routeSets, Continuation<NodeHandle, Exception> c) {
    ArrayList<NodeHandle> handles = new ArrayList<NodeHandle>();

    for (int i=0 ; i<routeSets.length ; i++) {
      RouteSet set = routeSets[i];

      if (set != null) {
        for (int j=0; j<set.size(); j++)
          handles.add(set.get(j));
      }
    }

    return closestToMe(handle, handles, c);
  }

  /**
   * This method returns the closest node to the current node out of
   * the union of the provided handle and the node handles in the
   * array
   *
   * @param handle The handle to include
   * @param handles The array to include
   * @return The closest node out of handle union array
   */
  private Cancellable closestToMe(final NodeHandle handle, final Collection<NodeHandle> handles, final Continuation<NodeHandle, Exception> c) {
    if (logger.level <= Logger.FINE) logger.log("closestToMe("+handle+","+handles+")");
    final AttachableCancellable ret = new AttachableCancellable();
    final NodeHandle[] closestNode = {handle};

    final Collection<NodeHandle> remaining = new HashSet<NodeHandle>(handles);
    if (!remaining.contains(handle)) remaining.add(handle);
    
    // shortest distance found till now    
    final int[] nearestdist = {Integer.MAX_VALUE}; //proximity(closestNode);  

    ArrayList<NodeHandle> temp = new ArrayList<NodeHandle>(remaining); // need to include handle when looping
    for (NodeHandle nh : temp) {
      final NodeHandle tempNode = nh;
      if (logger.level <= Logger.FINER) logger.log("closestToMe checking prox on "+tempNode+"("+handle+","+handles+")");      
      ret.attach(getProximity(handle, new Continuation<Integer, IOException>(){
      
        public void receiveResult(Integer result) {
          if (logger.level <= Logger.FINEST) logger.log("closestToMe got prox("+result.intValue()+") on "+tempNode+"("+handle+","+handles+")");      
          remaining.remove(tempNode);
          int prox = result.intValue();
          if ((prox >= 0) && (prox < nearestdist[0]) && tempNode.isAlive()) {
            nearestdist[0] = prox;
            closestNode[0] = tempNode;
          }
          finish();
        }
      
        public void receiveException(IOException exception) {
          remaining.remove(tempNode);
          finish();
        }      
        
        public void finish() {
          if (remaining.isEmpty()) {
            ret.cancel();
            c.receiveResult(closestNode[0]);
          }
        }
      }, 10000));
    }
    
    return ret;
  }


  
  class PNSDeserializer implements MessageDeserializer {
    public rice.p2p.commonapi.Message deserialize(InputBuffer buf, short type, int priority, rice.p2p.commonapi.NodeHandle sender) throws IOException {
      switch(type) {
      case LeafSetRequest.TYPE:
        return LeafSetRequest.build(buf, (NodeHandle)sender, getAddress());
      case LeafSetResponse.TYPE:
        return LeafSetResponse.build(buf, thePastryNode, getAddress());
      case RouteRowRequest.TYPE:
        return RouteRowRequest.build(buf, (NodeHandle)sender, getAddress());
      case RouteRowResponse.TYPE:
        return new RouteRowResponse(buf, thePastryNode, (NodeHandle)sender, getAddress());
      }
      // TODO Auto-generated method stub
      return null;
    }

  }
}
