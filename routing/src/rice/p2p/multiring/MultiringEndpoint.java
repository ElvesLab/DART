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

package rice.p2p.multiring;

import rice.*;

import java.io.IOException;
import java.util.*;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;

import rice.environment.Environment;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.multiring.messaging.RingMessage;
import rice.p2p.util.MCAdapter;
import rice.p2p.util.MRHAdapter;
import rice.p2p.util.rawserialization.JavaSerializedMessage;

/**
 * @(#) MultiringEndpoint.java
 *
 * This class wraps an endpoint and allows for applications to transparently
 * use multiring functionality.
 *
 * @version $Id: MultiringEndpoint.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
public class MultiringEndpoint implements Endpoint {
  
  /**
   * The multiring node supporting this endpoint
   */
  protected MultiringNode node;
  
  /**
   * The application this endpoint is for
   */
  protected Application application;
  
  /**
   * The node which this mulitring node is wrapping
   */
  protected Endpoint endpoint;
  
//  protected MessageDeserializer myDeserializer;
  
  /**
  * Constructor
   *
   * @param node The node to base this node off of
   */
  protected MultiringEndpoint(MultiringNode node, Endpoint endpoint, Application application) {
    this.node = node;
    this.endpoint = endpoint;
    this.application = application;
//    myDeserializer = endpoint.getDeserializer();
//    endpoint.setDeserializer(new MessageDeserializer() {
//    
//      public Message deserialize(InputBuffer buf, short type, byte priority,
//          NodeHandle sender) throws IOException {
//        switch (type) {
//          case RingMessage.TYPE:
//            return new RingMessage(buf,MultiringEndpoint.this.endpoint,myDeserializer,sender,priority);
//        }
//        throw new IllegalArgumentException("Invalid type:"+type);
//      }
//    
//    });
  }
  
  /**
   * Returns this node's id, which is its identifier in the namespace.
   *
   * @return The local node's id
   */
  public Id getId() {
    return RingId.build(node.getRingId(), endpoint.getId());
  }
  
  /**
   * This method makes an attempt to route the message to the root of the given id.
   * The hint handle will be the first hop in the route. If the id field is null, then
   * the message is routed directly to the given node, and delivers the message
   * there.  If the hint field is null, then this method makes an attempt to route
   * the message to the root of the given id.  Note that one of the id and hint fields can
   * be null, but not both.
   *
   * @param id The destination Id of the message.
   * @param message The message to deliver
   * @param hint The first node to send this message to, optional
   */  
  public MessageReceipt route(Id id, Message message, NodeHandle hint) {
    return route(id, message, hint, null);
  }
  
  public MessageReceipt route(Id id, Message message, NodeHandle hint, DeliveryNotification deliverAckToMe) {
    return route(id, message, hint, deliverAckToMe, null);
  }
  
  public MessageReceipt route(Id id, Message message, NodeHandle hint, DeliveryNotification deliverAckToMe, Map<String, Object> options) {
    if (message instanceof RawMessage) {
      return route(id, (RawMessage)message, hint, deliverAckToMe, options);
    } else {
      final MRHAdapter ret = new MRHAdapter();
      return route(id, (RawMessage)new JavaSerializedMessage(message), hint, 
          deliverAckToMe, options); 
    }
  }
  
  public MessageReceipt route(Id id, RawMessage message, NodeHandle hint) {
    return route(id, message, hint, null);
  }
  
  public MessageReceipt route(
      Id id, RawMessage message, 
      NodeHandle hint, 
      DeliveryNotification deliverAckToMe) {
    return route(id, message, hint, deliverAckToMe, null);
  }
  public MessageReceipt route(
      Id id, RawMessage message, 
      NodeHandle hint, 
      DeliveryNotification deliverAckToMe, Map<String, Object> options) {
    
    RingId mId = (RingId) id;
    MultiringNodeHandle mHint = (MultiringNodeHandle) hint;
          
    if (mId == null) {
      if (mHint.getRingId().equals(node.getRingId())) {
        return endpoint.route(null, message, mHint.getHandle(), deliverAckToMe, options);
      } else {
        return route(mHint.getId(), message, null, deliverAckToMe, options);
      }
    } else {
      if (mId.getRingId().equals(node.getRingId())) {
        if ((mHint != null) && (mHint.getRingId().equals(node.getRingId()))) {
          return endpoint.route(mId.getId(), message, mHint.getHandle(), deliverAckToMe, options);
        } else {
          return endpoint.route(mId.getId(), message, null, deliverAckToMe, options);
        }
      } else {
        return node.getCollection().route(mId, message, getInstance(), deliverAckToMe, options);
      }
    } 
  }
  
  
  /**
   * This call produces a list of nodes that can be used as next hops on a route towards
   * the given id, such that the resulting route satisfies the overlay protocol's bounds
   * on the number of hops taken.  If the safe flag is specified, then the fraction of
   * faulty nodes returned is no higher than the fraction of faulty nodes in the overlay.
   *
   * @param id The destination id.
   * @param num The number of nodes to return.
   * @param safe Whether or not to return safe nodes.
   */
  public NodeHandleSet localLookup(Id id, int num, boolean safe) {
    return new MultiringNodeHandleSet(node.getRingId(), endpoint.localLookup(((RingId) id).getId(), num, safe));
  }
  
  /**
   * This methods returns an unordered set of nodehandles on which are neighbors of the local
   * node in the id space.  Up to num handles are returned.
   *
   * @param num The number of desired handle to return.
   */
  public NodeHandleSet neighborSet(int num) {
    return new MultiringNodeHandleSet(node.getRingId(), endpoint.neighborSet(num));
  }
  
  /**
   * This methods returns an ordered set of nodehandles on which replicas of an object with
   * a given id can be stored.  The call returns nodes up to and including a node with maxRank.
   *
   * @param id The object's id.
   * @param maxRank The number of desired replicas.
   */
  public NodeHandleSet replicaSet(Id id, int maxRank) {
    if (((RingId) id).getRingId().equals(node.getRingId()))
      return new MultiringNodeHandleSet(node.getRingId(), endpoint.replicaSet(((RingId) id).getId(), maxRank));
    else
      return new MultiringNodeHandleSet(((RingId) id).getRingId(), node.getNode().getIdFactory().buildNodeHandleSet());
  }
  
  /**
   * This methods returns an ordered set of nodehandles on which replicas of an object with
   * a given id can be stored.  The call returns nodes up to and including a node with maxRank.
   * This call also allows the application to provide a remove "center" node, as well as
   * other nodes in the vicinity. 
   *
   * @param id The object's id.
   * @param maxRank The number of desired replicas.
   * @param handle The root handle of the remove set
   * @param set The set of other nodes around the root handle
   */
  public NodeHandleSet replicaSet(Id id, int maxRank, NodeHandle root, NodeHandleSet set) {
    if (((RingId) id).getRingId().equals(node.getRingId()))
      return new MultiringNodeHandleSet(node.getRingId(), endpoint.replicaSet(((RingId) id).getId(), 
                                                                              maxRank,
                                                                              ((MultiringNodeHandle) root).getHandle(),
                                                                              ((MultiringNodeHandleSet) set).getSet()));
    else
      return new MultiringNodeHandleSet(((RingId) id).getRingId(), node.getNode().getIdFactory().buildNodeHandleSet());      
  }
  
  /**
   * This operation provides information about ranges of keys for which the node is currently
   * a rank-root. The operations returns null if the range could not be determined, the range
   * otherwise. It is an error to query the range of a node not present in the neighbor set as
   * returned bythe update upcall or the neighborSet call. Certain implementations may return
   * an error if rank is greater than zero. Some protocols may have multiple, disjoint ranges
   * of keys for which a given node is responsible. The parameter lkey allows the caller to
   * specify which region should be returned. If the node referenced by is responsible for key
   * lkey, then the resulting range includes lkey. Otherwise, the result is the nearest range
   * clockwise from lkey for which is responsible.
   *
   * @param handle The handle whose range to check.
   * @param rank The root rank.
   * @param lkey An "index" in case of multiple ranges.
   */
  public IdRange range(NodeHandle handle, int rank, Id lkey) {
    if (lkey != null)
      lkey = ((RingId) lkey).getId();
    
    IdRange result = endpoint.range(((MultiringNodeHandle) handle).getHandle(), rank, lkey);
    
    if (result != null)
      return new MultiringIdRange(node.getRingId(), result);
    else
      return null;
  }
  
  /**
   * This operation provides information about ranges of keys for which the node is currently
   * a rank-root. The operations returns null if the range could not be determined, the range
   * otherwise. It is an error to query the range of a node not present in the neighbor set as
   * returned bythe update upcall or the neighborSet call. Certain implementations may return
   * an error if rank is greater than zero. Some protocols may have multiple, disjoint ranges
   * of keys for which a given node is responsible. The parameter lkey allows the caller to
   * specify which region should be returned. If the node referenced by is responsible for key
   * lkey, then the resulting range includes lkey. Otherwise, the result is the nearest range
   * clockwise from lkey for which is responsible.
   *
   * @param handle The handle whose range to check.
   * @param rank The root rank.
   * @param lkey An "index" in case of multiple ranges.
   * @param cumulative Whether to return the cumulative or single range
   */
  public IdRange range(NodeHandle handle, int rank, Id lkey, boolean cumulative) {
    if (lkey != null)
      lkey = ((RingId) lkey).getId();
    
    IdRange result = endpoint.range(((MultiringNodeHandle) handle).getHandle(), rank, lkey, cumulative);
    
    if (result != null)
      return new MultiringIdRange(node.getRingId(), result);
    else
      return null;
  }
  
  /**
   * Returns a handle to the local node below this endpoint.  This node handle is serializable,
   * and can therefore be sent to other nodes in the network and still be valid.
   *
   * @return A NodeHandle referring to the local node.
   */
  public NodeHandle getLocalNodeHandle() {
    return new MultiringNodeHandle(node.getRingId(), endpoint.getLocalNodeHandle());
  }
  
  /**
   * Schedules a message to be delivered to this application after the provided number of
   * milliseconds.
   *
   * @param message The message to be delivered
   * @param delay The number of milliseconds to wait before delivering the message
   */
  public CancellableTask scheduleMessage(Message message, long delay) {
    return endpoint.scheduleMessage(message, delay);
  }
  
  /**
   * Schedules a message to be delivered to this application every period number of 
   * milliseconds, after delay number of miliseconds have passed.
   *
   * @param message The message to be delivered
   * @param delay The number of milliseconds to wait before delivering the fist message
   * @param delay The number of milliseconds to wait before delivering subsequent messages
   */
  public CancellableTask scheduleMessage(Message message, long delay, long period) {
    return endpoint.scheduleMessage(message, delay, period);
  }
  
  public CancellableTask scheduleMessageAtFixedRate(Message message, long delay, long period) {
    return endpoint.scheduleMessageAtFixedRate(message, delay, period);
  }
  
  public List<NodeHandle> networkNeighbors(int num) {
    return endpoint.networkNeighbors(num);
  }
  
  /**
   * Schedules a job for processing on the dedicated processing thread.  CPU intensive jobs, such
   * as encryption, erasure encoding, or bloom filter creation should never be done in the context
   * of the underlying node's thread, and should only be done via this method.  
   *
   * @param task The task to run on the processing thread
   * @param command The command to return the result to once it's done
   */
  @SuppressWarnings("unchecked")
  public void process(Executable task, Continuation command) {
    endpoint.process(task, command);
  }
  
  /**
   * Internal method which delivers the message to the application
   *
   * @param id The target id
   * @param message The message to deliver
   */
  protected void deliver(RingId id, Message target) {
    application.deliver(id, target);
  }
  
  /**
   * Returns a unique instance name of this endpoint, sort of a mailbox name for this
   * application.
   * 
   * @return The unique instance name of this application
   */
  public String getInstance() {
    return "multiring" + endpoint.getInstance();
  }

  /* (non-Javadoc)
   * @see rice.p2p.commonapi.Endpoint#getEnvironment()
   */
  public Environment getEnvironment() {
    return node.getEnvironment();
  }

  /**
   * Passthrough to sub endpoint.
   */
  public void connect(NodeHandle handle, AppSocketReceiver receiver, int timeout) {
    MultiringNodeHandle mHandle = (MultiringNodeHandle) handle;
    endpoint.connect(mHandle.getHandle(), receiver, timeout);
  }

  public void accept(AppSocketReceiver receiver) {
    endpoint.accept(receiver);
  }

  public void setDeserializer(MessageDeserializer md) {
    endpoint.setDeserializer(md);
  }

  public MessageDeserializer getDeserializer() {
    return endpoint.getDeserializer();
  }

  public Id readId(InputBuffer buf, short type) throws IOException {
    if (type == RingId.TYPE)
      return new RingId(buf, endpoint);

    return endpoint.readId(buf, type);
  }

  public NodeHandle readNodeHandle(InputBuffer buf) throws IOException {
    return new MultiringNodeHandle(buf, endpoint);
  }

  public IdRange readIdRange(InputBuffer buf) throws IOException {
    return new MultiringIdRange(buf,endpoint);
//    return endpoint.readIdRange(buf);
  }

  public NodeHandle coalesce(NodeHandle handle) {
    if (handle instanceof MultiringNodeHandle) {
      MultiringNodeHandle mnh = (MultiringNodeHandle)handle;
      mnh.handle = endpoint.coalesce(mnh.handle);
      return mnh;
    }
    return endpoint.coalesce(handle);
  }

  public NodeHandleSet readNodeHandleSet(InputBuffer buf, short type) throws IOException {
    switch(type) {
      case MultiringNodeHandleSet.TYPE:
        return new MultiringNodeHandleSet(buf, endpoint);
    }
    return endpoint.readNodeHandleSet(buf, type);
  }
  
  public String toString() {
    return "MRE["+endpoint+"]";
  }
  
  public void register() {
    endpoint.register(); 
  }

  public int proximity(NodeHandle nh) {
    return endpoint.proximity(nh);    
  }
  
  public boolean isAlive(NodeHandle nh) {
    return endpoint.isAlive(nh);
  }

  public void setConsistentRouting(boolean val) {
    endpoint.setConsistentRouting(val);
  }

  public boolean routingConsistentFor(Id id) {
    return endpoint.routingConsistentFor(id);
  }
  
  public void setSendOptions(Map<String, Object> options) {
    endpoint.setSendOptions(options);
  }


}




