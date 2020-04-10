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

import java.util.*;

import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.RawMessage;

/**
 * @(#) MultiringNodeCollection.java
 *
 * This class is the class which coordinates all of the nodes on a given
 * host, allowing the nodes to route messages between each other.  
 *
 * @version $Id: MultiringNodeCollection.java 4476 2008-09-19 14:58:35Z jeffh $
 *
 * @author Alan Mislove
 */
public class MultiringNodeCollection {
  
  /**
   * The routing base for ring ids, in bytes
   */
  public int BASE;
  
  /**
   * The list of nodes in the collection
   */
  protected Vector nodes;
  
  /**
   * Constructor
   *
   * @param node The node to base this node off of
   */
  public MultiringNodeCollection(MultiringNode node, int BASE) {
    this.nodes = new Vector();
    this.nodes.add(node);
    this.BASE = BASE;
  }
  
  /**
   * This method allows other nodes to be dynamically added to the node
   * collection.  Using this method, a node is added to the collection, and
   * it is told of all of the other nodes in the collection.  Similarly, all
   * of the other nodes in the collection are told of this node.
   *
   * @param node The node to add to the collection
   */
  public void addNode(MultiringNode node) {
    if (! node.getNodeId().equals(((MultiringNode) nodes.elementAt(0)).getNodeId()))
      throw new IllegalArgumentException("Node added does not have the correct nodeId!");
        
    broadcastNewNode(node);
    nodes.add(node);
  }
  
  /**
   * This method informs all of the existing nodes of the newly added node, 
   * and informs the newly added node of all of the existing nodes.
   *
   * @param node The node that is being added
   */
  protected void broadcastNewNode(MultiringNode node) {
    for (int i=0; i<nodes.size(); i++) {
      MultiringNode thisNode = (MultiringNode) nodes.elementAt(i);
      
      if (thisNode.getRingId().equals(node.getRingId()))
        throw new IllegalArgumentException("ERROR: Attempt to add already-existing ringId " + node + " " + node.getId() + " " + thisNode + " " + thisNode.getId());
      
      thisNode.nodeAdded(node.getRingId());
      node.nodeAdded(thisNode.getRingId());
    }
  }
  
  /**
   * This method returns the best next-hop to a given target, using ring hopping,
   * if one exists.  Otherwise, if no local node in the collection is the next
   * hop for the target, null is returned.
   *
   * @param id The target id of the message
   * @param message The message to be sent
   * @param hint A suggested hint
   */
  protected MessageReceipt route(RingId id, RawMessage message, String application,
      DeliveryNotification deliverAckToMe, Map<String, Object> options) {
    MultiringNode best = (MultiringNode) nodes.elementAt(0);
    
    for (int i=1; i<nodes.size(); i++) {
      MultiringNode thisNode = (MultiringNode) nodes.elementAt(i);
      int bestShared = getLengthOfSharedPrefix((RingId) best.getId(), id);
      int thisShared = getLengthOfSharedPrefix((RingId) thisNode.getId(), id);
      int bestLength = getLength((RingId) best.getId());
      int thisLength = getLength((RingId) thisNode.getId());
        
      if ((thisShared > bestShared) || ((thisShared == bestShared) && (thisLength < bestLength)))
        best = thisNode;
    }
    
    //System.out.println("ROUTING MESSAGE TO TARGET " + id + " VIA NODE " + best.getId());
        
    return best.route(id, message, application, deliverAckToMe, options);
  }
  
  /**
   * This method returns the length of the given ringId
   *
   * @param a The ring Id
   * @return The number of shared bytes
   */
  protected int getLength(RingId a) {
    byte[] ba = a.getRingId().toByteArray();
    
    for (int i=0; i<ba.length / BASE; i++) {
      boolean zero = true;
      
      for (int j=0; j<BASE; j++) 
       if (ba[ba.length - 1 - (BASE * i + j)] != 0) 
         zero = false;
        
      if (zero) 
        return i;
    }
            
    return (ba.length/BASE);
  }
  
  /**
   * This method returns the length of the shared prefix, in bytes, of the
   * two provided ringIds by comparing the ring values.
   *
   * @param a The first Id
   * @param b The second Id
   * @return The number of shared bytes
   */
  protected int getLengthOfSharedPrefix(RingId a, RingId b) {
    byte[] ba = a.getRingId().toByteArray();
    byte[] bb = b.getRingId().toByteArray();
    
    for (int i=0; i<ba.length / BASE; i++) {
      boolean same = true;

      for (int j=0; j<BASE; j++) 
        if (ba[ba.length - 1 - (BASE * i + j)] != bb[ba.length - 1 - (BASE * i + j)]) 
          same = false;

      if (! same) 
        return i;
    }

    return (ba.length/BASE);
  }
}




