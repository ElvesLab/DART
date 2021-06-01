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

import java.io.IOException;
import java.util.*;


import rice.environment.logging.Logger;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.SizeCheckOutputBuffer;
import rice.pastry.*;
import rice.pastry.messaging.*;
import rice.pastry.socket.TransportLayerNodeHandle;

/**
 * the node handle used with the direct network
 *
 * @version $Id: DirectNodeHandle.java 4632 2008-12-11 15:39:45Z jeffh $
 * @author Andrew Ladd
 * @author Rongmei Zhang/Y. Charlie Hu
 */

public class DirectNodeHandle extends TransportLayerNodeHandle<NodeRecord> implements Observer {
//  private transient PastryNode remoteNode;
  public transient NetworkSimulator simulator;

  /**
   * Constructor for DirectNodeHandle.
   *
   * @param ln The local pastry node
   * @param sim The current network simulator
   */
  protected DirectNodeHandle(PastryNode ln, NetworkSimulator sim) {
    localnode = ln;
    logger = ln.getEnvironment().getLogManager().getLogger(getClass(), null);
//    if (rn == null) throw new IllegalArgumentException("rn must be non-null");
    simulator = sim;
    
    ln.addObserver(this);
  }

  /**
   * Gets the Remote attribute of the DirectNodeHandle object
   *
   * @return The Remote value
   */
  public PastryNode getRemote() {
    return localnode;
  }

  /**
   * Gets the NodeId attribute of the DirectNodeHandle object
   *
   * @return The NodeId value
   */
  public Id getNodeId() {
    return localnode.getNodeId();
  }

  /**
   * Gets the Alive attribute of the DirectNodeHandle object
   *
   * @return The Alive value
   */
  public int getLiveness() {
    if (simulator.isAlive(this)) {
      return LIVENESS_ALIVE;
    }
    return LIVENESS_DEAD; 
  }

  /**
   * Gets the Simulator attribute of the DirectNodeHandle object
   *
   * @return The Simulator value
   */
  public NetworkSimulator getSimulator() {
    return simulator;
  }

  /**
   * DESCRIBE THE METHOD
   *
   * @param arg DESCRIBE THE PARAMETER
   */
  public void notifyObservers(Object arg) {
    setChanged();
    super.notifyObservers(arg);
  }

  /**
   * DESCRIBE THE METHOD
   *
   * @return DESCRIBE THE RETURN VALUE
   */
  public boolean ping() {
    return isAlive();
  }
  
  public final void assertLocalNode() {
    if (DirectPastryNode.getCurrentNode() == null) {
//      ctor.printStackTrace();
      throw new RuntimeException("PANIC: localnode is null in " + this+"@"+System.identityHashCode(this));
    }
  }

  /**
   * DESCRIBE THE METHOD
   *
   * @deprecated
   * @return DESCRIBE THE RETURN VALUE
   */
  public int proximity() {
    assertLocalNode();
    return getLocalNode().proximity(this);
  }

  /**
   * DESCRIBE THE METHOD
   *
   * @deprecated use PastryNode.send()
   * @param msg DESCRIBE THE PARAMETER
   */
  public void receiveMessage(Message msg) {
    DirectPastryNode.getCurrentNode().send(this, msg,null, null);
  }

  /**
   * Equivalence relation for nodehandles. They are equal if and only if their corresponding NodeIds
   * are equal.
   *
   * @param obj the other nodehandle .
   * @return true if they are equal, false otherwise.
   */
  public boolean equals(Object obj) {
    // we know that there is only one of these per node in the simulator
    return obj == this;
    
//    if (obj == null) {
//      return false;
//    }
//    DirectNodeHandle nh = (DirectNodeHandle) obj;
//
//    if (this.remoteNode.getNodeId().equals(nh.remoteNode.getNodeId())) {
//      return true;
//    } else {
//      return false;
//    }
  }

  /**
   * Hash codes for node handles.It is the hashcode of their corresponding NodeId's.
   *
   * @return a hash code.
   */
  public int hashCode() {
    return this.getNodeId().hashCode();
  }

  /**
   * DESCRIBE THE METHOD
   *
   * @return DESCRIBE THE RETURN VALUE
   */
  public String toString() {
    return "[DNH " + getNodeId() + "]";
  }

  /**
   * Only notify if dead.  Note that this is limitied in that it's not possible 
   * to simulate a byzantine failure of a node.  But that's out of the scope of 
   * the simulator.  If we leave in the first arg, the node notifies DECLARED_LIVE 
   * way too often.
   */
  public void update(Observable arg0, Object arg1) {
    if (simulator.isAlive(this)) {
//      notifyObservers(NodeHandle.DECLARED_LIVE);      
    } else {
//      System.out.println(this+"Notifying dead");
      notifyObservers(NodeHandle.DECLARED_DEAD);      
    }
  }

  public void serialize(OutputBuffer buf) throws IOException {
    if (buf instanceof SizeCheckOutputBuffer) {
      ((SizeCheckOutputBuffer)buf).writeSpecial(this);
    } else {
      throw new RuntimeException("DirectNodeHandle.serialize() Should not be called.  If you are doing this to determine the size, please use a SizeCheckOutputBuffer such as the DirectSizeChecker.");
    }
  }

  public NodeRecord getAddress() {
    return simulator.getNodeRecord(this);
  }

  public long getEpoch() {
    return 0;
  }
}
