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

package rice.p2p.commonapi;

import java.io.*;
import java.util.*;

import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * @(#) NodeHandle.java
 *
 * This class is an abstraction of a node handle from the CommonAPI paper. A
 * node handle is a handle to a known node, which conceptually includes the
 * node's Id, as well as the node's underlying network address (such as IP/port).
 *
 * This class is (unfortunately) an abstact class due to the need to be observable.
 *
 * @version $Id: NodeHandle.java 4231 2008-06-24 12:22:18Z jeffh $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public abstract class NodeHandle extends Observable implements Serializable  {

  // constants defining types of observable events
  public static final Integer PROXIMITY_CHANGED = new Integer(1);
  public static final Integer DECLARED_DEAD = new Integer(2);
  public static final Integer DECLARED_LIVE = new Integer(3);
  
  // serialver
  private static final long serialVersionUID = 4761193998848368227L;
  
  /**
   * Returns this node's id.
   *
   * @return The corresponding node's id.
   */
  public abstract Id getId();

  /**
   * Returns whether or not this node is currently alive
   *
   * @deprecated use Endpoint.isAlive(NodeHandle) 
   * @return Whether or not this node is currently alive
   */
  public abstract boolean isAlive();

  /**
   * Returns the current proximity value of this node
   *
   * @deprecated use Endpoint.proximity(NodeHandle)
   * @return The current proximity value of this node
   */
  public abstract int proximity();
  
  /**
   * Requests that the underlying transport layer check to ensure
   * that the remote node is live.  If the node is found to be live, nothing
   * happens, but if the node does not respond, the transport layer
   * make take steps to verify that the node is dead.  Such steps
   * could include finding an alternate route to the node.
   *
   * @return Whether or not the node is currently alive
   */
  public abstract boolean checkLiveness();

  public abstract void serialize(OutputBuffer buf) throws IOException;
  
}


