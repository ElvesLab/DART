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
 * @(#) NodeHandleSet.java
 *
 * An interface to a generic set of node handles.
 *
 * @version $Id: NodeHandleSet.java 4098 2008-02-13 09:36:33Z jeffh $
 *
 * @author Jeff Hoye
 * @author Alan Mislove
 */
public interface NodeHandleSet extends Serializable /*, Iterable<NodeHandle>*/ {
  
  /**
   * Puts a NodeHandle into the set.
   *
   * @param handle the handle to put.
   *
   * @return true if the put succeeded, false otherwise.
   */
  public boolean putHandle(NodeHandle handle);

  /**
   * Finds the NodeHandle associated with the NodeId.
   *
   * @param id a node id.
   * @return the handle associated with that id or null if no such handle is found.
   */
  public NodeHandle getHandle(Id id);

  /**
   * Gets the ith element in the set.
   *
   * @param i an index.
   * @return the handle associated with that id or null if no such handle is found.
   */
  public NodeHandle getHandle(int i);

  /**
   * Verifies if the set contains this particular id.
   *
   * @param id a node id.
   * @return true if that node id is in the set, false otherwise.
   */
  public boolean memberHandle(Id id);

  /**
   * Removes a node id and its handle from the set.
   *
   * @param nid the node to remove.
   *
   * @return the node handle removed or null if nothing.
   */
  public NodeHandle removeHandle(Id id);

  /**
   * Gets the size of the set.
   *
   * @return the size.
   */
  public int size();
  
  /**
   * Gets the index of the element with the given node id.
   *
   * @param id the id.
   *
   * @return the index or throws a NoSuchElementException.
   */
  public int getIndexHandle(Id id) throws NoSuchElementException;
  
  public void serialize(OutputBuffer buf) throws IOException;  
  
  public short getType();
}
