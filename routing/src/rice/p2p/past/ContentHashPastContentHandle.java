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

package rice.p2p.past;

import java.io.*;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.past.rawserialization.RawPastContentHandle;

/**
 * @(#) ContentHashPastContentHandle.java
 *
 * A handle class for content-hash objects stored in Past.
 *
 * @version $Id: ContentHashPastContentHandle.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author Peter Druschel
 */
public class ContentHashPastContentHandle implements RawPastContentHandle {
  public static final short TYPE = -12;
  
  
  // the node on which the content object resides
  private NodeHandle storageNode;

  // the object's id
  private Id myId;

  /**
   * Constructor
   *
   * @param nh The handle of the node which holds the object
   * @param id key identifying the object to be inserted
   */
  public ContentHashPastContentHandle(NodeHandle nh, Id id) {
    storageNode = nh;
    myId = id;
  }

  
  // ----- PastCONTENTHANDLE METHODS -----

  /**
   * Returns the id of the PastContent object associated with this handle
   *
   * @return the id
   */
  public Id getId() {
    return myId;
  }

  /**
   * Returns the NodeHandle of the Past node on which the object associated
   * with this handle is stored
   *
   * @return the id
   */
  public NodeHandle getNodeHandle() {
    return storageNode;
  }

  public ContentHashPastContentHandle(InputBuffer buf, Endpoint endpoint) throws IOException {
    myId = endpoint.readId(buf, buf.readShort());
    storageNode = endpoint.readNodeHandle(buf);
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeShort(myId.getType());
    myId.serialize(buf);
    storageNode.serialize(buf);
  }
  
  public short getType() {
    return TYPE; 
  }
}










