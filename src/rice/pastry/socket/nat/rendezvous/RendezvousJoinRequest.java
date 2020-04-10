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
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.join.JoinAddress;
import rice.pastry.join.JoinRequest;
import rice.pastry.leafset.LeafSet;

/**
 * Includes the bootstrap (or some other node who will have a pilot from the joiner.)
 * 
 * @author Jeff Hoye
 *
 */
public class RendezvousJoinRequest extends JoinRequest {
  public static final short TYPE = 4;

  /**
   * The joiner has created a pilot connection to the pilot node.
   */
  protected NodeHandle pilot;
  
  public RendezvousJoinRequest(NodeHandle nh, byte rtBaseBitLength,
      long timestamp, NodeHandle pilot) {
    super(nh, rtBaseBitLength, timestamp);
    this.pilot = pilot;
  }

  public String toString() {
    return "RendezvousJoinRequest(" + (handle != null ? handle.getNodeId() : null) + ","
    + (joinHandle != null ? joinHandle.getNodeId() : null) +","+timestamp+ " pilot:"+pilot+")";
  }
  
  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {    
    super.serialize(buf);
    pilot.serialize(buf);
  }
  
  public RendezvousJoinRequest(InputBuffer buf, NodeHandleFactory nhf, NodeHandle sender, PastryNode localNode) throws IOException {
    super(buf, nhf, sender, localNode);
    pilot = nhf.readNodeHandle(buf);
  }

  public NodeHandle getPilot() {
    return pilot;
  }
}
