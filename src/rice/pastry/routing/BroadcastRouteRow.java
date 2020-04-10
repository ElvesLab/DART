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
package rice.pastry.routing;

import rice.p2p.commonapi.rawserialization.*;
import rice.pastry.*;
import rice.pastry.messaging.*;

import java.util.*;
import java.io.*;

/**
 * Broadcast message for a row from a routing table.
 * 
 * @version $Id: BroadcastRouteRow.java 3613 2007-02-15 14:45:14Z jstewart $
 * 
 * @author Andrew Ladd
 */

public class BroadcastRouteRow extends PRawMessage implements Serializable {

  
  private NodeHandle fromNode;

  private RouteSet[] row;

  public static final short TYPE = 2;

  
  
  /**
   * Constructor.
   * 
   * @param stamp the timestamp
   * @param from the node id
   * @param r the row
   */
  public BroadcastRouteRow(Date stamp, NodeHandle from, RouteSet[] r) {
    super(RouteProtocolAddress.getCode(), stamp);
    fromNode = from;
    row = r;
    setPriority(MAX_PRIORITY);
  }

  /**
   * Constructor.
   * 
   * @param from the node id
   * @param r the row
   */
  public BroadcastRouteRow(NodeHandle from, RouteSet[] r) {
    this(null, from, r);
  }

  /**
   * Gets the from node.
   * 
   * @return the from node.
   */
  public NodeHandle from() {
    return fromNode;
  }

  /**
   * Gets the row that was sent in the message.
   * 
   * @return the row.
   */
  public RouteSet[] getRow() {
    return row;
  }

  public String toString() {
    String s = "";

    s += "BroadcastRouteRow(of " + fromNode.getNodeId() + ")";

    return s;
  }
  
  public String toStringFull() {
    String s = "BRR{"+fromNode+"}:";
    for (int i = 0; i < row.length; i++) {
      s+=row[i]+"|";
    }
    return s;
  }
  
  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version
    fromNode.serialize(buf);
    buf.writeByte((byte) row.length);
    for (int i=0; i<row.length; i++) {
      if (row[i] != null) {
        buf.writeBoolean(true);
        row[i].serialize(buf);
      } else {
        buf.writeBoolean(false);
      }
    }
  }

  public BroadcastRouteRow(InputBuffer buf, NodeHandleFactory nhf, PastryNode localNode) throws IOException {
    super(RouteProtocolAddress.getCode(), null);    
    
    byte version = buf.readByte();
    switch(version) {
      case 0:
        fromNode = nhf.readNodeHandle(buf);
        row = new RouteSet[buf.readByte()];
        for (int i=0; i<row.length; i++)
          if (buf.readBoolean()) {
            row[i] = new RouteSet(buf, nhf, localNode);
          }
        break;
      default:
        throw new IOException("Unknown Version: "+version);
    }      
  }
}