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

import java.io.*;
import java.util.*;

/**
 * Request a row from the routing table from another node.
 * 
 * @version $Id: RequestRouteRow.java 3613 2007-02-15 14:45:14Z jstewart $
 * 
 * @author Andrew Ladd
 */

public class RequestRouteRow extends PRawMessage implements Serializable {
  public static final short TYPE = 1;

  private short row;

  /**
   * Constructor.
   * 
   * @param nh the return handle.
   * @param r which row
   */

  public RequestRouteRow(NodeHandle nh, short r) {
    this(null, nh, r);
  }

  /**
   * Constructor.
   * 
   * @param stamp the timestamp
   * @param nh the return handle
   * @param r which row
   */
  public RequestRouteRow(Date stamp, NodeHandle nh, short r) {
    super(RouteProtocolAddress.getCode(), stamp);
    setSender(nh);
    row = r;
    setPriority(MAX_PRIORITY);
  }

  /**
   * The return handle for the message
   * 
   * @return the node handle
   */

  public NodeHandle returnHandle() {
    return getSender();
  }

  /**
   * Gets the row that made the request.
   * 
   * @return the row.
   */

  public short getRow() {
    return row;
  }

  public String toString() {
    String s = "";

    s += "RequestRouteRow(row " + row + " by " + getSender().getNodeId() + ")";

    return s;
  }
  
  /***************** Raw Serialization ***************************************/  
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version    
    buf.writeShort(row);
  }
  
  public RequestRouteRow(NodeHandle sender, InputBuffer buf) throws IOException {
    super(RouteProtocolAddress.getCode(), null);
    setSender(sender);
    
    byte version = buf.readByte();
    switch(version) {
      case 0:
        row = buf.readShort();
        setPriority(MAX_PRIORITY);
        break;
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
}