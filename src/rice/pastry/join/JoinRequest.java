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
package rice.pastry.join;

import rice.p2p.commonapi.rawserialization.*;
import rice.pastry.*;
import rice.pastry.messaging.*;
import rice.pastry.routing.*;
import rice.pastry.leafset.*;

import java.io.*;
import java.util.*;

/**
 * Request to join this network.
 * 
 * 
 * 
 * 
 * @version $Id: JoinRequest.java 4062 2007-12-26 12:35:03Z jeffh $
 * 
 * @author Jeff Hoye, Andrew Ladd
 */
public class JoinRequest extends PRawMessage {

  public static final byte HAS_HANDLE = 0x01;
  public static final byte HAS_JOIN_HANDLE = 0x02;
  public static final byte HAS_LEAFSET = 0x02;
  
  static final long serialVersionUID = 231671018732832563L;
  
  public static final short TYPE = 1;
  
  protected NodeHandle handle;

  protected NodeHandle joinHandle;

  private short rowCount;

  private RouteSet rows[][];

  private LeafSet leafSet;

  private byte rtBaseBitLength;
  
  protected long timestamp;
  
  /**
   * Constructor.
   * 
   * @param nh a handle of the node trying to join the network.
   */
  public JoinRequest(NodeHandle nh, byte rtBaseBitLength) {
    this(nh, null, rtBaseBitLength);
  }

  public JoinRequest(NodeHandle nh, byte rtBaseBitLength, long timestamp) {
    this(nh, null, rtBaseBitLength);
    this.timestamp = timestamp;
  }

  /**
   * Constructor.
   * 
   * @param nh a handle of the node trying to join the network.
   * @param stamp the timestamp
   */
  public JoinRequest(NodeHandle nh, Date stamp, byte rtBaseBitLength) {
    super(JoinAddress.getCode(), stamp);
    handle = nh;
    initialize(rtBaseBitLength);
    setPriority(MAX_PRIORITY);
  }
  
  /**
   * Gets the handle of the node trying to join.
   * 
   * @return the handle.
   */

  public NodeHandle getHandle() {
    return handle;
  }

  /**
   * Gets the handle of the node that accepted the join request;
   * 
   * @return the handle.
   */

  public NodeHandle getJoinHandle() {
    return joinHandle;
  }

  /**
   * Gets the leafset of the node that accepted the join request;
   * 
   * @return the leafset.
   */

  public LeafSet getLeafSet() {
    return leafSet;
  }

  /**
   * Returns true if the request was accepted, false if it hasn't yet.
   */

  public boolean accepted() {
    return joinHandle != null;
  }

  /**
   * Accept join request.
   * 
   * @param nh the node handle that accepts the join request.
   */

  public void acceptJoin(NodeHandle nh, LeafSet ls) {
    joinHandle = nh;
    leafSet = ls;
  }

  /**
   * Returns the number of rows left to determine (in order).
   * 
   * @return the number of rows left.
   */

  public int lastRow() {
    return rowCount;
  }

  /**
   * Push row.
   * 
   * @param row the row to push.
   */

  public void pushRow(RouteSet row[]) {
    rows[--rowCount] = row;
  }

  /**
   * Get row.
   * 
   * @param i the row to get.
   * 
   * @return the row.
   */

  public RouteSet[] getRow(int i) {
    return rows[i];
  }

  /**
   * Get the number of rows.
   * 
   * @return the number of rows.
   */

  public int numRows() {
    return rows.length;
  }

  private void initialize(byte rtBaseBitLength) {
    joinHandle = null;
    this.rtBaseBitLength = rtBaseBitLength;
    rowCount = (short)(Id.IdBitLength / rtBaseBitLength);

    rows = new RouteSet[rowCount][];
  }

  public String toString() {
    return "JoinRequest(" + (handle != null ? handle.getNodeId() : null) + ","
        + (joinHandle != null ? joinHandle.getNodeId() : null) +","+timestamp+ ")";
  }

  /***************** Raw Serialization ***************************************/  
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {    
//    buf.writeByte((byte)0); // version
    
    // version 1
    buf.writeByte((byte)1); // version
    buf.writeLong(timestamp);
    
    buf.writeByte((byte) rtBaseBitLength);    
    handle.serialize(buf);
    if (joinHandle != null) {
      buf.writeBoolean(true);
      joinHandle.serialize(buf);
    } else {
      buf.writeBoolean(false);
    }
    
    // encode the table
    buf.writeShort((short) rowCount);
    int maxIndex = Id.IdBitLength / rtBaseBitLength;
    for (int i=0; i<maxIndex; i++) {
      RouteSet[] thisRow = rows[i];
      if (thisRow != null) {
        buf.writeBoolean(true);
        for (int j=0; j<thisRow.length; j++) {
          if (thisRow[j] != null) {
            buf.writeBoolean(true);
            thisRow[j].serialize(buf);
          } else {
            buf.writeBoolean(false);
          }
        }
      } else {
        buf.writeBoolean(false);
      }
    }
    
    if (leafSet != null) {
      buf.writeBoolean(true);
      leafSet.serialize(buf);
    } else {
      buf.writeBoolean(false);
    }
    
  }

  public JoinRequest(InputBuffer buf, NodeHandleFactory nhf, NodeHandle sender, PastryNode localNode) throws IOException {
    super(JoinAddress.getCode());
    
    byte version = buf.readByte();
    switch(version) {
      case 1:
        timestamp = buf.readLong();
      case 0:
        setSender(sender);
        rtBaseBitLength = buf.readByte();
        initialize(rtBaseBitLength);
        
        handle = nhf.readNodeHandle(buf);
        if (buf.readBoolean())
          joinHandle = nhf.readNodeHandle(buf);
    
        rowCount = buf.readShort();
        int numRows = Id.IdBitLength / rtBaseBitLength;
        int numCols = 1 << rtBaseBitLength;
        for (int i=0; i<numRows; i++) {
          RouteSet[] thisRow;
          if (buf.readBoolean()) {
            thisRow = new RouteSet[numCols];
            for (int j=0; j<numCols; j++) {
              if (buf.readBoolean()) {
                thisRow[j] = new RouteSet(buf, nhf, localNode);
              } else {
                thisRow[j] = null;
              }
            }
          } else {
            thisRow = null;
          }
          rows[i] = thisRow;
        }
        
        if (buf.readBoolean())
          leafSet = LeafSet.build(buf, nhf);
        break;        
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
}

