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
package rice.pastry.leafset;

import rice.p2p.commonapi.rawserialization.*;
import rice.pastry.*;
import rice.pastry.messaging.*;

import java.io.*;
import java.util.*;

/**
 * Broadcast a leaf set to another node.
 * 
 * @version $Id: BroadcastLeafSet.java 3855 2007-09-14 15:46:49Z jeffh $
 * 
 * @author Andrew Ladd
 */

public class BroadcastLeafSet extends PRawMessage {
  public static final short TYPE = 2;
  
  public static final int Update = 0;

  public static final int JoinInitial = 1;

  public static final int JoinAdvertise = 2;

  public static final int Correction = 3;

  private NodeHandle fromNode;

  private LeafSet theLeafSet;

  private int theType;

  private long requestTimeStamp;
  
  /**
   * Constructor.
   */

  public BroadcastLeafSet(NodeHandle from, LeafSet leafSet, int type, long requestTimeStamp) {
    this(null, from, leafSet, type, requestTimeStamp);
  }

  /**
   * Constructor.
   * 
   * @param stamp the timestamp
   */

  public BroadcastLeafSet(Date stamp, NodeHandle from, LeafSet leafSet, int type, long requestTimeStamp) {
    super(LeafSetProtocolAddress.getCode(), stamp);

    if (leafSet == null) throw new IllegalArgumentException("Leafset is null");
    
    fromNode = from;
    theLeafSet = leafSet.copy();
    theType = type;
    this.requestTimeStamp = requestTimeStamp;
    setPriority(MAX_PRIORITY);
  }

  /**
   * Returns the node id of the node that broadcast its leaf set.
   * 
   * @return the node id.
   */

  public NodeHandle from() {
    return fromNode;
  }

  /**
   * Returns the leaf set that was broadcast.
   * 
   * @return the leaf set.
   */

  public LeafSet leafSet() {
    return theLeafSet;
  }

  /**
   * Returns the type of leaf set.
   * 
   * @return the type.
   */

  public int type() {
    return theType;
  }

  public String toString() {
    String s = "BroadcastLeafSet("+theLeafSet+","+requestTimeStamp+")";
    return s;
  }
  
  /***************** Raw Serialization ***************************************/  
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version
    fromNode.serialize(buf);
    theLeafSet.serialize(buf);
    buf.writeByte((byte) theType);
    buf.writeLong(requestTimeStamp);
  }
  
  public BroadcastLeafSet(InputBuffer buf, NodeHandleFactory nhf) throws IOException {
    super(LeafSetProtocolAddress.getCode());
    
    byte version = buf.readByte();
    switch(version) {
      case 0:
        fromNode = nhf.readNodeHandle(buf);
        theLeafSet = LeafSet.build(buf, nhf);
        theType = buf.readByte();
        requestTimeStamp = buf.readLong();
        break;
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }

  public long getTimeStamp() {
    return requestTimeStamp;
  }  
}