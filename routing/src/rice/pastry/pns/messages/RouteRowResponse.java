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
package rice.pastry.pns.messages;

import java.io.IOException;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.pastry.NodeHandle;
import rice.pastry.NodeHandleFactory;
import rice.pastry.PastryNode;
import rice.pastry.messaging.Message;
import rice.pastry.messaging.PRawMessage;
import rice.pastry.routing.RouteSet;

public class RouteRowResponse extends PRawMessage {

  public static final short TYPE = 4;
  public short index;
  public RouteSet[] row;

  public RouteRowResponse(NodeHandle sender, short index, RouteSet[] row, int address) {
    super(address);
    if (sender == null) throw new IllegalArgumentException("sender == null!");
    setSender(sender);    
    this.index = index;
    this.row = row;
    setPriority(HIGH_PRIORITY);
  }
  
  public String toString() {
    return "RRresp["+index+"]:"+getSender();
  }

  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version    
    buf.writeShort(index);
    buf.writeInt(row.length);
    for (int i = 0; i < row.length; i++) {
      if (row[i] == null) {
        buf.writeBoolean(false);
      } else {
        buf.writeBoolean(true);
        row[i].serialize(buf); 
      }
    }    
  }

  public RouteRowResponse(InputBuffer buf, PastryNode localNode, NodeHandle sender, int dest) throws IOException {
    super(dest);
    byte version = buf.readByte();
    switch(version) {
      case 0:
        setSender(sender);
        index = buf.readShort();
        int numRouteSets = buf.readInt();
        row = new RouteSet[numRouteSets];
        for (int i = 0; i<numRouteSets; i++) {      
          if (buf.readBoolean()) {
            row[i] = new RouteSet(buf, localNode, localNode); 
          }
        }
        break;
      default:
        throw new IOException("Unknown Version: "+version);
    }     
  }

  public short getType() {
    return TYPE;
  }

}
