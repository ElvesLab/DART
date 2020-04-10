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
/*
 * Created on Apr 13, 2005
 */
package rice.pastry.standard;

import java.io.IOException;
import java.util.*;

import rice.p2p.commonapi.rawserialization.*;
import rice.pastry.*;
import rice.pastry.join.JoinAddress;
import rice.pastry.leafset.LeafSet;
import rice.pastry.messaging.*;

/**
 * @author Jeff Hoye
 */
public class ConsistentJoinMsg extends PRawMessage {
  private static final long serialVersionUID = -8942404626084999673L;
  
  public static final short TYPE = 2;
  
  LeafSet ls;
  boolean request;
  HashSet<NodeHandle> failed;
  
  /**
   * 
   */
  public ConsistentJoinMsg(LeafSet ls, HashSet<NodeHandle> failed, boolean request) {
    super(JoinAddress.getCode());
    this.ls = ls;
    this.request = request;
    this.failed = failed;
  }
  
  public String toString() {
    return "ConsistentJoinMsg "+ls+" request:"+request; 
  }
  
  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {    
    buf.writeByte((byte)0); // version
    ls.serialize(buf);
    buf.writeBoolean(request);
    buf.writeInt(failed.size());
    Iterator<NodeHandle> i = failed.iterator();
    while(i.hasNext()) {
      NodeHandle h = (NodeHandle)i.next(); 
      h.serialize(buf);
    }
  }
  
  public ConsistentJoinMsg(InputBuffer buf, NodeHandleFactory nhf, NodeHandle sender) throws IOException {
    super(JoinAddress.getCode());    
    byte version = buf.readByte();
    switch(version) {
      case 0:
        setSender(sender);
        ls = LeafSet.build(buf, nhf);
        request = buf.readBoolean();
        failed = new HashSet<NodeHandle>();
        int numInSet = buf.readInt();
        for (int i = 0; i < numInSet; i++) {
          failed.add(nhf.readNodeHandle(buf));
        }
        break;
      default:
        throw new IOException("Unknown Version: "+version);
    }      
  }
}
