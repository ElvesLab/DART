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

package rice.p2p.replication.messaging;

import java.io.IOException;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.replication.*;

/**
 * @(#) ReaponseMessage.java
 *
 * This class represents a response for a set of keys in the replication
 * system.
 *
 * @version $Id: ResponseMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class ResponseMessage extends ReplicationMessage {
  public static final short TYPE = 3;

  // the list of ranges for this message
  protected IdRange[] ranges;
  
  // the list of keys for this message
  protected Id[][] ids;
  
  /**
  * Constructor which takes a unique integer Id
   *
   * @param source The source address
   * @param topic The topic
   */
  public ResponseMessage(NodeHandle source, IdRange[] ranges, IdSet[] setA) {
    super(source);
    
    this.ranges = ranges;
    ids = new Id[setA.length][];
    for (int i = 0; i < setA.length; i++) {
      ids[i] = setA[i].asArray();
    }
  }
  
  /**
   * Method which returns this messages' ranges
   *
   * @return The ranges of this message
   */
  public IdRange[] getRanges() {
    return ranges;
  }
  
  /**
   * Method which returns this messages' ranges
   *
   * @return The ranges of this message
   */
  public Id[][] getIdSets() {
    return ids;
  }
  
  public String toString() {
    int numResponses = 0;
    for (int a = 0; a < ids.length; a++) {
      numResponses+=ids[a].length;
    }
    return "ResponseMessage("+getSource()+"):"+numResponses; 
  }
  

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version
    super.serialize(buf);
    // encode the ids table
    // note, can uncomment thes lines if this array turns out to be non-full
    buf.writeInt(ids.length);
    for (int i=0; i<ids.length; i++) {
      Id[] thisRow = ids[i];
//      if (thisRow != null) {
//        buf.writeByte((byte)1);
        buf.writeInt(thisRow.length);
        for (int j=0; j<thisRow.length; j++) {
//          if (thisRow[j] != null) {
//            buf.writeByte((byte)1);
            buf.writeShort(thisRow[j].getType());
            thisRow[j].serialize(buf);
//          } else {
//            buf.writeByte((byte)0);
//          }
        }
//      } else {
//        buf.writeByte((byte)0);
//      }
    }
    
    // ranges
    buf.writeInt(ranges.length);
    for (int i = 0; i < ranges.length; i++) {
      ranges[i].serialize(buf); 
    }    
  }
  
  public static ResponseMessage build(InputBuffer buf, Endpoint endpoint) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new ResponseMessage(buf, endpoint);
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
    
  private ResponseMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
    super(buf, endpoint);
    
    // decode ids
    ids = new Id[buf.readInt()][];
    for (int i=0; i<ids.length; i++) {
      ids[i] = new Id[buf.readInt()];
      for (int j = 0; j<ids[i].length;j++) {
        ids[i][j] = endpoint.readId(buf, buf.readShort());
      }
    }
    
    // decode ranges
    ranges = new IdRange[buf.readInt()];
    for (int i = 0; i < ranges.length; i++) {
      ranges[i] = endpoint.readIdRange(buf);
    }    
  }
}

