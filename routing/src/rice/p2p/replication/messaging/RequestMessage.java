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
import rice.p2p.util.*;

/**
 * @(#) RequestMessage.java
 *
 * This class represents a request for a set of keys in the replication
 * system.
 *
 * @version $Id: RequestMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class RequestMessage extends ReplicationMessage {
  public static final short TYPE = 2;

  // the list of ranges for this message
  protected IdRange[] ranges;
  
  // the list of hashes for this message
  protected IdBloomFilter[] filters;
  
  /**
   * Constructor which takes a unique integer Id
   *
   * @param source The source address
   * @param topic The topic
   */
  public RequestMessage(NodeHandle source, IdRange[] ranges, IdBloomFilter[] filters) {
    super(source);
    
    this.ranges = ranges;
    this.filters = filters;
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
   * Method which returns this messages' bloom filters
   *
   * @return The bloom filters of this message
   */
  public IdBloomFilter[] getFilters() {
    return filters;
  }

  public String toString() {
    return "RequestMessage("+getSource()+"):"+(ranges == null?null:ranges.length); 
  }

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version
    super.serialize(buf);

    buf.writeInt(filters.length);
    for (int i = 0; i < filters.length; i++) {
      filters[i].serialize(buf); 
    }

    buf.writeInt(ranges.length);
    for (int i = 0; i < ranges.length; i++) {
      ranges[i].serialize(buf); 
    }
  }
  
  public static RequestMessage build(InputBuffer buf, Endpoint endpoint) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new RequestMessage(buf, endpoint);
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
    
  private RequestMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
    super(buf, endpoint);
    
    filters = new IdBloomFilter[buf.readInt()];
    for (int i = 0; i < filters.length; i++) {
      filters[i] = new IdBloomFilter(buf);
    }
    
    ranges = new IdRange[buf.readInt()];
    for (int i = 0; i < ranges.length; i++) {
      ranges[i] = endpoint.readIdRange(buf);
    }    
  }
}

 