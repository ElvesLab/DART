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
 * Created on Apr 4, 2006
 */
package rice.p2p.aggregation.messaging;

import java.io.IOException;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.past.*;
import rice.p2p.past.rawserialization.*;

/**
 * Just wraps a header in Past to know that it is something other than an Aggregate
 * 
 * @author Jeff Hoye
 */
public class NonAggregate implements RawPastContent {
  public static final short TYPE = 2;
  
  public RawPastContent content;

  public NonAggregate(PastContent content) {
    this(content instanceof RawPastContent ? (RawPastContent)content : new JavaSerializedPastContent(content));
  }

  public NonAggregate(RawPastContent subContent) {
    this.content = subContent;
  }
  
  public PastContent checkInsert(Id id, PastContent existingContent) throws PastException {
    content = (RawPastContent)content.checkInsert(id, ((NonAggregate)existingContent).content);
    return this;
  }

  public PastContentHandle getHandle(Past local) {
    return content.getHandle(local);
  }

  public Id getId() {
    return content.getId();
  }

  public boolean isMutable() {
    return content.isMutable();
  }
  
  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version        
    buf.writeShort(content.getType());
    content.serialize(buf);
  }
  
  public NonAggregate(InputBuffer buf, Endpoint endpoint, RawPastContent subContent, PastContentDeserializer pcd) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        short subType = buf.readShort();
        PastContent temp = pcd.deserializePastContent(buf, endpoint, subType);
        if (subType == 0) {
          this.content = new JavaSerializedPastContent(temp);
        } else {
          this.content = (RawPastContent)temp; 
        }
        break;
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
}
