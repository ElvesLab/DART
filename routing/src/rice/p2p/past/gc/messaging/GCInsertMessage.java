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

package rice.p2p.past.gc.messaging;

import java.io.IOException;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.past.*;
import rice.p2p.past.messaging.*;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.gc.*;

/**
 * @(#) GCInsertMessage.java
 *
 * This class represents a message which is an insert request in past, 
 * coupled with an expiration time for the object.
 *
 * @version $Id: GCInsertMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class GCInsertMessage extends InsertMessage {
  public static final short TYPE = 9;

  // the timestamp at which the object expires
  protected long expiration;
  
  /**
   * Constructor which takes a unique integer Id, as well as the
   * data to be stored
   *
   * @param uid The unique id
   * @param content The content to be inserted
   * @param expiration The expiration time
   * @param source The source address
   * @param dest The destination address
   */
  public GCInsertMessage(int uid, PastContent content, long expiration, NodeHandle source, Id dest) {
    super(uid, content, source, dest);

    this.expiration = expiration;
  }

  /**
   * Method which returns the expiration time
   *
   * @return The contained expiration time
   */
  public long getExpiration() {
    return expiration;
  }

  /**
    * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  public String toString() {
    return "[GCInsertMessage for " + content + " exp " + expiration + "]";
  }

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version        
    super.serializeHelper(buf);
    buf.writeLong(expiration);
  }
  
  public static GCInsertMessage buildGC(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new GCInsertMessage(buf, endpoint, pcd);
      default:
        throw new IOException("Unknown Version: "+version);        
    }
  }  
  
  private GCInsertMessage(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
    super(buf, endpoint, pcd);
    expiration = buf.readLong();
  }
}

