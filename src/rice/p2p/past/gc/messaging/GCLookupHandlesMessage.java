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
import rice.p2p.past.gc.*;

/**
 * @(#) GCLookupHandlesMessage.java
 *
 * This class represents a message which is an request to find the leafset of
 * a remote node in the GC Past.
 *
 * @version $Id: GCLookupHandlesMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class GCLookupHandlesMessage extends LookupHandlesMessage {
  public static final short TYPE = 10;

  /**
   * Constructor
   *
   * @param uid The unique id
   * @param id The location to be stored
   * @param source The source address
   * @param dest The destination address
   */
  public GCLookupHandlesMessage(int uid, Id id, NodeHandle source, Id dest) {
    super(uid, id, Integer.MAX_VALUE, source, dest);
  }

  /**
    * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  public String toString() {
    return "[GCLookupHandlesMessage for " + getId() + "]";
  }
  
  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version        
    serializeHelper(buf);
  }
  
  public static GCLookupHandlesMessage buildGC(InputBuffer buf, Endpoint endpoint) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new GCLookupHandlesMessage(buf, endpoint);
      default:
        throw new IOException("Unknown Version: "+version);        
    }
  }  
  
 private GCLookupHandlesMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
    super(buf, endpoint);
  }  
}

