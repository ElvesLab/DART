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
import rice.environment.Environment;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.past.*;
import rice.p2p.past.messaging.*;

/**
 * @(#) GCCollectMessage.java
 *
 * This class represents a message which tells GC Past that it's time to
 * delete all the expired objects in the local store.
 *
 * @version $Id: GCCollectMessage.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public class GCCollectMessage extends PastMessage {
  public static final short TYPE = 8;

  /**
   * Constructor
   *
   * @param id The location to be stored
   * @param source The source address
   * @param dest The destination address
   */
  public GCCollectMessage(int id, NodeHandle source, Id dest) {
    super(id, source, dest);
  }

  /**
   * Method by which this message is supposed to return it's response -
   * in this case, it lets the continuation know that a the message was
   * lost via the receiveException method.
   *
   * @param c The continuation to return the reponse to.
   */
  public void returnResponse(Continuation c, Environment env, String instance) {
    c.receiveException(new PastException("Should not be called!"));
  }
  
  /**
  * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  public String toString() {
    return "[GCCollectMessage]";
  }
  
  public short getType() {
    return TYPE; 
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    throw new RuntimeException("serialize() not supported in MessageLostMessage"); 
  }
}

