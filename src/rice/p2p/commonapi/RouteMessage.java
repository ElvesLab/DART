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

package rice.p2p.commonapi;

import java.io.*;

import rice.p2p.commonapi.rawserialization.*;

/**
 * @(#) RouteMessage.java
 *
 * This interface is a container which represents a message, as it is
 * about to be forwarded to another node.
 *
 * @version $Id: RouteMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public interface RouteMessage extends Serializable {

  /**
   * Returns the destination Id for this message
   *
   * @return The destination Id
   */
  public Id getDestinationId();

  /**
   * Returns the next hop handle for this message
   *
   * @return The next hop
   */
  public NodeHandle getNextHopHandle();

  /**
   * Returns the enclosed message inside of this message
   *
   * @return The enclosed message
   * @deprecated use getMesage(MessageDeserializer)
   */
  public Message getMessage();
  
  public Message getMessage(MessageDeserializer md) throws IOException;

  /**
   * Sets the destination Id for this message
   *
   * @param id The destination Id
   */
  public void setDestinationId(Id id);

  /**
   * Sets the next hop handle for this message
   *
   * @param nextHop The next hop for this handle
   */
  public void setNextHopHandle(NodeHandle nextHop);

  /**
   * Sets the internal message for this message
   *
   * @param message The internal message
   */
  public void setMessage(Message message);
  
  /**
   * Sets the internal message for this message
   *
   * Does the same as setMessage(Message) but with better
   * performance, because it doesn't have to introspect 
   * if the message is a RawMessage
   *
   * @param message The internal message
   */
  public void setMessage(RawMessage message);
  
}


