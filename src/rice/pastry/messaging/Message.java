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
package rice.pastry.messaging;

import java.io.*;
import java.util.*;

import rice.pastry.*;

/**
 * This is an abstract implementation of a message object.
 * 
 * @version $Id: Message.java 3818 2007-08-02 13:08:10Z jeffh $
 * 
 * @author Andrew Ladd
 * @author Sitaram Iyer
 */

public abstract class Message implements Serializable, rice.p2p.commonapi.Message {
  private static final long serialVersionUID = 8921944904321235696L;

  public static final int DEFAULT_PRIORITY_LEVEL = rice.p2p.commonapi.Message.MEDIUM_PRIORITY;

  private int destination;

  private NodeHandle sender;

  /**
   * Potentially needed for reverse compatability with serialization?  
   * Remove this when move to byte-level protocol.
   */
  private boolean priority;

  private int priorityLevel = DEFAULT_PRIORITY_LEVEL;

  private transient Date theStamp;

  /**
   * Constructor.
   * 
   * @param dest the destination.
   */

  public Message(int dest) {
    this(dest, null);
  }

  /**
   * Constructor.
   * 
   * @param dest the destination.
   * @param timestamp the timestamp
   */

  public Message(int dest, Date timestamp) {
    if (dest == 0) throw new IllegalArgumentException("dest must != 0");
    destination = dest;
    this.theStamp = timestamp;
    sender = null;
    priority = false;
  }
  /**
   * Gets the address of message receiver that the message is for.
   * 
   * @return the destination id.
   */
  public int getDestination() {
    return destination;
  }

  /**
   * Gets the timestamp of the message, if it exists.
   * 
   * @return a timestamp or null if the sender did not specify one.
   */
  public Date getDate() {
    return theStamp;
  }

  /**
   * Get sender Id.
   * 
   * @return the immediate sender's NodeId.
   */
  public Id getSenderId() {
    if (sender == null)
      return null;
    return sender.getNodeId();
  }

  /**
   * Get sender.
   * 
   * @return the immediate sender's NodeId.
   */
  public NodeHandle getSender() {
    return sender;
  }

  /**
   * Set sender Id. Called by NodeHandle just before dispatch, so that this Id
   * is guaranteed to belong to the immediate sender.
   * 
   * @param the immediate sender's NodeId.
   */
  public void setSender(NodeHandle nh) {
    sender = nh;
  }

  /**
   * Get priority
   * 
   * @return the priority of this message.
   */
  public int getPriority() {
    return priorityLevel;
  }

  /**
   * Set priority.
   * 
   * @param the new priority.
   */
  protected void setPriority(int prio) {
    priorityLevel = prio;
  }

  /**
   * If the message has no timestamp, this will stamp the message.
   * 
   * @param time the timestamp.
   * 
   * @return true if the message was stamped, false if the message already had a
   *         timestamp.
   */
  public boolean stamp(Date time) {
    if (theStamp == null) {
      theStamp = time;
      return true;
    } else
      return false;
  }

}
