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

/**
 * @(#) Message.java
 *
 * This interface is an abstraction of a message which is sent through
 * the common API-based system.
 *
 * @version $Id: Message.java 3647 2007-03-13 13:50:26Z jeffh $
 *
 * @author Alan Mislove
 * @author Peter Druschel
 */
public interface Message extends Serializable {
  
  // different priority levels
  public static final int MAX_PRIORITY = -15;
  public static final int HIGH_PRIORITY = -10;
  public static final int MEDIUM_HIGH_PRIORITY = -5;
  public static final int MEDIUM_PRIORITY = 0;
  public static final int MEDIUM_LOW_PRIORITY = 5;
  public static final int LOW_PRIORITY = 10;
  public static final int LOWEST_PRIORITY = 15;
  public static final int DEFAULT_PRIORITY = MEDIUM_PRIORITY;

  /**
   * Method which should return the priority level of this message.  The messages
   * can range in priority from -15 (highest priority) to 15 (lowest) -
   * when sending messages across the wire, the queue is sorted by message priority.
   * If the queue reaches its limit, the lowest priority messages are discarded.  Thus,
   * applications which are very verbose should have LOW_PRIORITY or lower, and
   * applications which are somewhat quiet are allowed to have MEDIUM_PRIORITY or
   * possibly even HIGH_PRIORITY.
   *
   * @return This message's priority
   */
  public int getPriority();
  
}


