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
package rice.pastry;

import rice.environment.logging.Logger;
import rice.pastry.messaging.Message;
import rice.selector.TimerTask;

/**
 * A class that represents scheduled message events
 * 
 * @version $Id: ScheduledMessage.java 2808 2005-11-22 15:38:49 +0100 (Tue, 22
 *          Nov 2005) jeffh $
 * 
 * @author Peter Druschel
 */
public class ScheduledMessage extends TimerTask {
  protected PastryNode localNode;

  protected Message msg;

  /**
   * Constructor
   * 
   * @param the
   *          message
   */
  public ScheduledMessage(PastryNode pn, Message msg) {
    localNode = pn;
    this.msg = msg;
  }

  /**
   * Returns the message
   * 
   * @return the message
   */
  public Message getMessage() {
    return msg;
  }

  public PastryNode getLocalNode() {
    return localNode;
  }

  /**
   * deliver the message
   */
  public void run() {
    try {
      // timing with cancellation
      Message m = msg;
      if (m != null) localNode.receiveMessage(msg);
    } catch (Exception e) {
      Logger logger = localNode.getEnvironment().getLogManager().getLogger(getClass(), null);
      if (logger.level <= Logger.WARNING) logger.logException("Delivering " + this + " caused exception ", e);
    }
  }

  public String toString() {
    return "SchedMsg for " + msg;
  }

  /*
   * (non-Javadoc)
   * 
   * @see rice.p2p.commonapi.CancellableTask#cancel()
   */
  public boolean cancel() {
    // memory management
    msg = null;
    localNode = null;
    return super.cancel();
  }

}
