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
package rice.pastry.direct;


import java.util.Hashtable;
import java.util.Map;

import org.mpisws.p2p.transport.SocketRequestHandle;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.pastry.Id;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.ReadyStrategy;
import rice.pastry.ScheduledMessage;
import rice.pastry.client.PastryAppl;
import rice.pastry.join.InitiateJoin;
import rice.pastry.messaging.Message;
import rice.pastry.routing.RouteMessage;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;
import rice.selector.SelectorManager;
import rice.selector.Timer;

/**
 * Direct pastry node. Subclasses PastryNode, and does about nothing else.
 * 
 * @version $Id: DirectPastryNode.java 4221 2008-05-19 16:41:19Z jeffh $
 * 
 * @author Sitaram Iyer
 */

public class DirectPastryNode {
  /**
   * Used for proximity calculation of DirectNodeHandle. This will probably go
   * away when we switch to a byte-level protocol.
   */
  static private Hashtable<Thread, PastryNode> currentNode = new Hashtable<Thread, PastryNode>();
  
  /**
   * Returns the previous one.
   * 
   * @param dnh
   * @return
   */
  public static synchronized PastryNode setCurrentNode(PastryNode dpn) {
    Thread current = Thread.currentThread();
    PastryNode ret = currentNode.get(current);
    if (dpn == null) {
      currentNode.remove(current);
    } else {
      currentNode.put(current, dpn);
    } 
    return ret;
  }
  
  public static synchronized PastryNode getCurrentNode() {
    Thread current = Thread.currentThread();
    PastryNode ret = currentNode.get(current);
    return ret;    
  }  
}

