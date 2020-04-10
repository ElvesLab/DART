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

import java.io.IOException;
import java.util.Map;

import org.mpisws.p2p.transport.direct.Delivery;
import org.mpisws.p2p.transport.direct.DirectTransportLayer;

import rice.environment.logging.Logger;

  /**
   * 
   * @version $Id: EuclideanNetwork.java 2561 2005-06-09 16:22:02Z jeffh $
   * @author amislove
   */
  class MessageDelivery<Identifier, MessageType> implements Delivery {
    protected MessageType msg;
    protected Identifier node;    
    protected Logger logger;
    protected int seq;
    BasicNetworkSimulator<Identifier, MessageType> networkSimulator;
    Identifier from;
    protected Map<String, Object> options;
    
    /**
     * Constructor for MessageDelivery.
     */
    public MessageDelivery(MessageType m, Identifier to, Identifier from, Map<String, Object> options, BasicNetworkSimulator<Identifier, MessageType> sim) {
      logger = ((DirectTransportLayer<Identifier, MessageType>)sim.getTL(to)).getLogger();
//      if (m instanceof rice.pastry.routing.RouteMessage) {
//        rice.pastry.routing.RouteMessage m1 = (rice.pastry.routing.RouteMessage)m;
//        rice.p2p.commonapi.Message m2 = m1.unwrap();
//        if (m2 instanceof PastryEndpointMessage) {
//          PastryEndpointMessage m3 = (PastryEndpointMessage)m2;
//          rice.p2p.commonapi.Message m4 = m3.getMessage();
//          if (m4 instanceof rice.p2p.scribe.messaging.SubscribeMessage) {
//            rice.p2p.scribe.messaging.SubscribeMessage m5 = (rice.p2p.scribe.messaging.SubscribeMessage)m4;
//            if (m5.getTopic() == null) {
//              logger.logException("MD.ctor("+m+" to "+to+" from:"+from+")",m5.ctor);
//              throw new RuntimeException("bad");
//            }
//          }
//        }
//      }
      msg = m;
      node = to;
      this.options = options;
      this.from = from;
      this.networkSimulator = sim;
      this.seq = ((DirectTransportLayer<Identifier, MessageType>)sim.getTL(to)).getNextSeq();
      
      // Note: this is done to reduce memory thrashing.  There are a ton of strings created
      // in getLogger(), and this is a really temporary object.
      logger = ((DirectTransportLayer<Identifier, MessageType>)sim.getTL(to)).getLogger();
//      logger = pn.getEnvironment().getLogManager().getLogger(MessageDelivery.class, null);
    }

    public void deliver() {
      if (logger.level <= Logger.FINE) logger.log("MD: deliver "+msg+" to "+node);
      try {
        DirectTransportLayer<Identifier, MessageType> tl = networkSimulator.getTL(node);
        if (tl != null) {
          networkSimulator.notifySimulatorListenersReceived(msg, from, node);
          tl.incomingMessage(from, msg, options);
        } else {
          if (logger.level <= Logger.WARNING) logger.log("Message "+msg+" dropped because destination "+node+" is dead.");
          // Notify sender? tough decision, this would be lost in the network, but over tcp, this would be noticed
          // maybe notify sender if tcp?
        }
      } catch (IOException ioe) {
        if (logger.level <= Logger.WARNING) logger.logException("Error delivering message "+this, ioe);
      }
//      networkSimulator.notifySimulatorListenersReceived(msg, from, node);
      
//      if (isAlive(msg.getSenderId())) {
//        environment.getLogManager().getLogger(EuclideanNetwork.class, null).log(Logger.FINER, 
//            "delivering "+msg+" to " + node);
//        node.receiveMessage(msg);
//      } else {
//        environment.getLogManager().getLogger(EuclideanNetwork.class, null).log(Logger.INFO, 
//            "Cant deliver "+msg+" to " + node + "because it is not alive.");        
//      }
    } 
    
    public int getSeq() {
      return seq; 
    }
    
    public String toString() {
      return "MD["+msg+":"+from+"=>"+node+":"+seq+"]";
    }
  }