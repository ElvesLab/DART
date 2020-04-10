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

import java.util.*;

import rice.Destructable;
import rice.environment.logging.Logger;
import rice.pastry.*;
import rice.pastry.client.PastryAppl;
import rice.pastry.transport.Deserializer;

/**
 * An object which remembers the mapping from names to MessageReceivers
 * and dispatches messages by request.
 * 
 * For consistent routing, modified to only deliver messages to applications 
 * if the PastryNode.isReady().  It will still deliver messages to any non-PastryAppl
 * because these "services" may be needed to boot the node into the ring.  Any 
 * messages to a PastryAppl will be buffered until the node goes ready.
 * 
 * TODO:  We need to make it explicit which apps can receive messages before
 * PastryNode.isReady().
 * 
 * @version $Id: MessageDispatch.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Jeff Hoye
 * @author Andrew Ladd
 */

public class MessageDispatch implements Destructable {

  // have modified from HashMap to HashMap to use the internal representation
  // of a LocalAddress.  Otherwise remote node cannot get its message delivered
  // because objects constructed differently are not mapped to the same value
  private HashMap<Integer,PastryAppl> addressBook;

  protected PastryNode localNode;
  
  protected Logger logger;
  
  /**
   * Also held by the transport layer to allow it to deserialize the messages.
   */
  protected Deserializer deserializer;
  
  /**
   * Constructor.
   */
  public MessageDispatch(PastryNode pn, Deserializer deserializer) {
    this.deserializer = deserializer;
    addressBook = new HashMap<Integer, PastryAppl>();
    this.localNode = pn;
    this.logger = pn.getEnvironment().getLogManager().getLogger(getClass(), null);    
  }

  /**
   * Registers a receiver with the mail service.
   *
   * @param name a name for a receiver.
   * @param receiver the receiver.
   */
  public void registerReceiver(int address, PastryAppl receiver) {
    // the stack trace is to figure out who registered for what, it is not an error
    

    
    if (logger.level <= Logger.FINE) logger.log(
        "Registering "+receiver+" for address " + address);
    if (logger.level <= Logger.FINEST) logger.logException(
        "Registering receiver for address " + address, new Exception("stack trace"));
    if (addressBook.get(Integer.valueOf(address)) != null) {
      throw new IllegalArgumentException("Registering receiver for already-registered address " + address);
//      if (logger.level <= Logger.SEVERE) logger.logException(
//          "ERROR - Registering receiver for already-registered address " + address, new Exception("stack trace"));
    }

    deserializer.setDeserializer(address, receiver.getDeserializer());
    addressBook.put(Integer.valueOf(address), receiver);
  }
  
  public PastryAppl getDestination(Message msg) {
    return getDestinationByAddress(msg.getDestination());    
  }

  public PastryAppl getDestinationByAddress(int addr) {
    PastryAppl mr = (PastryAppl) addressBook.get(Integer.valueOf(addr));    
    return mr;
  }

  /**
   * Dispatches a message to the appropriate receiver.
   * 
   * It will buffer the message under the following conditions:
   *   1) The MessageReceiver is not yet registered.
   *   2) The MessageReceiver is a PastryAppl, and localNode.isReady() == false
   *
   * @param msg the message.
   *
   * @return true if message could be dispatched, false otherwise.
   */
  public boolean dispatchMessage(Message msg) {
    if (msg.getDestination() == 0) {
      Logger logger = localNode.getEnvironment().getLogManager().getLogger(MessageDispatch.class, null);
      if (logger.level <= Logger.WARNING) logger.logException(
          "Message "+msg+","+msg.getClass().getName()+" has no destination.", new Exception("Stack Trace"));
      return false;
    }
    // NOTE: There is no safety issue with calling localNode.isReady() because this is on the 
    // PastryThread, and the only way to set a node ready is also on the ready thread.
    PastryAppl mr = (PastryAppl) addressBook.get(Integer.valueOf(msg.getDestination()));

    if (mr == null) {
      if ((logger.level <= Logger.FINE) ||
          (localNode.isReady() && (logger.level <= Logger.INFO))) { 
        logger.log(
          "Dropping message " + msg + " because the application address " + msg.getDestination() + " is unknown.");
      }
      return false;
    } else {
      mr.receiveMessage(msg); 
      return true;
    }
  }  
  
//  public boolean dispatchMessage(RawMessageDelivery msg) {
//    if (msg.getAddress() == 0) {
//      Logger logger = localNode.getEnvironment().getLogManager().getLogger(MessageDispatch.class, null);
//      if (logger.level <= Logger.WARNING) logger.logException(
//          "Message "+msg+","+msg.getClass().getName()+" has no destination.", new Exception("Stack Trace"));
//      return false;
//    }
//    // NOTE: There is no safety issue with calling localNode.isReady() because this is on the 
//    // PastryThread, and the only way to set a node ready is also on the ready thread.
//    PastryAppl mr = (PastryAppl) addressBook.get(Integer.valueOf(msg.getAddress()));
//
//    if (mr == null) {
//      if (logger.level <= Logger.WARNING) logger.log(
//          "Dropping message " + msg + " because the application address " + msg.getAddress() + " is unknown.");
//      return false;
//    } else {
//      mr.receiveMessageInternal(msg); 
//      return true;
//    }
//  }  
  
  public void destroy() {
    Iterator<PastryAppl> i = addressBook.values().iterator();
    while(i.hasNext()) {
      PastryAppl mr = i.next();
      if (logger.level <= Logger.INFO) logger.log("Destroying "+mr);
      mr.destroy(); 
    }      
    addressBook.clear();
  }
}
