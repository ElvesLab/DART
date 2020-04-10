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
package rice.pastry.transport;

import java.util.Map;

import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;

import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.pastry.NodeHandle;
import rice.pastry.messaging.Message;
import rice.pastry.socket.TransportLayerNodeHandle;

public class PMessageReceiptImpl implements PMessageReceipt {
  MessageRequestHandle<NodeHandle, RawMessage> internal;
  Message message;
  Map<String, Object> options;
  boolean cancelled = false;
  
  public PMessageReceiptImpl(Message msg, Map<String, Object> options) {
    this.message = msg;
    this.options = options;
  }

  public NodeHandle getIdentifier() {
    if (internal == null) return null;
    return (NodeHandle)internal.getIdentifier();
  }

  public Message getMessage() {
    return message;
  }

  public Map<String, Object> getOptions() {
    return options;
//    return internal.getOptions();
  }

  /**
   * The synchronization code here must do the following:
   * 
   * cancel/setInternal can be called on any thread at any time
   * if both cancel and setInternal are called, then internal.cancel() is called the first time the second call is made.
   * cannot hold a lock while calling internal.cancel()
   * 
   * can cancel be called off of the selector?
   * 
   * @return true if it has been cancelled for sure, false if it may/may-not be cancelled
   */
  public boolean cancel() {    
    boolean callCancel = false;
    synchronized(this) {
      if (cancelled) return false;
      cancelled = true;
      if (internal != null) callCancel = true;
    }
    if (callCancel) return internal.cancel();
    return false;
  }

  /**
   * See synchronization note on cancel()
   * 
   * @param name
   */
//  Exception setInternalStack; // delme
  public void setInternal(MessageRequestHandle<NodeHandle, RawMessage> name) {
    boolean callCancel = false;
    synchronized(this) {      
      if (internal != null && internal != name) {
//        setInternalStack.printStackTrace();
        throw new RuntimeException("Internal already set old:"+internal+" new:"+name);
      }
      internal = name;
//      setInternalStack = new Exception();
      callCancel = cancelled;
    }
    if (callCancel) internal.cancel();
  }
  
  public MessageRequestHandle<NodeHandle, RawMessage> getInternal() {
    return internal;
  }
  
  public boolean isCancelled() {
    return cancelled;
  }
  
  public String toString() {
    return "PMsgRecptI{"+message+","+getIdentifier()+"}";
  }
}
