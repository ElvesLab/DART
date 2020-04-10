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
package rice.pastry.standard;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.mpisws.p2p.transport.exception.NodeIsFaultyException;
import org.mpisws.p2p.transport.liveness.LivenessListener;
import org.mpisws.p2p.transport.priority.QueueOverflowException;

import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.messaging.Message;
import rice.pastry.messaging.MessageDispatch;
import rice.pastry.routing.RouteMessage;
import rice.pastry.routing.RouterStrategy;
import rice.pastry.routing.SendOptions;
import rice.pastry.transport.PMessageNotification;
import rice.pastry.transport.PMessageReceipt;

/**
 * The superclass makes the routing decisions.  This class handles the rapid-rerouting.
 * 
 * @author Jeff Hoye
 *
 */
public class RapidRerouter extends StandardRouter implements LivenessListener<NodeHandle> {

  /**
   * The max times to try to reroute a message.
   */
  public static final int MAX_RETRIES = 10;
    
  /**
   * These messages should be rapidly rerouted if the node goes suspected.
   */
  Map<NodeHandle, Collection<RouterNotification>> pending;
  
  public RapidRerouter(PastryNode thePastryNode, MessageDispatch dispatch, RouterStrategy strategy) {
    super(thePastryNode, dispatch, strategy);
    pending = new HashMap<NodeHandle, Collection<RouterNotification>>();
    
    thePastryNode.addLivenessListener(this);
  }
  
  
  @Override
  protected void sendTheMessage(RouteMessage rm, NodeHandle handle) {
//    logger.log("sendTheMessage("+rm+","+handle+") reroute:"+rm.getOptions().rerouteIfSuspected());
    if (rm.getOptions().multipleHopsAllowed() && rm.getOptions().rerouteIfSuspected()) {
      // can be rapidly rerouted
      if (handle.getLiveness() >= LIVENESS_SUSPECTED) {
//        if (logger.level <= Logger.WARNING) logger.log("Reroutable message "+rm+" sending to non-alive node:"+handle+" liveness:"+handle.getLiveness());
        super.sendTheMessage(rm, handle);
        return;
      }
      
      RouterNotification notifyMe = new RouterNotification(rm, handle);
      addToPending(notifyMe, handle);
      rm.setTLCancellable(notifyMe);
      notifyMe.setCancellable(thePastryNode.send(handle, rm, notifyMe, rm.getTLOptions()));
    } else {
      super.sendTheMessage(rm, handle);
    }
  }
  
  protected void rerouteMe(final RouteMessage rm, NodeHandle oldDest, Exception ioe) {        
    if (logger.level <= Logger.FINE) logger.log("rerouteMe("+rm+" oldDest:"+oldDest+")");

    rm.numRetries++;
    if (rm.numRetries > MAX_RETRIES) {
      // TODO: Notify some kind of Error Handler
      boolean dontPrint = false;
      if (ioe == null) {
        dontPrint = rm.sendFailed(new TooManyRouteAttempts(rm, MAX_RETRIES));
      } else {
        dontPrint = rm.sendFailed(ioe);
      }
      if (dontPrint) {
        if (logger.level <= Logger.CONFIG) logger.log("rerouteMe() dropping "+rm+" after "+rm.numRetries+" attempts to (re)route.");
      } else {
        if (logger.level <= Logger.WARNING) logger.log("rerouteMe() dropping "+rm+" after "+rm.numRetries+" attempts to (re)route.");        
      }
      return;
    }

    // give the selector a chance to do some IO before trying to schedule again
    thePastryNode.getEnvironment().getSelectorManager().invoke(new Runnable() {
      public void run() {
        // this is going to make forward() be called again, can prevent this with a check in getPrevNode().equals(localNode)
        rm.getOptions().setRerouteIfSuspected(SendOptions.defaultRerouteIfSuspected);
        route(rm);
      }    
    });
  }
  
  private void addToPending(RouterNotification notifyMe, NodeHandle handle) {
    if (logger.level <= Logger.FINE) logger.log("addToPending("+notifyMe+" to:"+handle+")");
    synchronized(pending) {
      Collection<RouterNotification> c = pending.get(handle);
      if (c == null) {
        c = new HashSet<RouterNotification>();
        pending.put(handle, c);
      }
      c.add(notifyMe);
    }
  }

  /**
   * Return true if it was still pending.
   * 
   * @param notifyMe
   * @param handle
   * @return true if still pending
   */
  private boolean removeFromPending(RouterNotification notifyMe, NodeHandle handle) {
    synchronized(pending) {
      Collection<RouterNotification> c = pending.get(handle);
      if (c == null) {
        if (logger.level <= Logger.FINE) logger.log("removeFromPending("+notifyMe+","+handle+") had no pending messages for handle.");
        return false;
      }
      boolean ret = c.remove(notifyMe);
      if (c.isEmpty()) {
        pending.remove(handle);
      }
      if (!ret) {
        if (logger.level <= Logger.FINE) logger.log("removeFromPending("+notifyMe+","+handle+") msg was not there."); 
      }
      return ret;
    }    
  }
  
  public void livenessChanged(NodeHandle i, int val, Map<String, Object> options) {
    if (val >= LIVENESS_SUSPECTED) {
      Collection<RouterNotification> rerouteMe;
      synchronized(pending) {
        rerouteMe = pending.remove(i);        
      }
      if (rerouteMe != null) {
        if (logger.level <= Logger.FINE) logger.log("removing all messages to:"+i);
        for (RouterNotification rn : rerouteMe) {
          rn.cancel();
          rerouteMe(rn.rm, rn.dest, null);
        }
      }
    }
  }
  
  @Override
  public void destroy() {
    super.destroy();
    thePastryNode.removeLivenessListener(this);
  }

  class RouterNotification implements Cancellable, PMessageNotification {
    RouteMessage rm;
    NodeHandle dest;
    PMessageReceipt cancellable;

    public RouterNotification(RouteMessage rm, NodeHandle handle) {
      this.rm = rm;
      this.dest = handle;
      if (logger.level <= Logger.FINE) logger.log("RN.ctor() "+rm+" to:"+dest);
    }

    public void setCancellable(PMessageReceipt receipt) {
      if (receipt == null) if (logger.level <= Logger.WARNING) logger.logException(this+".setCancellable(null)", new Exception("Stack Trace"));
          
      this.cancellable = receipt;
    }

    public void sendFailed(PMessageReceipt msg, Exception reason) {
      // what to do..., rapidly reroute? 
      failed = true;
      cancellable = null;
      rm.setTLCancellable(null);      
      if (reason instanceof QueueOverflowException) {
        // todo, reroute this to a node w/o a full queue
        removeFromPending(this, dest);
        if (rm.sendFailed(reason)) {
          if (logger.level <= Logger.CONFIG) logger.logException("sendFailed("+msg.getMessage()+")=>"+msg.getIdentifier(), reason);
        } else {
          if (logger.level <= Logger.FINE) {
            logger.logException("sendFailed("+msg.getMessage()+")=>"+msg.getIdentifier(), reason);
          } else {
            if (logger.level <= Logger.INFO) {
              if (msg.getIdentifier() == null) {
                logger.logException("sendFailed("+msg.getMessage()+")=>"+msg.getIdentifier()+" "+reason+" identifier was null!!!", new Exception("Stack Trace"));
              } else {
                logger.log("sendFailed("+msg.getMessage()+")=>"+msg.getIdentifier()+" "+reason);          
              }
            }
          }
        }
        return; 
      }
      
      if (reason instanceof NodeIsFaultyException) {
        if (msg.getIdentifier().isAlive()) {
          if (logger.level <= Logger.WARNING) {
            logger.logException("Threw NodeIsFaultyException, and node is alive.  Node:"+msg.getIdentifier()+" Liveness:"+msg.getIdentifier().getLiveness(), reason);
            logger.logException("RRTrace", new Exception("Stack Trace"));
          }
        }
      }
      
      if (removeFromPending(this, dest)) {
        if (logger.level <= Logger.INFO) logger.logException("Send failed on message "+rm+" to "+dest+" rerouting."+msg, reason);
        rerouteMe(rm, dest, reason);
      } else {        
        if (rm.sendFailed(reason)) {
          if (logger.level <= Logger.CONFIG) logger.logException("sendFailed("+msg.getMessage()+")=>"+msg.getIdentifier(), reason);
        } else {
          if (logger.level <= Logger.INFO) logger.logException("sendFailed("+msg.getMessage()+")=>"+msg.getIdentifier(), reason);          
        }
      }
    }

    boolean failed = false;
    boolean sent = false;
    boolean cancelled = false;
    
    public void sent(PMessageReceipt msg) {
      if (logger.level <= Logger.FINE) logger.log("Send success "+rm+" to:"+dest+" "+msg);
      sent = true;
      cancellable = null;
      rm.setTLCancellable(null);
      removeFromPending(this, dest);      
      rm.sendSuccess(dest);
    }

    public boolean cancel() {
      if (logger.level <= Logger.FINE) logger.log("cancelling "+this);
      if (cancellable == null && logger.level <= Logger.WARNING) logger.log("cancellable = null c:"+cancelled+" s:"+sent+" f:"+failed);
      cancelled = true;
      if (cancellable != null) return cancellable.cancel();
      return true;
    }    
    
    public String toString() {
      return "RN{"+rm+"->"+dest+"}";
    }
  }


}
