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
package rice.pastry.socket.nat.rendezvous;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.mpisws.p2p.transport.rendezvous.ContactDirectStrategy;


import rice.Continuation;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.pastry.NodeHandle;
import rice.pastry.PastryNode;
import rice.pastry.leafset.LeafSet;
import rice.pastry.pns.PNSApplication;
import rice.pastry.routing.RouteSet;

/**
 * Does not try to connect to NATted nodes during PNS.
 * 
 * TODO: Cull out firewalled nodes from return values
 * TODO: Make Proximity of firewalled nodes MAX_VALUE
 * TODO: Make Rendezvous Layer check the MultiInetSocketAddress to see if they can contact eachother directly
 *  
 * @author Jeff Hoye
 *
 */
public class RendezvousPNSApplication extends PNSApplication {

  ContactDirectStrategy<RendezvousSocketNodeHandle> contactDirectStrategy;
  
  public RendezvousPNSApplication(PastryNode pn, ContactDirectStrategy<RendezvousSocketNodeHandle> contactDirectStrategy) {
    super(pn, pn.getEnvironment().getLogManager().getLogger(RendezvousPNSApplication.class, null));
    this.contactDirectStrategy = contactDirectStrategy;
    if (contactDirectStrategy == null) throw new IllegalArgumentException("Need contactDirectStrategy was null");
  }

  /**
   * This method decides who to bother trying to connect to.
   * 
   * If the node is not NATted, return false.
   * 
   * If the node is NATted:
   *   c.receiveException()
   *   return true
   * 
   * @param handle
   * @param c
   * @return
   */
  @SuppressWarnings("unchecked")
  protected boolean ignore(NodeHandle handle, Continuation c) {
    if (useHandle(handle)) return false;
    if (logger.level <= Logger.FINE) logger.log("PNS not using firewalled node "+handle);
    c.receiveException(new NodeIsFirewalledException(handle));
    return true;
  }
  
  /**
   * Separate this out to make it super easy to change the policy.
   * 
   * @param handle
   * @return
   */
  protected boolean useHandle(NodeHandle handle) {
    RendezvousSocketNodeHandle rsnh = (RendezvousSocketNodeHandle)handle;
    return contactDirectStrategy.canContactDirect(rsnh);
//    return rsnh.canContactDirect();    
  }

  /**
   * Don't return any non-contactDirect handles unless all of them are.
   */
  @Override
  protected List<NodeHandle> getNearHandlesHelper(List<NodeHandle> handles) {
    ArrayList<NodeHandle> contactDirect = new ArrayList<NodeHandle>();
    ArrayList<NodeHandle> notContactDirect = new ArrayList<NodeHandle>();
    
    for (NodeHandle foo : handles) {
      RendezvousSocketNodeHandle rsnh = (RendezvousSocketNodeHandle)foo;
      if (rsnh.canContactDirect()) {
        contactDirect.add(foo);
      } else {
        notContactDirect.add(foo);        
      }
    }
    if (contactDirect.isEmpty()) return notContactDirect;
    return contactDirect;
  }

  
  /**
   * This is the first step, cull out the bootHandles that we can't use good.
   */
  @Override
  public Cancellable getNearHandles(Collection<NodeHandle> bootHandles,
      Continuation<Collection<NodeHandle>, Exception> deliverResultToMe) {
    ArrayList<NodeHandle> newBootHandles = new ArrayList<NodeHandle>();
    if (logger.level <= Logger.INFO) logger.log("Booting off of "+bootHandles.size()+" nodes. "+bootHandles);
    for (NodeHandle handle : bootHandles) {
      if (useHandle(handle)) {
        newBootHandles.add(handle);
      } else {
        if (logger.level <= Logger.WARNING) logger.log("Can't use "+handle+" it is firewalled.");
      }
    }
    
    return super.getNearHandles(newBootHandles, deliverResultToMe);
  }

  
  
  @Override
  public Cancellable getLeafSet(final NodeHandle input,
      final Continuation<LeafSet, Exception> c) {
//    logger.log(this+".getLeafSet("+input+")");
    if (ignore(input, c)) return null;
    return super.getLeafSet(input, new Continuation<LeafSet, Exception>() {
    
      public void receiveResult(LeafSet result) {
        for (NodeHandle handle : result) {
          if (!useHandle(handle)) {
            if (logger.level <= Logger.FINE) logger.log("getLeafSet("+input+") Dropping "+handle);
            result.remove(handle);
          }
        }
        c.receiveResult(result);
      }
    
      public void receiveException(Exception exception) {
        c.receiveException(exception);
      }    
    });
  }

  @Override
  public Cancellable getRouteRow(final NodeHandle input, final short row,
      final Continuation<RouteSet[], Exception> c) {
    if (ignore(input, c)) return null;
    return super.getRouteRow(input, row, new Continuation<RouteSet[], Exception>() {
    
      public void receiveResult(RouteSet[] result) {
        for (int ctr = 0; ctr < result.length; ctr++) {
          RouteSet rs = result[ctr];
          if (rs != null) {
            for (NodeHandle handle : rs) {
              if (handle != null && !useHandle(handle)) {
                if (logger.level <= Logger.FINE) logger.log("getRouteRow("+input+","+row+") Dropping "+handle+" because it is FireWalled");
                rs.remove(handle);
              }
            }
            if (rs.isEmpty()) result[ctr] = null;
          }
        }
        c.receiveResult(result);
      }
    
      public void receiveException(Exception exception) {
        c.receiveException(exception);
      }
    
    });
  }


  @Override
  public Cancellable getNearest(NodeHandle seed,
      Continuation<Collection<NodeHandle>, Exception> retToMe) {
    if (ignore(seed, retToMe)) return null;
    return super.getNearest(seed, retToMe);
  }

  @Override
  public Cancellable getProximity(NodeHandle handle,
      Continuation<Integer, IOException> c, int timeout) {
    if (!useHandle(handle)) {
      c.receiveResult(Integer.MAX_VALUE);
      return null;
    }
    return super.getProximity(handle, c, timeout);
  }

}
