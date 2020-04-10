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
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.pastry.messaging.*;
import rice.pastry.socket.TransportLayerNodeHandle;

import java.io.*;
import java.util.*;


/**
 * Interface for handles to remote nodes.
 *
 * @version $Id: NodeHandle.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Andrew Ladd
 */
public abstract class NodeHandle extends rice.p2p.commonapi.NodeHandle implements RawSerializable 
{

  public static final int LIVENESS_ALIVE = 1;
  public static final int LIVENESS_SUSPECTED = 2;
  public static final int LIVENESS_DEAD = 3;
    
  // the local pastry node
  protected transient PastryNode localnode;
  protected transient Logger logger;
  
  static final long serialVersionUID = 987479397660721015L;
  /**
   * Gets the nodeId of this Pastry node.
   *
   * @return the node id.
   */
  public abstract Id getNodeId();

  public rice.p2p.commonapi.Id getId() {
    return getNodeId();
  }

  /**
   * Returns the last known liveness information about the Pastry node associated with this handle.
   * Invoking this method does not cause network activity.
   *
   * @return true if the node is alive, false otherwise.
   * 
   * @deprecated use PastryNode.isAlive(NodeHandle) 
   */
  public final boolean isAlive() {
    return getLiveness() < LIVENESS_DEAD; 
  }

  /**
   * A more detailed version of isAlive().  This can return 3 states:
   * 
   * @return LIVENESS_ALIVE, LIVENESS_SUSPECTED, LIVENESS_DEAD
   */
  public abstract int getLiveness();
  
  /**
   * Method which FORCES a check of liveness of the remote node.  Note that
   * this method should ONLY be called by internal Pastry maintenance algorithms - 
   * this is NOT to be used by applications.  Doing so will likely cause a
   * blowup of liveness traffic.
   *
   * @return true if node is currently alive.
   */
  public boolean checkLiveness() {
    return ping();
  }

  /**
   * Returns the last known proximity information about the Pastry node associated with this handle.
   * Invoking this method does not cause network activity.
   *
   * Smaller values imply greater proximity. The exact nature and interpretation of the proximity metric
   * implementation-specific.
   *
   * @deprecated use PastryNode.proximity() or Endpoint.proximity()
   * @return the proximity metric value
   */
  public abstract int proximity();

  /**
   * Ping the node. Refreshes the cached liveness status and proximity value of the Pastry node associated
   * with this.
   * Invoking this method causes network activity.
   *
   * @return true if node is currently alive.
   */
  public abstract boolean ping();

  /**
   * Accessor method.
   */
  public final PastryNode getLocalNode() {
    return localnode;
  }

//  transient Exception ctor;
//  public NodeHandle() {
//    ctor = new Exception("ctor"); 
//  }
  
  /**
   * May be called from handle etc methods to ensure that local node has
   * been set, either on construction or on deserialization/receivemsg.
   */
  public void assertLocalNode() {
    if (localnode == null) {
//      ctor.printStackTrace();
      throw new RuntimeException("PANIC: localnode is null in " + this+"@"+System.identityHashCode(this));
    }
  }

  /**
   * Equality operator for nodehandles.
   *
   * @param obj a nodehandle object
   * @return true if they are equal, false otherwise.
   */
  public abstract boolean equals(Object obj);

  /**
   * Method which is used by Pastry to start the bootstrapping process on the 
   * local node using this handle as the bootstrap handle.  Default behavior is
   * simply to call receiveMessage(msg), but transport layer implementations may
   * care to perform other tasks by overriding this method, since the node is
   * not technically part of the ring yet.
   *
   * @param msg the bootstrap message.
   * @throws IOException 
   */
  public void bootstrap(Message msg) throws IOException {
    receiveMessage(msg);
  }

  /**
   * Hash codes for nodehandles.
   *
   * @return a hash code.
   */
  public abstract int hashCode();
  
  /**
   * @deprecated use PastryNode.send() or Endpoint.send()
   * @param msg
   */
  public abstract void receiveMessage(Message msg);

  public abstract void serialize(OutputBuffer buf) throws IOException;
  
  /****************** Overriding of the Observer pattern to include priority **********/
  
  /**
   * Class which holds an observer and a priority.  For sorting.
   * @author Jeff Hoye
   */
  static class ObsPri implements Comparable<ObsPri> {
    Observer obs;
    int pri;
    
    public ObsPri(Observer o, int priority) {
      obs = o;
      pri = priority;
    }

    public int compareTo(ObsPri o) {
      ObsPri that = (ObsPri)o;
      int ret = that.pri - pri;
      if (ret == 0) {
        if (that.equals(o)) {
          return 0;
        } else {
          return System.identityHashCode(that)-System.identityHashCode(this);
        }
      }
//      System.out.println(this+".compareTo("+that+"):"+ret);
      return ret;
    }
    
    public String toString() {
      return obs+":"+pri;      
    }
  }

  transient List<ObsPri> obs = new ArrayList<ObsPri>();
  /**
   * Need to construct obs in deserialization.
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    obs = new ArrayList<ObsPri>();
  }
  
  public void addObserver(Observer o) {
    addObserver(o, 0);  
  }
  
  /**
   * 
   * @param o
   * @param priority higher priority observers will be called first
   */
  public void addObserver(Observer o, int priority) {
    if (logger.level <= Logger.FINER) logger.log(this+".addObserver("+o+")");
    synchronized (obs) {
      // this pass makes sure it's not already an observer, and also finds 
      // the right spot for it
      for (int i = 0; i < obs.size(); i++) {
        ObsPri op = obs.get(i);
        if (op.obs.equals(o)) {
          if (op.pri != priority) {
            // change the priority, resort, return
            // Should we warn?  Hopefully this won't happen very often...
            if (logger.level <= Logger.WARNING) logger.log(this+".addObserver("+o+","+priority+") changed priority, was:"+op);      
            op.pri = priority;
            Collections.sort(obs);
            return;
          }
          // TODO: can optimize this if need be, make this look for the insertPoint.  For now, we're just going to add then sort.
//        } else {
//          if (op.pri <= priority) {
//            insertPoint = i; 
//          }
        }
      }
      obs.add(new ObsPri(o,priority));
//      logger.log(this+"addObserver("+o+")list1:"+obs);
      Collections.sort(obs);
//      logger.log(this+"addObserver("+o+")list2:"+obs);
    }
//    super.addObserver(o); 
  }
  
  public void deleteObserver(Observer o) {
    if (logger.level <= Logger.FINER) logger.log(this+".deleteObserver("+o+")");
//    super.deleteObserver(o); 
    synchronized (obs) {
      for (int i = 0; i < obs.size(); i++) {
        ObsPri op = obs.get(i);
        if (op.obs.equals(o)) {
          if (logger.level <= Logger.FINEST) logger.log(this+".deleteObserver("+o+"):success");
          obs.remove(i);
          return;
        }
      }
      if (logger.level <= Logger.INFO) logger.log(this+".deleteObserver("+o+"):failure "+o+" was not an observer.");      
    }
  }
  
  public void notifyObservers(Object arg) {
    List<ObsPri> l;
    synchronized(obs) {
      l = new ArrayList<ObsPri>(obs);    
//      logger.log(this+"notifyObservers("+arg+")list1:"+obs);
//      logger.log(this+"notifyObservers("+arg+")list2:"+l);
    }
    for(ObsPri op : l) {
//      logger.log(this+".notifyObservers("+arg+"):notifying "+op);            
      if (logger.level <= Logger.FINEST) logger.log(this+".notifyObservers("+arg+"):notifying "+op);            
      op.obs.update(this, arg); 
    }    
  }

  public synchronized int countObservers() {
    return obs.size();
  }

  public synchronized void deleteObservers() {
    obs.clear();
  }

  
  /**
   * Method which allows the observers of this socket node handle to be updated.
   * This method sets this object as changed, and then sends out the update.
   *
   * @param update The update
   */
  public void update(Object update) {
    if (logger != null) {
//      logger.log(this+".update("+update+")"+countObservers());
      if (logger.level <= Logger.FINE) {
//        String s = "";
//        if (obs != null)
//          synchronized(obs) {
//            Iterator i = obs.iterator(); 
//            while(i.hasNext())
//              s+=","+i.next();
//          }
        logger.log(this+".update("+update+")"+countObservers());
      }
    }
//    setChanged();
    notifyObservers(update);
    if (logger != null) {
      if (logger.level <= Logger.FINEST) {
//        String s = "";
//        if (obs != null)
//          synchronized(obs) {
//            Iterator i = obs.iterator(); 
//            while(i.hasNext())
//              s+=","+i.next();
//          }
        logger.log(this+".update("+update+")"+countObservers()+" done");
      }
    }
  }  
}



