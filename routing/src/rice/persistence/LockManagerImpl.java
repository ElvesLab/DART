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
package rice.persistence;

import java.util.*;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Id;

@SuppressWarnings("unchecked")
public class LockManagerImpl implements LockManager {

  // map: Id -> List<Continuation>
  private HashMap locks;
  
  protected Logger logger;

  public LockManagerImpl(Environment env) {
    this.locks = new HashMap();
    this.logger =  env.getLogManager().getLogger(StorageManagerImpl.class,null);
  }

  
  public void lock(Id id, Continuation c) {
    Continuation torun = null;
    if (logger.level <= Logger.FINE) logger.log("locking on id "+id+" for continuation "+c);
    synchronized (this) {
      if (locks.containsKey(id)) {
        List locklist = ((List)locks.get(id));
        if (logger.level <= Logger.FINER) logger.log("locking on id "+id+"; blocked on "+locklist.size()+" earlier continuations");
        if (locklist.size() > 10 && logger.level <= Logger.INFO) logger.log("locking on id "+id+"; "+locklist.size()+" continuations in queue.  That seems large");
        locklist.add(c);
      } else {
        locks.put(id, new LinkedList());
        if (logger.level <= Logger.FINER) logger.log("locking on id "+id+"; no contention so running "+c);
        torun = c;
      }
    }
    if (torun != null)
      torun.receiveResult(null);
  }
  
  public void unlock(Id id) {
    Continuation torun = null;
    if (logger.level <= Logger.FINE) logger.log("unlocking on id "+id);
    synchronized (this) {
      if (locks.containsKey(id)) {
        if (((List)locks.get(id)).isEmpty()) {
          if (logger.level <= Logger.FINER) logger.log("unlocking on id "+id+"; last out the door -- removing lock ");
          locks.remove(id);
        } else {
          Continuation next = (Continuation)((List)locks.get(id)).remove(0);
          if (logger.level <= Logger.FINER) logger.log("unlocking on id "+id+"; starting next continuation "+next);
          torun = next;
        }
      } else {
        if (logger.level <= Logger.WARNING) logger.log("unlocking on id "+id+"; no lock currently held!!");
      }
    }
    if (torun != null)
      torun.receiveResult(null);
  }

}
