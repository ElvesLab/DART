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

package rice.p2p.past;

import rice.*;
import rice.Continuation.*;
import rice.p2p.commonapi.*;
import rice.p2p.past.messaging.*;
import rice.persistence.*;

/**
 * @(#) PastPolicy.java This interface represents a policy for Past, which is asked whenever
 * the local node is told to replicate or validate an item.  This allows for applications to
 * control replication and object validate behavior, permitting behavior specific for
 * mutable or self-authenticating data.
 *
 * @version $Id: PastPolicy.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public interface PastPolicy {
  
  /**
   * This method is called when Past is told to fetch a key.  This method allows the application
   * to specify how a replica is fetched and authenticated.  The client should fetch the object
   * (possibly using the past instance provided) and then return the object to the provided
   * continuation.  The client *MUST* call the continuation at some point in the future, even if
   * the request is lost.
   *
   * @param id The id to fetch
   * @param past The local past instance 
   * @param backup The backup cache, where the object *might* be located
   * @param command The command to call with the replica to store
   */
  public void fetch(Id id, NodeHandle hint, Cache backup, Past past, Continuation command);
  
  /**
   * This method is call before an insert() is processed on the local node.  This allows applications
   * to make a decision on whether or not to store the replica.  Unless you know what you are doing,
   * don't return anything but 'true' here.
   *
   * @param content The content about to be stored
   * @return Whether the insert should be allowed
   */
  public boolean allowInsert(PastContent content);
  
  /**
   * The default policy for Past, which fetches any available copy of a replicated object and
   * always allows inserts locally.
   *
   * @author Alan Mislove
   */
  public static class DefaultPastPolicy implements PastPolicy {
    
    /**
     * This method fetches the object via a lookup() call.
     *
     * @param id The id to fetch
     * @param hint A hint as to where the key might be
     * @param backup The backup cache, where the object *might* be located
     * @param past The local past instance 
     * @param command The command to call with the replica to store
     */
    public void fetch(final Id id, final NodeHandle hint, final Cache backup, final Past past, Continuation command) {
      if ((backup != null) && backup.exists(id)) {
        backup.getObject(id, command);
      } else {
        past.lookup(id, false, new StandardContinuation(command) {
          public void receiveResult(Object o) {
            if (o != null) 
              parent.receiveResult(o);
            else 
              past.lookupHandle(id, hint, new StandardContinuation(parent) {
                public void receiveResult(Object o) {
                  if (o != null) 
                    past.fetch((PastContentHandle) o, parent);
                  else
                    parent.receiveResult(null);
                }
              });
          }
        });
      }
    }
    
    /**
     * This method always return true;
     *
     * @param content The content about to be stored
     * @return Whether the insert should be allowed
     */
    public boolean allowInsert(PastContent content) {
      return true;
    }
  }
}

