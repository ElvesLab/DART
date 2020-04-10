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

package rice.p2p.replication;

import java.util.*;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.replication.messaging.*;

/**
 * @(#) ReplicationPolicy.java This interface represents a policy for Replication, 
 * which is asked whenever the replication manager need to make an application-specific
 * decision.
 *
 * @version $Id: ReplicationPolicy.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 */
public interface ReplicationPolicy {
  
  /**
   * This method is given a list of local ids and a list of remote ids, and should return the
   * list of remote ids which need to be fetched.  Thus, this method should return the set
   * B-A, where the result is a subset of B.
   *
   * @param local The set of local ids
   * @param remote The set of remote ids
   * @param factory The factory to use to create IdSets
   * @return A subset of the remote ids which need to be fetched
   */
  public IdSet difference(IdSet local, IdSet remote, IdFactory factory);
  
  /**
   * The default policy for Replication, which simply does a direct diff between the sets
   *
   * @author Alan Mislove
   */
  public static class DefaultReplicationPolicy implements ReplicationPolicy {
    
    /**
     * This method simply returns remote-local.
     *
     * @param local The set of local ids
     * @param remote The set of remote ids
     * @param factory The factory to use to create IdSets
     * @return A subset of the remote ids which need to be fetched
     */
    public IdSet difference(IdSet local, IdSet remote, IdFactory factory) {
      IdSet result = factory.buildIdSet();
      Iterator<Id> i = remote.getIterator();
      
      while (i.hasNext()) {
        Id id = (Id) i.next();
        
        if (! local.isMemberId(id)) 
          result.addId(id);
      }
      
      return result;
    }
  }
}

