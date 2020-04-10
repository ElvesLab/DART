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

import rice.p2p.commonapi.*;

/**
 * @(#) Replication.java
 *
 * This interface is exported by Replication Manager (RM) for any applications 
 * which need to replicate objects across k+1 nodes closest to the object 
 * identifier in the NodeId space. The 'closest' (to the object identifier)
 * of the k+1 nodes is referred to as the 0-root in which the object is
 * stored by default when not using the replica manager. 
 * Additionally the RM assists in maintaining the invariant that the object
 * is also stored in the other k nodes referred to as the i-roots (1<=i<=k).
 * In the RM literature, k is called the ReplicaFactor and is used when
 * an instance of the replica manager is being instantiated.
 *
 * @version $Id: Replication.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public interface Replication {
  
  /**
   * Method which invokes the replication process.  This should not normally be called by
   * applications, as the Replication class itself periodicly invokes this process.  However,
   * applications are allowed to use this method to initiate a replication request.
   */
  public void replicate();
  
}








