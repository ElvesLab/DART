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

package rice.p2p.replication.manager;

import rice.*;

import rice.p2p.commonapi.*;

/**
 * @(#) ReplicationManagerClient.java
 *
 * This interface represents client of the replication manager, which is the
 * ultimate user of the replication.
 *
 * @version $Id: ReplicationManagerClient.java 4654 2009-01-08 16:33:07Z jeffh $
 *
 * @author Alan Mislove
 */
@SuppressWarnings("unchecked")
public interface ReplicationManagerClient {
  
  /**
   * This upcall is invoked to tell the client to fetch the given id, 
   * and to call the given command with the boolean result once the fetch
   * is completed.  The client *MUST* call the command at some point in the
   * future, as the manager waits for the command to return before continuing.
   *
   * @param id The id to fetch
   * @param hint A hint where to find the key from.  This is where the local node
   *           heard about the key.
   * @param command The command to return the result to
   */
  public void fetch(Id id, NodeHandle hint, Continuation command);
  
  /**
   * This upcall is to notify the client that the given id can be safely removed
   * from the storage.  The client may choose to perform advanced behavior, such
   * as caching the object, or may simply delete it.
   *
   * @param id The id to remove
   */
  public void remove(Id id, Continuation command);
  
  /**
   * This upcall should return the set of keys that the application
   * currently stores in this range. Should return a empty IdSet (not null),
   * in the case that no keys belong to this range.
   *
   * @param range the requested range
   */
  public IdSet scan(IdRange range);
  
  /**
   * This upcall should return whether or not the given id is currently stored
   * locally by the client.
   *
   * @param id The id in question
   * @return Whether or not the id exists
   */
  public boolean exists(Id id);
  
  /**
   * This upcall should return whether or not the given id is currently stored
   * somewhere in the overlay by the client.
   * 
   * @param id The id in question
   * @return Whether or not the id exists somewhere in the overlay
   */
  public void existsInOverlay(Id id, Continuation command);
  
  /**
   * Asks a client to reinsert an object it already holds into the overlay
   * 
   * @param id The id in question
   * @return 
   */
  public void reInsert(Id id, Continuation command);
}

