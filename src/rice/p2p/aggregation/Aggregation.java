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
package rice.p2p.aggregation;

/**
 * @(#) Aggregation.java
 * 
 * This interface is exported by all instances of the Aggregation
 * service, in addition to the Past and GCPast interfaces.
 * The additional functions are used mainly to recover from failures,
 * i.e. cases in which the service loses its local metadata.
 *
 * @version $Id: Aggregation.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Andreas Haeberlen
 */

import java.io.Serializable;
import rice.Continuation;
import rice.p2p.commonapi.Id;

@SuppressWarnings("unchecked")
public interface Aggregation {

  /**
   * Fetches the handle object. This object is important in the event
   * of a failure that causes Aggregation to lose its local metadata
   * cache. When the handle object is later restored via setHandle(), 
   * Aggregation can recover all objects that have been aggregated
   * prior to the getHandle() call, except objects that have already
   * expired. Note that this does not include objects that were still
   * in the local buffer when the failure occurred. To prevent data
   * loss, it is recommended to invoke flush() before fetching
   * the handle object.
   * 
   * @return the current handle object
   */
  public Serializable getHandle();

  /**
   * Restores the handle object. This method should always be invoked
   * at startup, using the most current handle object available.
   * When the continuation is invoked, all non-expired objects linked 
   * to the handle are accessible from the local node. 
   *
   * @param handle the handle object
   * @param command Command to be performed when the method completes.
   */
  public void setHandle(Serializable handle, Continuation command);

  /**
   * Creates an aggregate that includes the most current object
   * with the specified key. When the continuation is invoked, 
   * the object is persistent and linked to the current handle object.
   *
   * @param id the key of the object to be made persistent.
   * @param command Command to be performed when the method completes.
   */
  public void flush(Id id, Continuation command);

  /**
   * Creates aggregates from all objects in the local object cache.
   * When the continuation is invoked, all objects that were inserted
   * prior to flush() are persistent and linked to the current
   * handle object. 
   *
   * @param command Command to be performed when the method completes.
   */
  public void flush(Continuation command);

  /**
   * Attempts to retrieve the most recent object that has been inserted 
   * by the local node under the specified key. This is useful when
   * the object has been overwritten by an attacker. This method may
   * fail or return an outdated version of the object if a) the object
   * is not linked to the current handle, or b) the object was never
   * aggregated because of the aggregation policy.
   *
   * @param id the key of the object to be retrieved
   * @param command Command to be performed when the method completes.
   */
  public void rollback(Id id, Continuation command);

  /**
   * Deletes all local state, including the aggregate list and all
   * objects waiting in the local buffer. This is useful when the
   * local instance appears to have been corrupted or tampered with.
   * The state can be recovered by invoking setHandle() with an 
   * earlier handle object, and by using rollback() on objects that
   * appear to have been overwritten.
   *
   * @param command Command to be performed when the method completes.
   */
  public void reset(Continuation command);
  
}

