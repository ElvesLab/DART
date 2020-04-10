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

/*
 * @(#) Storage.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 *
 * @version $Id: Storage.java 4654 2009-01-08 16:33:07Z jeffh $
 */
import java.io.*;

import rice.*;
import rice.p2p.commonapi.*;

/**
 * This interface is the abstraction of something which provides a
 * local storage service, such as a persistence storage service or
 * an in-memory storage service.  Two implementations are provided,
 * the PersistentStorage and MemoryStorage, respsectively.
 */
@SuppressWarnings("unchecked")
public interface Storage extends Catalog {

  /**
   * Stores an object in this storage. This method is non-blocking.
   * If the object has already been stored at the location id, this
   * method has the effect of calling <code>unstore(id)</code> followed
   * by <code>store(id, obj)</code>. This method finishes by calling
   * receiveResult() on the provided continuation with the success
   * or failure of the store.
   *
   * Returns <code>True</code> if the action succeeds, else
   * <code>False</code> (through receiveResult on c).
   *
   * @param id The object's id.
   * @param metadata The object's metadata
   * @param obj The object to store.
   * @param c The command to run once the operation is complete
   */
  public void store(Id id, Serializable metadata, Serializable obj, Continuation c);

  /**
   * Removes the object from the list of stored objects. This method is
   * non-blocking. If the object was not in the stored list in the first place,
   * nothing happens and <code>False</code> is returned.
   *
   * Returns <code>True</code> if the action succeeds, else
   * <code>False</code>  (through receiveResult on c).
   *
   * @param pid The object's persistence id
   * @param c The command to run once the operation is complete
   */
  public void unstore(Id id, Continuation c);
}
