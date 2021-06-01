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

package rice.p2p.past.gc;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.past.*;

/**
 * @(#) GCPast.java
 * 
 * This interface represents an extension to the Past interface, which adds
 * support for garbage collection and versionsing.  Individual objects are
 * each given versions and timeout times.  Version numbers are used to keep
 * different replicas in sync, and the timeout times are used to determine
 * when space can be reclaimed.
 *
 * Applications must periodically invoke the refresh() operation on all objects
 * which they are interested in.  Otherwise, objects may be reclaimed and
 * will be no longer available.
 *
 * @version $Id: GCPast.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Alan Mislove
 * @author Andreas Haeberlen
 */
@SuppressWarnings("unchecked")
public interface GCPast extends Past {
  
  /**
   * Timeout value which indicates that the object should never expire.
   * Note that objects with this timeout value _will_ be deleted in 
   * the year 292473178. If this is a problem, applications should
   * check for this value explicitly.
   */
  public static final long INFINITY_EXPIRATION = Long.MAX_VALUE;
  
  /**
   * Inserts an object with the given ID into this instance of Past.
   * Asynchronously returns a PastException to command, if the
   * operation was unsuccessful.  If the operation was successful, a
   * Boolean[] is returned representing the responses from each of
   * the replicas which inserted the object.
   *
   * This method is equivalent to 
   *
   * insert(obj, INFINITY_EXPIRATION, command)
   *
   * as it inserts the object with a timeout value of infinity.  This
   * is done for simplicity, as well as backwards-compatibility for 
   * applications.
   * 
   * @param obj the object to be inserted
   * @param command Command to be performed when the result is received
   */
  public void insert(PastContent obj, Continuation command);
  
  /**
   * Inserts an object with the given ID into this instance of Past.
   * Asynchronously returns a PastException to command, if the
   * operation was unsuccessful.  If the operation was successful, a
   * Boolean[] is returned representing the responses from each of
   * the replicas which inserted the object.
   *
   * The contract for this method is that the provided object will be 
   * stored until the provided expiration time.  Thus, if the application
   * determines that it is still interested in this object, it must refresh
   * the object via the refresh() method.
   * 
   * @param obj the object to be inserted
   * @param expiration the time until which the object must be stored
   * @param command Command to be performed when the result is received
   */
  public void insert(PastContent obj, long expiration, Continuation command);

  /**
   * Updates the objects stored under the provided keys id to expire no
   * earlier than the provided expiration time.  Asyncroniously returns
   * the result to the caller via the provided continuation.  
   *
   * The result of this operation is an Object[], which is the same length
   * as the input array of Ids.  Each element in the array is either 
   * Boolean(true), representing that the refresh succeeded for the 
   * cooresponding Id, or an Exception describing why the refresh failed.  
   * Specifically, the possible exceptions which can be returned are:
   * 
   * ObjectNotFoundException - if no object was found under the given key
   * RefreshFailedException - if the refresh operation failed for any other
   *   reason (the getMessage() will describe the failure)
   * 
   * @param id The keys which to refresh
   * @param expiration The time to extend the lifetime to
   * @param command Command to be performed when the result is received
   */
  public void refresh(Id[] ids, long[] expiration, Continuation command);

  /**
   * Updates the objects stored under the provided keys id to expire no
   * earlier than the provided expiration time.  Asyncroniously returns
   * the result to the caller via the provided continuation.  
   *
   * The result of this operation is an Object[], which is the same length
   * as the input array of Ids.  Each element in the array is either 
   * Boolean(true), representing that the refresh succeeded for the 
   * cooresponding Id, or an Exception describing why the refresh failed.  
   * Specifically, the possible exceptions which can be returned are:
   * 
   * ObjectNotFoundException - if no object was found under the given key
   * RefreshFailedException - if the refresh operation failed for any other
   *   reason (the getMessage() will describe the failure)
   * 
   * @param id The keys which to refresh
   * @param expiration The time to extend the lifetime to (applies to all keys)
   * @param command Command to be performed when the result is received
   */
  public void refresh(Id[] ids, long expiration, Continuation command);

}

