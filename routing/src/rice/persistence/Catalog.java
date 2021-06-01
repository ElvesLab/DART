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
import rice.p2p.util.*;

/*
 * @(#) Catalog.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 * 
 * @version $Id: Catalog.java 4654 2009-01-08 16:33:07Z jeffh $
 */
import java.io.*;

import rice.*;
import rice.p2p.commonapi.*;

/**
 * This interface is the abstraction of something which holds objects
 * which are available for lookup.  This interface does not, however,
 * specify how the objects are inserted, and makes NO guarantees as to
 * whether objects available at a given point in time will be available
 * at a later time.
 *
 * Implementations of the Catalog interface are designed to be interfaces
 * which specify how objects are inserted and stored, and are designed to
 * include a cache and persistent storage interface.
 */
@SuppressWarnings("unchecked")
public interface Catalog {

  /**
   * Returns whether or not an object is present in the location <code>id</code>.
   *
   * @param id The id of the object in question.
   * @return Whether or not an object is present at id.
   */
  public boolean exists(Id id);

  /**
   * Returns the object identified by the given id, or <code>null</code> if
   * there is no corresponding object (through receiveResult on c).
   *
   * @param id The id of the object in question.
   * @param c The command to run once the operation is complete
   */
  public void getObject(Id id, Continuation c);
  
  /**
   * Returns the metadata associated with the provided object, or null if
   * no metadata exists.  The metadata must be stored in memory, so this 
   * operation is guaranteed to be fast and non-blocking.
   *
   * The metadata returned from this method must *NOT* be mutated in any way,
   * as the actual reference to the internal object is returned.  Mutating
   * this metadata may make the internal indices incorrect, resulting 
   * in undefined behavior.  Changing the metadata should be done by creating
   * a new metadata object and calling setMetadata().
   *
   * @param id The id for which the metadata is needed
   * @return The metadata, or null if none exists
   */
  public Serializable getMetadata(Id id);
  
  /**
   * Updates the metadata stored under the given key to be the provided
   * value.  As this may require a disk access, the requestor must
   * also provide a continuation to return the result to.  
   *
   * @param id The id for the metadata 
   * @param metadata The metadata to store
   * @param c The command to run once the operation is complete
   */
  public void setMetadata(Id id, Serializable metadata, Continuation command);
  
  /**
   * Renames the given object to the new id.  This method is potentially faster
   * than store/cache and unstore/uncache.
   *
   * @param oldId The id of the object in question.
   * @param newId The new id of the object in question.
   * @param c The command to run once the operation is complete
   */
  public void rename(Id oldId, Id newId, Continuation c);

 /**
   * Return the objects identified by the given range of ids. The IdSet 
   * returned contains the Ids of the stored objects. The range is
   * partially inclusive, the lower range is inclusive, and the upper
   * exclusive.
   *
   * NOTE: This method blocks so if the behavior of this method changes and
   * no longer stored in memory, this method may be deprecated.
   *
   * @param range The range to query  
   * @return The idset containing the keys 
   */
  public IdSet scan(IdRange range);
  
  /**
   * Return all objects currently stored by this catalog
   *
   * NOTE: This method blocks so if the behavior of this method changes and
   * no longer stored in memory, this method may be deprecated.
   *
   * @return The idset containing the keys 
   */
  public IdSet scan();

  /**
   * Returns a map which contains keys mapping ids to the associated 
   * metadata.  
   *
   * @param range The range to query  
   * @return The map containing the keys 
   */
  public SortedMap scanMetadata(IdRange range);
  
  /**
   * Returns a map which contains keys mapping ids to the associated 
   * metadata.  
   *
   * @return The treemap mapping ids to metadata 
   */
  public SortedMap scanMetadata();
  
  /**
   * Returns the submapping of ids which have metadata less than the provided
   * value.
   *
   * @param value The maximal metadata value 
   * @return The submapping
   */
  public SortedMap scanMetadataValuesHead(Object value);
  
  /**
   * Returns the submapping of ids which have metadata null
   *
   * @return The submapping
   */
  public SortedMap scanMetadataValuesNull();
  
  /**
   * Returns the number of Ids currently stored in the catalog
   *
   * @return The number of ids in the catalog
   */
  public int getSize();
  
  /**
   * Returns the total size of the stored data in bytes.
   *
   * @return The total storage size
   */
  public long getTotalSize();
  
  /**
   * Method which is used to erase all data stored in the Catalog.  
   * Use this method with care!
   *
   * @param c The command to run once done
   */
  public void flush(Continuation c);
}
