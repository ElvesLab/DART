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
 * @(#) LRUCache.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 *
 * @version $Id: EmptyCache.java 4654 2009-01-08 16:33:07Z jeffh $
 */
import java.io.*;
import java.util.*;
import java.util.zip.*;

import rice.*;
import rice.Continuation.*;
import rice.p2p.commonapi.*;
import rice.p2p.util.*;

/**
 * This class is a cahcce which doesn't store anything.
 */
@SuppressWarnings("unchecked")
public class EmptyCache implements Cache {
  
  /**
   * The facotry for building id sets
   */
  protected IdFactory factory;

  /**
   * Builds an emtpy cache
   */
  public EmptyCache(IdFactory factory) {
    this.factory = factory;
  }
  
  /**
   * Renames the given object to the new id.  This method is potentially faster
   * than store/cache and unstore/uncache.
   *
   * @param oldId The id of the object in question.
   * @param newId The new id of the object in question.
   * @param c The command to run once the operation is complete
   */
  public void rename(final Id oldId, final Id newId, Continuation c) {
    c.receiveException(new IllegalArgumentException("EmptyCache has no objects!"));
  }
  
  /**
   * Caches an object in this storage. This method is non-blocking.
   * If the object has already been stored at the location id, this
   * method has the effect of calling <code>uncachr(id)</code> followed
   * by <code>cache(id, obj)</code>. This method finishes by calling
   * receiveResult() on the provided continuation with whether or not
   * the object was cached.  Note that the object may not actually be
   * cached due to the cache replacement policy.
   *
   * Returns <code>True</code> if the cache actaully stores the object, else
   * <code>False</code> (through receiveResult on c).
   *
   * @param id The object's id.
   * @param obj The object to cache.
   * @param c The command to run once the operation is complete
   */
  public void cache(Id id, Serializable metadata, Serializable obj, Continuation c) {
    c.receiveResult(new Boolean(true));
  }

  /**
   * Removes the object from the list of cached objects. This method is
   * non-blocking. If the object was not in the cached list in the first place,
   * nothing happens and <code>False</code> is returned.
   *
   * Returns <code>True</code> if the action succeeds, else
   * <code>False</code>  (through receiveResult on c).
   *
   * @param pid The object's id
   * @param c The command to run once the operation is complete
   */
  public void uncache(Id id, Continuation c) {
    c.receiveResult(new Boolean(true));
  }

  /**
   * Returns whether or not an object is cached in the location <code>id</code>.
   *
   * @param id The id of the object in question.
   * @return Whether or not an object is present at id.
   */
  public boolean exists(Id id) {
    return false;
  }

  /**
   * Returns the object identified by the given id.
   *
   * @param id The id of the object in question.
   * @param c The command to run once the operation is complete
   * @return The object, or <code>null</code> if there is no cooresponding
   * object (through receiveResult on c).
   */
  public void getObject(Id id, Continuation c) {
    c.receiveResult(null);
  }
  
  /**
   * Returns the metadata associated with the provided object, or null if
   * no metadata exists.  The metadata must be stored in memory, so this 
   * operation is guaranteed to be fast and non-blocking.
   *
   * @param id The id for which the metadata is needed
   * @return The metadata, or null of non exists
   */
  public Serializable getMetadata(Id id) {
    return null;
  }
  
  /**
   * Updates the metadata stored under the given key to be the provided
   * value.  As this may require a disk access, the requestor must
   * also provide a continuation to return the result to.  
   *
   * @param id The id for the metadata 
   * @param metadata The metadata to store
   * @param c The command to run once the operation is complete
   */
  public void setMetadata(Id id, Serializable metadata, Continuation c) {
    c.receiveResult(new Boolean(true));
  }

  /**
   * Return the objects identified by the given range of ids. The IdSet 
   * returned contains the Ids of the stored objects. The range is
   * partially inclusive, the lower range is inclusive, and the upper
   * exclusive.
   *
   *
   * NOTE: This method blocks so if the behavior of this method changes and
   * uses the disk, this method may be deprecated.
   *
   * @param range The range to query  
   * @return The idset containg the keys 
   */
   public IdSet scan(IdRange range){
     return factory.buildIdSet();
   }
  
  /**
   * Return all objects currently stored by this catalog
   *
   * NOTE: This method blocks so if the behavior of this method changes and
   * no longer stored in memory, this method may be deprecated.
   *
   * @return The idset containg the keys 
   */
  public IdSet scan() {
    return factory.buildIdSet();
  }
  
  /**
   * Returns a map which contains keys mapping ids to the associated 
   * metadata.  
   *
   * @param range The range to query  
   * @return The map containg the keys 
   */
  public SortedMap scanMetadata(IdRange range) {
    return new RedBlackMap();
  }
  
  /**
    * Returns a map which contains keys mapping ids to the associated 
   * metadata.  
   *
   * @return The treemap mapping ids to metadata 
   */
  public SortedMap scanMetadata() {
    return new RedBlackMap();
  }
  
  /**
   * Returns the submapping of ids which have metadata less than the provided
   * value.
   *
   * @param value The maximal metadata value 
   * @return The submapping
   */
  public SortedMap scanMetadataValuesHead(Object value) {
    return new RedBlackMap();
  }
  
  /**
   * Returns the submapping of ids which have metadata null
   *
   * @return The submapping
   */
  public SortedMap scanMetadataValuesNull() {
    return new RedBlackMap();
  }

  /**
   * Returns the maximum size of the cache, in bytes. The result
   * is returned via the receiveResult method on the provided
   * Continuation with an Integer representing the size.
   *
   * @param c The command to run once the operation is complete
   */
  public long getMaximumSize() {
    return 0;
  }

  /**
   * Returns the total size of the stored data in bytes. The result
   * is returned via the receiveResult method on the provided
   * Continuation with an Integer representing the size.
   *
   * @param c The command to run once the operation is complete
   * @return The total size, in bytes, of data stored.
   */
  public long getTotalSize() {
    return 0;
  }
  
  /**
   * Returns the number of Ids currently stored in the catalog
   *
   * @return The number of ids in the catalog
   */
  public int getSize() {
    return 0;
  }

  /**
   * Sets the maximum size of the cache, in bytes. Setting this
   * value to a smaller value than the current value may result in
   * object being evicted from the cache.
   *
   * @param size The new maximum size, in bytes, of the cache.
   * @param c The command to run once the operation is complete
   * @return The success or failure of the setSize operation
   * (through receiveResult on c).
   */
  public void setMaximumSize(final int size, final Continuation c) {
    c.receiveResult(Boolean.TRUE);
  }
  
  /**
   * Method which is used to erase all data stored in the Catalog.  
   * Use this method with care!
   *
   * @param c The command to run once done
   */
  public void flush(Continuation c) {
    c.receiveResult(Boolean.TRUE);
  }
}
