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
 * @(#) PersistenceManager.java
 *
 * @author Ansley Post
 * @author Alan Mislove
 * 
 * @version $Id: MemoryStorage.java 4654 2009-01-08 16:33:07Z jeffh $
 */
import java.io.*;
import java.util.*;
import java.util.zip.*;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.util.*;

/**
 * This class is an implementation of Storage which provides
 * in-memory storage. This class is specifically *NOT* designed
 * to provide persistent storage, and simply functions as an
 * enhanced hash table.
 */
@SuppressWarnings("unchecked")
public class MemoryStorage implements Storage {

  // the map used to store the data
  private HashMap storage;
  
  // the map used to store the metadata
  private ReverseTreeMap metadata;

  // the current list of Ids
  private IdSet idSet;
  
  // the current total size
  private int currentSize;

  // the factory for manipulating the ids
  private IdFactory factory;
  
  /**
   * Builds a MemoryStorage object.
   *
   * @param factory The factory to build protocol-specific Ids from.
   */
  public MemoryStorage(IdFactory factory) {
    this.factory = factory;
    idSet = factory.buildIdSet();
    storage = new HashMap();
    metadata = new ReverseTreeMap();
    currentSize = 0;
  } 
  
  /**
   * Method which is used to erase all data stored in the Storage.  
   * Use this method with care!
   *
   * @param c The command to run once done
   */
  public void flush(Continuation c) {
    storage = new HashMap();
    metadata = new ReverseTreeMap();
    idSet = factory.buildIdSet();
    currentSize = 0;
    
    c.receiveResult(Boolean.TRUE);
  }
  
  /**
   * Renames the given object to the new id.  This method is potentially faster
   * than store/cache and unstore/uncache.
   *
   * @param oldId The id of the object in question.
   * @param newId The new id of the object in question.
   * @param c The command to run once the operation is complete
   */
  public void rename(Id oldId, Id newId, Continuation c) {
    if (! idSet.isMemberId(oldId)) {
      c.receiveResult(new Boolean(false));
      return;
    }
    
    idSet.removeId(oldId);
    idSet.addId(newId);
    
    storage.put(newId, storage.get(oldId));
    storage.remove(oldId);
    
    metadata.put(newId, metadata.get(oldId));
    metadata.remove(oldId);
    
    c.receiveResult(new Boolean(true));
  }
    
  /**
   * Stores the object under the key <code>id</code>.  If there is already
   * an object under <code>id</code>, that object is replaced.
   *
   * This method completes by calling recieveResult() of the provided continuation
   * with the success or failure of the operation.
   *
   * @param obj The object to be made persistent.
   * @param id The object's id.
   * @param metadata The object's metadata
   * @param c The command to run once the operation is complete
   * @return <code>true</code> if the action succeeds, else
   * <code>false</code>.
   */
  public void store(Id id, Serializable metadata, Serializable obj, Continuation c) {
    if (id == null || obj == null) {
      c.receiveResult(new Boolean(false));
      return;
    }
    
    currentSize += getSize(obj);
    
    this.storage.put(id, obj);
    this.metadata.put(id, metadata);
    idSet.addId(id);
    c.receiveResult(new Boolean(true));
  }

  /**
   * Removes the object from the list of stored objects. If the object was not
   * in the cached list in the first place, nothing happens and <code>false</code>
   * is returned.
   *
   * This method completes by calling recieveResult() of the provided continuation
   * with the success or failure of the operation.
   *
   * @param id The object's persistence id
   * @param c The command to run once the operation is complete
   * @return <code>true</code> if the action succeeds, else
   * <code>false</code>.
   */
  public void unstore(Id id, Continuation c) {
    Object stored = storage.remove(id);
    metadata.remove(id);
    idSet.removeId(id);

    if (stored != null) {
      currentSize -= getSize(stored);
      c.receiveResult(new Boolean(true));
    } else {
      c.receiveResult(new Boolean(false));
    }
  }

  /**
   * Returns whether or not the provided id exists
   *
   * @param id The id to check
   * @return Whether or not the given id is stored
   */
  public boolean exists(Id id) {
    return storage.containsKey(id);
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
    return (Serializable) metadata.get(id);
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
  public void setMetadata(Id id, Serializable metadata, Continuation command) {
    if (exists(id)) 
      this.metadata.put(id, metadata);

    command.receiveResult(new Boolean(exists(id)));
  }

  /**
   * Returns the object identified by the given id, or <code>null</code> if
   * there is no cooresponding object (through receiveResult on c).
   *
   * @param id The id of the object in question.
   * @param c The command to run once the operation is complete
   */
  public void getObject(Id id, Continuation c) {
    c.receiveResult(storage.get(id));
  }

  /**
   * Return the objects identified by the given range of ids. The IdSet 
   * returned contains the Ids of the stored objects. The range is
   * partially inclusive, the lower range is inclusive, and the upper
   * exclusive.
   *
   *
   * NOTE: This method blocks so if the behavior of this method changes and
   * the guys don't fit in memory, this method may be deprecated.
   *
   * @param range The range to query  
   * @return The idset containg the keys 
   */
  public IdSet scan(IdRange range){
    return idSet.subSet(range);
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
    return idSet;
  }
  
  /**
   * Returns a map which contains keys mapping ids to the associated 
   * metadata.  
   *
   * @param range The range to query  
   * @return The map containg the keys 
   */
  public SortedMap scanMetadata(IdRange range) {    
    if (range.isEmpty()) 
      return new RedBlackMap();
    else if (range.getCCWId().equals(range.getCWId())) 
      return scanMetadata();
    else 
      return new ImmutableSortedMap(metadata.keySubMap(range.getCCWId(), range.getCWId()));
  }
  
  /**
   * Returns a map which contains keys mapping ids to the associated 
   * metadata.  
   *
   * @return The treemap mapping ids to metadata 
   */
  public SortedMap scanMetadata() {
    return new ImmutableSortedMap(metadata.keyMap());
  }
  
  /**
   * Returns the submapping of ids which have metadata less than the provided
   * value.
   *
   * @param value The maximal metadata value 
   * @return The submapping
   */
  public SortedMap scanMetadataValuesHead(Object value) {
    return new ImmutableSortedMap(metadata.valueHeadMap(value));
  }
  
  /**
    * Returns the submapping of ids which have metadata null
   *
   * @return The submapping
   */
  public SortedMap scanMetadataValuesNull() {
    return new ImmutableSortedMap(metadata.valueNullMap());
  }

  /**
   * Returns the total size of the stored data in bytes.The result
   * is returned via the receiveResult method on the provided
   * Continuation with an Integer representing the size.
   *
   * @param c The command to run once the operation is complete
   */
  public long getTotalSize() {
    return currentSize;
  }
  
  /**
   * Returns the number of Ids currently stored in the catalog
   *
   * @return The number of ids in the catalog
   */
  public int getSize() {
    return idSet.numElements();
  }

  /**
   * Returns the size of the given object, in bytes.
   *
   * @param obj The object to determine the size of
   * @return The size, in bytes
   */
  private int getSize(Object obj) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(baos)));

      oos.writeObject(obj);
      oos.close();

      return baos.toByteArray().length;
    } catch (IOException e) {
      throw new RuntimeException("Object " + obj + " was not serialized correctly! "+e.toString(),e);
    }
  }
}
