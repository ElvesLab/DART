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

package rice.p2p.past.messaging;

import java.io.IOException;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.past.*;
import rice.p2p.past.gc.GCId;
import rice.p2p.past.rawserialization.*;

/**
 * @(#) LookupMessage.java
 *
 * This class is the representation of a lookup request (by Id) in Past.
 *
 * response should be PastContent
 *
 * @version $Id: LookupMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
public class LookupMessage extends ContinuationMessage {
  public static final short TYPE = 6;

  // the id to fetch
  private Id id;

  // whether or not this message has been cached
  private boolean cached = false;

  // the list of nodes where this message has been
  private NodeHandle handle;
  
  /**
   * Constructor
   *
   * @param uid The unique id
   * @param id The location to be stored
   * @param useReplicas Whether or not to look for nearest replicas
   * @param source The source address
   * @param dest The destination address
   */
  public LookupMessage(int uid, Id id, NodeHandle source, Id dest) {
    super(uid, source, dest);

    this.id = id;
  }

  /**
   * Method which returns the id
   *
   * @return The contained id
   */
  public Id getId() {
    return id;
  }

  /**
   * Returns whether or not this message has been cached
   *
   * @return Whether or not this message has been cached
   */
  public boolean isCached() {
    return cached;
  }

  /**
   * Sets this message as having been cached.
   */
  public void setCached() {
    cached = true;
  }

  /**
   * Method which is designed to be overridden by subclasses if they need
   * to keep track of where they've been.
   *
   * @param handle The current local handle
   */
  public void addHop(NodeHandle handle) {
    this.handle = handle;
  }

  /**
   * Method which returns the previous hop (where the message was just at)
   *
   * @return The previous hop
   */
  public NodeHandle getPreviousNodeHandle() {
    return handle;
  }

  /**
    * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  public String toString() {
    return "[LookupMessage for " + id + " data " + response + "]";
  }
  
  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version        
    if (response != null && response instanceof RawPastContent) {
      super.serialize(buf, false); 
      RawPastContent rpc = (RawPastContent)response;
      buf.writeShort(rpc.getType());
      rpc.serialize(buf);
    } else {
      super.serialize(buf, true);       
    }
    
    buf.writeBoolean(handle != null);
    if (handle != null) handle.serialize(buf);
    
    buf.writeShort(id.getType());
    id.serialize(buf);      
    buf.writeBoolean(cached);
  }

  public static LookupMessage build(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new LookupMessage(buf, endpoint, pcd);
      default:
        throw new IOException("Unknown Version: "+version);        
    }
  }  
  
  private LookupMessage(InputBuffer buf, Endpoint endpoint, PastContentDeserializer pcd) throws IOException {
    super(buf, endpoint);
    if (serType == S_SUB) {
      short contentType = buf.readShort();
      response = pcd.deserializePastContent(buf, endpoint, contentType);
    }
    if (buf.readBoolean())handle = endpoint.readNodeHandle(buf); 
    try {
      id = endpoint.readId(buf, buf.readShort());
    } catch (IllegalArgumentException iae) {
      System.out.println(iae+" "+this+" serType:"+serType+" UID:"+getUID()+" d:"+dest+" s:"+source);
      throw iae; 
    }
    cached = buf.readBoolean();
  }
}

