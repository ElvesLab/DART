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
import rice.p2p.multiring.MultiringNodeHandleSet;
import rice.p2p.past.*;

/**
 * @(#) LookupMessage.java
 *
 * This class represents a request for all of the replicas of a given object.
 *
 * @version $Id: LookupHandlesMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * result should be MultiringNodeHandleSet
 *
 * @author Alan Mislove
 * @author Ansley Post
 * @author Peter Druschel
 */
public class LookupHandlesMessage extends ContinuationMessage {
  public static final short TYPE = 5;

  // the id to fetch
  private Id id;

  // the number of replicas to fetch
  private int max;
   
  /**
   * Constructor
   *
   * @param uid The unique id
   * @param id The location to be stored
   * @param max The number of replicas
   * @param source The source address
   * @param dest The destination address
   */
  public LookupHandlesMessage(int uid, Id id, int max, NodeHandle source, Id dest) {    
    super(uid, source, dest);

    this.id = id;
    this.max = max;
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
   * Method which returns the number of replicas
   *
   * @return The number of replicas to fetch
   */
  public int getMax() {
    return max;
  }

  /**
    * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  public String toString() {
    return "[LookupHandlesMessage (response " + isResponse() + " " + response + ") for " + id + " max " + max + "]";
  }

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version        
    serializeHelper(buf);
  }
  
  /**
   * So that it can be subclassed without serializing a version here
   * @param buf
   * @throws IOException
   */
  protected void serializeHelper(OutputBuffer buf) throws IOException {
    if (response != null && response instanceof NodeHandleSet) {
      super.serialize(buf, false); 
      NodeHandleSet set = (NodeHandleSet)response;
      buf.writeShort(set.getType());
      set.serialize(buf);
    } else {
      super.serialize(buf, true);       
    }
    buf.writeInt(max);
    buf.writeShort(id.getType());
    id.serialize(buf);
  }
  
  public static LookupHandlesMessage build(InputBuffer buf, Endpoint endpoint) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new LookupHandlesMessage(buf, endpoint);
      default:
        throw new IOException("Unknown Version: "+version);        
    }
  }  
  
  protected LookupHandlesMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
    super(buf, endpoint);    
    // if called super.serializer(x, true) these will be set
    if (serType == S_SUB) {
      short type = buf.readShort();
      response = endpoint.readNodeHandleSet(buf, type);
    }
    max = buf.readInt();    
    id = endpoint.readId(buf, buf.readShort());
  }
}

