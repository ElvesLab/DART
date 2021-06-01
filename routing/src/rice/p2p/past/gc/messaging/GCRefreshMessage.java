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

package rice.p2p.past.gc.messaging;

import java.io.IOException;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.past.*;
import rice.p2p.past.messaging.*;
import rice.p2p.past.gc.*;

/**
 * @(#) GCRefreshMessage.java
 *
 * This class represents a message which is an request to extend the lifetime
 * of a set of keys stored in GCPast.
 *
 * response is expected to be Boolean[]
 *
 * @version $Id: GCRefreshMessage.java 3613 2007-02-15 14:45:14Z jstewart $
 *
 * @author Alan Mislove
 */
public class GCRefreshMessage extends ContinuationMessage {
  public static final short TYPE = 11;

  // the list of keys which should be refreshed
  protected GCId[] keys;
  
  /**
   * Constructor which takes a unique integer Id, as well as the
   * keys to be refreshed
   *
   * @param uid The unique id
   * @param keys The keys to be refreshed
   * @param expiration The new expiration time
   * @param source The source address
   * @param dest The destination address
   */
  public GCRefreshMessage(int uid, GCIdSet keys, NodeHandle source, Id dest) {
    super(uid, source, dest);
    
    this.keys = new GCId[keys.numElements()];
    System.arraycopy(keys.asArray(),0,this.keys, 0, this.keys.length);
  }

  /**
   * Method which returns the list of keys
   *
   * @return The list of keys to be refreshed
   */
  public GCId[] getKeys() {
    return keys;
  }

  /**
    * Returns a string representation of this message
   *
   * @return A string representing this message
   */
  public String toString() {
    return "[GCRefreshMessage of " + keys.length + "]";
  }
  
  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version    
    if (response != null && response instanceof Boolean[]) {
      super.serialize(buf, false);       
      Boolean[] array = (Boolean[])response;
      buf.writeInt(array.length);
      for (int i = 0; i < array.length; i++) {
        buf.writeBoolean(array[i].booleanValue()); 
      }
    } else {
      super.serialize(buf, true);       
    }

    buf.writeInt(keys.length);
    for (int i = 0; i < keys.length; i++) {
      buf.writeShort(keys[i].getType());      
      keys[i].serialize(buf);
    }
  }
  
  public static GCRefreshMessage build(InputBuffer buf, Endpoint endpoint) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new GCRefreshMessage(buf, endpoint);
      default:
        throw new IOException("Unknown Version: "+version);        
    }
  }  
  
  private GCRefreshMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
    super(buf, endpoint);
    
    if (serType == S_SUB) {      
      int arrayLength = buf.readInt();      
      Boolean[] array = new Boolean[arrayLength];
      for (int i = 0; i < arrayLength; i++) {
        array[i] = new Boolean(buf.readBoolean()); 
      }
    }
    
    keys = new GCId[buf.readInt()];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = (GCId)endpoint.readId(buf, buf.readShort());
    }
  }
}

