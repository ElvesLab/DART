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

import rice.p2p.past.gc.GCPastContent;
import rice.p2p.past.*;
import rice.p2p.past.gc.*;
import rice.p2p.commonapi.Id;
import rice.p2p.glacier.VersionKey;
import java.security.*;
import java.io.*;

public class Aggregate implements GCPastContent {
  protected GCPastContent[] components;
  protected Id[] pointers;
  protected Id myId;
  
  private static final long serialVersionUID = -4891386773008082L;
  
  public Aggregate(GCPastContent[] components, Id[] pointers) {
    this.components = components;
    this.myId = null;
    this.pointers = pointers;
  }
  
  public void setId(Id myId) {
    this.myId = myId;
  }
  
  public Id getId() {
    return myId;
  }
  
  public Id[] getPointers() {
    return pointers;
  }
  
  public int numComponents() {
    return components.length;
  }
  
  public GCPastContent getComponent(int index) {
    return components[index];
  }
  
  public long getVersion() {
    return 0;
  }
  
  public boolean isMutable() {
    return false;
  }
  
  public PastContent checkInsert(rice.p2p.commonapi.Id id, PastContent existingContent) throws PastException {
    if (existingContent == null) {
      return this;
    } else {
      return existingContent;
    }
  }

  public byte[] getContentHash() {
    byte[] bytes = null;
    
    try {
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);

      objectStream.writeObject(components);
      objectStream.writeObject(pointers);
      objectStream.flush();

      bytes = byteStream.toByteArray();
    } catch (IOException ioe) {
      // too bad we don't have a good way to throw a serialziation exception
      return null;
    }
    
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("SHA");
    } catch (NoSuchAlgorithmException e) {
      return null;
    }

    md.reset();
    md.update(bytes);
    
    return md.digest();
  }

  public PastContentHandle getHandle(Past local) {
    return new AggregateHandle(local.getLocalNodeHandle(), myId, getVersion(), GCPast.INFINITY_EXPIRATION);
  }

  public GCPastContentHandle getHandle(GCPast local, long expiration) {
    return new AggregateHandle(local.getLocalNodeHandle(), myId, getVersion(), expiration);
  }

  public GCPastMetadata getMetadata(long expiration) {
    return new GCPastMetadata(expiration);
  }
};
