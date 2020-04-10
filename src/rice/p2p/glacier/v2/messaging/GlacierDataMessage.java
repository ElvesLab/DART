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
package rice.p2p.glacier.v2.messaging;

import java.io.IOException;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;
import rice.p2p.glacier.v2.Manifest;
import rice.p2p.glacier.Fragment;
import rice.p2p.glacier.FragmentKey;

public class GlacierDataMessage extends GlacierMessage {

  public static final short TYPE = 1;
  
  protected FragmentKey[] keys;
  protected Fragment[] fragments;
  protected Manifest[] manifests;

  public GlacierDataMessage(int uid, FragmentKey key, Fragment fragment, Manifest manifest, NodeHandle source, Id dest, boolean isResponse, char tag) {
    this(uid, new FragmentKey[] { key }, new Fragment[] { fragment }, new Manifest[] { manifest }, source, dest, isResponse, tag);
  }

  public GlacierDataMessage(int uid, FragmentKey[] keys, Fragment[] fragments, Manifest[] manifests, NodeHandle source, Id dest, boolean isResponse, char tag) {
    super(uid, source, dest, isResponse, tag);

    this.keys = keys;
    this.fragments = fragments;
    this.manifests = manifests;
  }

  public int numKeys() {
    return keys.length;
  }

  public FragmentKey getKey(int index) {
    return keys[index];
  }

  public Fragment getFragment(int index) {
    return fragments[index];
  }

  public Manifest getManifest(int index) {
    return manifests[index];
  }

  public String toString() {
    return "[GlacierData for " + keys[0] + " ("+(numKeys()-1)+" more keys)]";
  }
  

  /***************** Raw Serialization ***************************************/
  public short getType() {
    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeByte((byte)0); // version    
    super.serialize(buf);
    
    int l = fragments.length;
    buf.writeInt(l);
    for (int i = 0; i < l; i++) {
      if (fragments[i] == null) {
        buf.writeBoolean(false);        
      } else {
        buf.writeBoolean(true);
        fragments[i].serialize(buf);
      }
    }
    
    l = keys.length;
    buf.writeInt(l);
    for (int i = 0; i < l; i++) {
      keys[i].serialize(buf);
    }
    
    l = manifests.length;
    buf.writeInt(l);
    for (int i = 0; i < l; i++) {
      if (manifests[i] == null) {
        buf.writeBoolean(false);        
      } else {
        buf.writeBoolean(true);
        manifests[i].serialize(buf);
      }
    }
  }
  
  public static GlacierDataMessage build(InputBuffer buf, Endpoint endpoint) throws IOException {
    byte version = buf.readByte();
    switch(version) {
      case 0:
        return new GlacierDataMessage(buf, endpoint);
      default:
        throw new IOException("Unknown Version: "+version);
    }
  }
    
  private GlacierDataMessage(InputBuffer buf, Endpoint endpoint) throws IOException {
    super(buf, endpoint); 

    fragments = new Fragment[buf.readInt()];
    for (int i = 0; i < fragments.length; i++) {
      if (buf.readBoolean())
        fragments[i] = new Fragment(buf);
    }
    
    keys = new FragmentKey[buf.readInt()];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = new FragmentKey(buf, endpoint);
    }
    
    manifests = new Manifest[buf.readInt()];
    for (int i = 0; i < manifests.length; i++) {
      if (buf.readBoolean())
        manifests[i] = new Manifest(buf);
    }
  }
}

