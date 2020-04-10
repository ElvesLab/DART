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

import java.io.Serializable;
import rice.p2p.commonapi.Id;

public class ObjectDescriptor implements Serializable, Comparable<ObjectDescriptor> {
  
  private static final long serialVersionUID = -3035115249019556223L;
  
  public Id key;
  public long version;
  public long currentLifetime;
  public long refreshedLifetime;
  public int size;
  
  public ObjectDescriptor(Id key, long version, long currentLifetime, long refreshedLifetime, int size) {
    this.key = key;
    this.currentLifetime = currentLifetime;
    this.refreshedLifetime = refreshedLifetime;
    this.size = size;
    this.version = version;
  }
  
  public String toString() {
    return "objDesc["+key.toStringFull()+"v"+version+", lt="+currentLifetime+", rt="+refreshedLifetime+", size="+size+"]";
  }
  
  public boolean isAliveAt(long pointInTime) {
    return (currentLifetime > pointInTime) || (refreshedLifetime > pointInTime);
  }

  public int compareTo(ObjectDescriptor other) {
    ObjectDescriptor metadata = (ObjectDescriptor) other;
    
    int result = this.key.compareTo(metadata.key);
    if (result != 0)
      return result;
      
    if (metadata.version > this.version)
      return -1;
    if (metadata.version < this.version)
      return 1;
    
    if (metadata.currentLifetime > this.currentLifetime) 
      return -1;
    if (metadata.currentLifetime < this.currentLifetime) 
      return 1;

    if (metadata.refreshedLifetime > this.refreshedLifetime) 
      return -1;
    if (metadata.refreshedLifetime < this.refreshedLifetime) 
      return 1;

    if (metadata.size > this.size) 
      return -1;
    else if (metadata.size < this.size) 
      return 1;

    return 0;
  }
};

