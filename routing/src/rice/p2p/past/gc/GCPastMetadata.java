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

import java.io.*;

import rice.p2p.past.*;

/**
 * @(#) GCPastMetadata.java
 * 
 * Class which is used as the metadata storage for the GC past implementation.
 * Basically wraps the long timestamp.
 *
 * @version $Id: GCPastMetadata.java 4654 2009-01-08 16:33:07Z jeffh $
 * @author Peter Druschel 
 */
public class GCPastMetadata implements Serializable, Comparable<GCPastMetadata> {
  
  // serialver for backwards compatibility
  private static final long serialVersionUID = -2432306227012003387L;

  // the expiration time
  protected long expiration;
  
  /**
   * Constructor.
   *
   * @param msg The string representing the error.
   */
  public GCPastMetadata(long expiration) {
    this.expiration = expiration;
  }
  
  /**
   * Method which returns the expiration time
   *
   * @return The contained expiration time
   */
  public long getExpiration() {
    return expiration;
  }
  
  /**
   * Method which sets the expiration time
   *
   * @param The new expiration time
   */
  public GCPastMetadata setExpiration(long expiration) {
    return new GCPastMetadata(expiration);
  }
  
  public boolean equals(Object o) {
    return ((GCPastMetadata) o).expiration == expiration;
  }
  
  public int hashCode() {
    return (int) expiration;
  }
  
  /**
   * Comparable, returns -1 if less, 0 if equal, and 1 if greater
   *
   * @param other The object ot compare to
   * @return the comparison
   */
  public int compareTo(GCPastMetadata other) {
    GCPastMetadata metadata = (GCPastMetadata) other;
    
    if (metadata.expiration > expiration) 
      return -1;
    else if (metadata.expiration < expiration) 
      return 1;
    else
      return 0;
  }
  
  public String toString() {
    return "GCPMetadata " + expiration;
  }
  
  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
    ois.defaultReadObject();
    
    if (expiration == 1096560000000L)
      expiration = GCPastImpl.DEFAULT_EXPIRATION;
  }
}





