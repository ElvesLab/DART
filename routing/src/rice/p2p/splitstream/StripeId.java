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

package rice.p2p.splitstream;

import rice.p2p.commonapi.*;

/**
 * This class wraps the nodeId object so we can use type checking and allow more readable and
 * understandable code. All it does is subclass the nodeId and provide a constructor that allows the
 * wrapping of a NodeId object to create a concrete subclass
 *
 * @version $Id: StripeId.java 3613 2007-02-15 14:45:14Z jstewart $
 * @author Ansley Post
 * @author Alan Mislove
 */
public class StripeId {

  /**
   * This stripe's Id
   */
  protected Id id;

  /**
   * Constructor that takes in a nodeId and makes a StripeId
   *
   * @param id The Id for this stripe
   */
  public StripeId(Id id) {
    this.id = id;
  }

  /**
   * Gets the Id attribute of the StripeId object
   *
   * @return The Id value
   */
  public Id getId() {
    return id;
  }

  public String toString() {
    return "[StripeId " + id + "]";
  }

  public int hashCode() {
    return id.hashCode();
  }

  public boolean equals(Object o) {
    if (! (o instanceof StripeId)) {
      return false;
    }

    return ((StripeId) o).id.equals(id);
  }
}
