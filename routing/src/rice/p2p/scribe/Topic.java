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

package rice.p2p.scribe;

import java.io.*;

import rice.*;
import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.*;

/**
 * @(#) Topic.java
 *
 * This class represents a specific topic in the Scribe system.
 *
 * @version $Id: Topic.java 3653 2007-03-27 18:39:52Z jeffh $
 * @author Alan Mislove
 */
public class Topic implements Serializable, Comparable<Topic> {

  /**
   * The Id to which this topic is mapped
   */
  protected Id id;

  /**
   * Constructor which takes an Id for this topic
   *
   * @param id The Id for this topic
   */
  public Topic(Id id) {
    this.id = id;
  }

  /**
   * Constructor which takes a name for this topic
   *
   * @param factory The factory to use when creating the id
   * @param name The name for this topic
   */
  public Topic(IdFactory factory, String name) {
    this.id = getId(factory, name);
  }

  /**
   * Returns the Id to which this topic is mapped
   *
   * @return The id to which this topic is mapped
   */
  public Id getId() {
    return id;
  }

  /**
   * Returns the Id to which the string is mapped
   *
   * @param factory The factory to use when creating the id
   * @param name The string to map
   * @return The id to which this string is mapped
   */
  public static Id getId(IdFactory factory, String name) {
    return factory.buildId(name);
  }

  /**
   * Returns whether this is equal to o or not
   *
   * @param o The object to compare to
   * @return Whether or not they are equal
   */
  public boolean equals(Object o) {
    if (o instanceof Topic) {
      return ((Topic) o).id.equals(id);
    }

    return false;
  }

  /**
   * Returns the hashCode for this topic
   *
   * @return The hashcode for this topic
   */
  public int hashCode() {
    return id.hashCode();
  }

  /**
   * Returns a String representation of this topic
   *
   * @return A String representation of this topic
   */
  public String toString() {
    return "[TOPIC " + id + "]";
  }  

  public Topic(InputBuffer buf, Endpoint endpoint) throws IOException {
    id = endpoint.readId(buf, buf.readShort());
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
    buf.writeShort(id.getType());
    id.serialize(buf); 
  }

  public int compareTo(Topic that) {
    return this.id.compareTo(that.id);
  }
}

