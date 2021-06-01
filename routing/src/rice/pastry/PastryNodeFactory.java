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

package rice.pastry;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Node;
import rice.pastry.leafset.*;
import rice.pastry.routing.*;

import java.io.*;
import java.util.*;

/**
 * The interface to an object which can construct PastryNodes.
 *
 * @version $Id: PastryNodeFactory.java 4131 2008-02-27 18:27:49Z jeffh $
 *
 * @author Andrew Ladd
 * @author Alan Mislove
 * @author Merziyah Poonawala
 * @author Abhishek Ray
 */
public abstract class PastryNodeFactory {

  
  // max number of handles stored per routing table entry
  protected final byte rtMax;

  // leafset size
  protected final byte lSetSize;

  protected final byte rtBase;
  

  protected Environment environment;
  
  protected Logger logger;
  
  public PastryNodeFactory(Environment env) {
    this.environment = env;
    rtMax = (byte)environment.getParameters().getInt("pastry_rtMax");
    rtBase = (byte)environment.getParameters().getInt("pastry_rtBaseBitLength");
    lSetSize = (byte)environment.getParameters().getInt("pastry_lSetSize");
    logger = env.getLogManager().getLogger(getClass(), null);
  }
  
  /**
   * Call this to construct a new node of the type chosen by the factory.
   *
   * @deprecated use newNode() then call PastryNode.boot(address);
   * @param bootstrap The node handle to bootstrap off of
   */
  public abstract PastryNode newNode(NodeHandle bootstrap);
  public abstract PastryNode newNode() throws IOException;

  /**
   * Call this to construct a new node of the type chosen by the factory, with
   * the given nodeId.
   *
   * @deprecated use newNode(nodeId) then call PastryNode.boot(address);
   * @param bootstrap The node handle to bootstrap off of
   * @param nodeId The nodeId of the new node
   */
  public abstract PastryNode newNode(NodeHandle bootstrap, Id nodeId);  
  public abstract PastryNode newNode(Id nodeId) throws IOException;  
  
  public Environment getEnvironment() {
    return environment;  
  }
  

//  abstract public PastryNode newNode(Id id) throws IOException;
//
//  public Node newNode() throws IOException {
//    // TODO Auto-generated method stub
//    return null;
//  }
}
