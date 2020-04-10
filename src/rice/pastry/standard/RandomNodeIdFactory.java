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
package rice.pastry.standard;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.random.RandomSource;
import rice.pastry.*;

import java.security.*;

/**
 * Constructs random node ids by SHA'ing consecutive numbers, with random
 * starting value.
 * 
 * @version $Id: RandomNodeIdFactory.java 3613 2007-02-15 14:45:14Z jstewart $
 * 
 * @author Andrew Ladd
 * @author Peter Druschel
 */

public class RandomNodeIdFactory implements NodeIdFactory {
  private long next;

  Environment environment;

  protected Logger logger;
  /**
   * Constructor.
   */

  public RandomNodeIdFactory(Environment env) {
    this.environment = env;
    next = env.getRandomSource().nextLong();
    this.logger = env.getLogManager().getLogger(getClass(), null);
  }

  /**
   * generate a nodeId
   * 
   * @return the new nodeId
   */

  public Id generateNodeId() {

    //byte raw[] = new byte[NodeId.nodeIdBitLength >> 3];
    //rng.nextBytes(raw);

    byte raw[] = new byte[8];
    long tmp = ++next;
    for (int i = 0; i < 8; i++) {
      raw[i] = (byte) (tmp & 0xff);
      tmp >>= 8;
    }

    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("SHA");
    } catch (NoSuchAlgorithmException e) {
      if (logger.level <= Logger.SEVERE) logger.log(
          "No SHA support!");
      throw new RuntimeException("No SHA support!",e);
    }

    md.update(raw);
    byte[] digest = md.digest();

    Id nodeId = Id.build(digest);

    return nodeId;
  }

}

