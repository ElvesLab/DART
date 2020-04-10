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
package rice.pastry.direct.proximitygenerators;

import rice.environment.random.RandomSource;
import rice.pastry.direct.NodeRecord;
import rice.pastry.direct.ProximityGenerator;

public class EuclideanProximityGenerator implements ProximityGenerator{
  int side;
  RandomSource random;
  
  /**
   * Constructor.
   */
  public EuclideanProximityGenerator(int maxDiameter) {
    side = (int)(maxDiameter/Math.sqrt(2.0));
  }

  public NodeRecord generateNodeRecord() {
    return new EuclideanNodeRecord(); 
  }
  
  /**
   * Initialize a random Euclidean NodeRecord
   *
   * @version $Id: EuclideanNetwork.java 3613 2007-02-15 14:45:14Z jstewart $
   * @author amislove
   */
  private class EuclideanNodeRecord implements NodeRecord {
    /**
     * The euclidean position.
     */
    public int x, y;

    public boolean alive;

    /**
     * Constructor for NodeRecord.
     *
     * @param nh 
     */
    public EuclideanNodeRecord() {
      x = random.nextInt() % side;
      y = random.nextInt() % side;

      alive = true;
    }

    public float proximity(NodeRecord that) {
      return Math.round((networkDelay(that)*2.0));
    }
    
    public float networkDelay(NodeRecord that) {
      EuclideanNodeRecord nr = (EuclideanNodeRecord)that;
      int dx = x - nr.x;
      int dy = y - nr.y;
      
      float ret = (float)Math.sqrt(dx * dx + dy * dy);
//      int ret = (int)Math.round(sqrt);
      if ((ret < 2.0) && !this.equals(that)) return (float)2.0;
      
      return ret;
    }
    
    public String toString() {
      return "ENR("+x+","+y+")"; 
    }

    public void markDead() {
    }
    
  }

  public void setRandom(RandomSource random) {
    this.random = random;
  }  

}
