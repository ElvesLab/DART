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

import rice.environment.Environment;
import rice.environment.random.RandomSource;
import rice.pastry.direct.NodeRecord;
import rice.pastry.direct.ProximityGenerator;

public class SphereNetworkProximityGenerator implements ProximityGenerator {
  int maxDiameter;
  RandomSource random;

  public SphereNetworkProximityGenerator(int maxDiameter) {
    this.maxDiameter = maxDiameter;
  }


  public NodeRecord generateNodeRecord() {
    return new SphereNodeRecord(); 
  }
  
  /**
   * Initialize a random Sphere NodeRecord
   *
   * @version $Id: SphereNetwork.java 3613 2007-02-15 14:45:14Z jstewart $
   * @author amislove
   */
  private class SphereNodeRecord implements NodeRecord {
    /**
     * DESCRIBE THE FIELD
     */
    public double theta, phi;
    double radius;
    
    /**
     * Constructor for NodeRecord.
     *
     * @param nh DESCRIBE THE PARAMETER
     */
    public SphereNodeRecord() {
      this(Math.asin(2.0 * random.nextDouble() - 1.0),
           2.0 * Math.PI * random.nextDouble());
    }

    public SphereNodeRecord(double theta, double phi) {
      this.theta = theta;
      this.phi = phi;
      radius = maxDiameter/Math.PI;
    }
    
    /**
     * DESCRIBE THE METHOD
     *
     * @param nr DESCRIBE THE PARAMETER
     * @return DESCRIBE THE RETURN VALUE
     */
    public float proximity(NodeRecord that) {
      return (float)Math.round(networkDelay(that)*2.0);
    }
    
    public float networkDelay(NodeRecord that) {
      SphereNodeRecord nr = (SphereNodeRecord)that;
      double ret = (radius * Math.acos(Math.cos(phi - nr.phi) * Math.cos(theta) * Math.cos(nr.theta) +
        Math.sin(theta) * Math.sin(nr.theta)));
      
      if ((ret < 2.0) && !this.equals(that)) return (float)2.0;
      
      return (float)ret;
    }

    public void markDead() {
    }
  }
  
  public void test() {
    System.out.println(new SphereNodeRecord(0,0).proximity(new SphereNodeRecord(0, Math.PI))); 
    System.out.println(new SphereNodeRecord(-1,0).proximity(new SphereNodeRecord(1, Math.PI))); 
    for (int i = 0; i < 100; i++) {
      System.out.println(new SphereNodeRecord().proximity(new SphereNodeRecord())); 
    }
  }
  
  public static void main(String[] argz) {
    System.out.println("hello world"); 
    new SphereNetworkProximityGenerator(Environment.directEnvironment().getParameters().getInt("pastry_direct_max_diameter")).test();    
  }
  
  public void setRandom(RandomSource random) {
    this.random = random;
  }  
}
