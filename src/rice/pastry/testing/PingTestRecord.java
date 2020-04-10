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
package rice.pastry.testing;

import rice.pastry.direct.TestRecord;

/**
 * PingAddress
 * 
 * A performance test suite for pastry.
 * 
 * @version $Id: PingTestRecord.java 3613 2007-02-15 14:45:14Z jstewart $
 * 
 * @author Rongmei Zhang
 */

public class PingTestRecord extends TestRecord {
  private int nIndex;

  private int nHops[];

  private double fProb[];

  private double fHops;

  private double fDistance = 0;

  public PingTestRecord(int n, int k, int baseBitLength) {
    super(n, k);

    nIndex = (int) Math
        .ceil(Math.log(n) / Math.log(Math.pow(2, baseBitLength)));
    nIndex *= 3;
    nHops = new int[nIndex*2];
    fProb = new double[nIndex*2];
  }

  public void doneTest() {
    int i;
    //calculate averages ...
    long sum = 0;
    for (i = 0; i < nIndex; i++) {
      sum += nHops[i] * i;
    }
    fHops = ((double) sum) / nTests;
    fDistance = fDistance / nTests;

    for (i = 0; i < nIndex; i++) {
      fProb[i] = i * nHops[i] / ((double) sum);
    }
  }

  public void addHops(int index) {
    nHops[index]++;
  }

  public void addDistance(double rDistance) {
    fDistance += rDistance;
  }

  public double getAveHops() {
    return fHops;
  }

  public double getAveDistance() {
    return fDistance;
  }

  public double[] getProbability() {
    return fProb;
  }
}

