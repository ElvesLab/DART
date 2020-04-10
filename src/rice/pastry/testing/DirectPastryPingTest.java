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

import rice.environment.Environment;
import rice.environment.params.simple.SimpleParameters;
import rice.environment.time.simulated.DirectTimeSource;
import rice.pastry.*;
import rice.pastry.direct.*;
import rice.pastry.standard.*;
import rice.pastry.join.*;

import java.util.*;
import java.io.*;
import java.lang.*;

/**
 * DirectPastryPingTest
 * 
 * A performance test suite for pastry.
 * 
 * @version $Id: DirectPastryPingTest.java 3613 2007-02-15 14:45:14Z jstewart $
 * 
 * @author Rongmei Zhang
 */

public class DirectPastryPingTest {

  public DirectPastryPingTest() {
  }

  private static boolean parseInput(String in, Environment environment) {
    StringTokenizer tokened = new StringTokenizer(in);
    if (!tokened.hasMoreTokens()) {
      return false;
    }

    String token = tokened.nextToken();
    int n = -1;
    int k = -1;
    SinglePingTest spt;
    int i;

    if (token.startsWith("q")) { //quit
      return true;
    } else if (token.startsWith("s")) { //standalone
      Vector trlist = new Vector();

      //      k = 200000;

      for (i = 0; i < 8; i++) {
        n = k = (i + 1) * 1000;
        PingTestRecord tr = new PingTestRecord(n, k, environment.getParameters().getInt("pastry_rtBaseBitLength"));
        spt = new SinglePingTest(tr, environment);
        spt.test();
        System.out.println(tr.getNodeNumber() + "\t" + tr.getAveHops() + "\t"
            + tr.getAveDistance());
        //    System.out.println( "probability of " + i + " hops: " +
        // tr.getProbability()[i] );
      }
      /*
       * for( i=0; i <10; i++ ){ trlist.addElement( new PingTestRecord(
       * (i+1)*10000, k ) ); spt = new SinglePingTest(
       * (PingTestRecord)(trlist.lastElement()) ); spt.test(); PingTestRecord tr =
       * (PingTestRecord)trlist.elementAt(i); System.out.println(
       * tr.getNodeNumber() + "\t" + tr.getAveHops() +"\t"+ tr.getAveDistance() );
       * System.out.println( "probability of " + i + " hops: " +
       * tr.getProbability()[i] ); }
       */
    }
    return false;
  }

  public static void main(String args[]) throws IOException {
    boolean quit = false;
    Environment env = Environment.directEnvironment();
    BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
    String command = null;

    System.out.println("Usage: s - run standalone test");
    System.out.println("       q - quit");

    while (!quit) {
      try {
        command = input.readLine();
      } catch (Exception e) {
        System.out.println(e);
      }
      quit = parseInput(command, env);
    }

  }
}

