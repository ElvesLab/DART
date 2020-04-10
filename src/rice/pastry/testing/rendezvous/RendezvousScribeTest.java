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

package rice.pastry.testing.rendezvous;
import java.io.IOException;
import java.io.PrintStream;

import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.p2p.scribe.Topic;
import rice.p2p.scribe.testing.RawScribeRegrTest;

/**
 * @(#) DistScribeRegrTest.java Provides regression testing for the Scribe service using distributed
 * nodes.
 *
 * @version $Id: ScribeRegrTest.java 3157 2006-03-19 12:16:58Z jeffh $
 * @author Alan Mislove
 */

public class RendezvousScribeTest /*extends RawScribeRegrTest*/ {

//  public RendezvousScribeTest(Environment env) throws IOException {
//    super(env);
//  }
  
  /**
   * Usage: DistScribeRegrTest [-port p] [-bootstrap host[:port]] [-nodes n] [-protocol (direct|wire|rendezvous)]
   * [-help]
   *
   * @param args DESCRIBE THE PARAMETER
   */
  public static void main(String args[]) throws IOException {
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-tofile")) {
        System.setOut(new PrintStream("delme.txt"));
        System.setErr(System.out);
        break;
      }
    }
    
    Environment env = RawScribeRegrTest.parseArgs(args);
    
    Parameters p = env.getParameters();
//    p.setInt("org.mpisws.p2p.transport_loglevel", Logger.FINE);
//    p.setInt("org.mpisws.p2p.transport.rendezvous_loglevel", Logger.FINE);
//    p.setInt("org.mpisws.p2p.transport.wire_loglevel", Logger.FINE);
//    p.setInt("rice.pastry.socket.nat.rendezvous_loglevel", Logger.FINE);
    
//    p.setInt("org.mpisws.p2p.transport.rendezvous.RendezvousTransportLayerImpl_loglevel", Logger.FINEST);
//    p.setInt("rice.pastry.socket.nat.rendezvous.RendezvousRouterStrategy_loglevel", Logger.FINEST);
//    p.setInt("rice.pastry.socket.nat.rendezvous.RendezvousApp_loglevel", Logger.FINE);    
//    p.setInt("loglevel", Logger.INFO);
//    p.setInt("rice.pastry.socket.nat.rendezvous.RendezvousSocketPastryNodeFactory_loglevel", Logger.FINE);
//    p.setInt("rice.p2p.scribe.testing.RawScribeRegrTest_loglevel", Logger.FINE);
    
    p.setBoolean("rendezvous_test_firewall", true);
    p.setBoolean("rendezvous_test_makes_bootstrap", true);
    p.setFloat("rendezvous_test_num_firewalled", 0.5f);
    
    RawScribeRegrTest scribeTest = new RawScribeRegrTest(env);
    
    scribeTest.start();
    env.destroy();
  }
}














