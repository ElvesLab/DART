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
import rice.pastry.*;
import rice.pastry.dist.*;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.*;

import java.util.*;
import java.io.IOException;
import java.net.*;

/**
 * a regression test suite for pastry with "distributed" nodes. All nodes are on
 * one physical machine, but they communicate through one of the network
 * transport protocols, i.e., RMI or WIRE.
 * 
 * See the usage for more information, the -protocol option can be used to
 * specify which protocol to run the test with.
 * 
 * @version $Id: DistPastryRegrTest.java 4654 2009-01-08 16:33:07Z jeffh $
 * 
 * @author Alan Mislove
 */

public class DistPastryRegrTest extends PastryRegrTest {

  private static int port = 5009;

  private static String bshost;

  private static int bsport = 5009;

  private static int numnodes = 10;

  private static int protocol = DistPastryNodeFactory.PROTOCOL_DEFAULT;

  private InetSocketAddress bsaddress;

  static {
    try {
      bshost = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      System.out.println("Error determining local host: " + e);
    }
  }

  // constructor

  public DistPastryRegrTest(Environment env) throws IOException {
    super(env);

    // we need to wrap the TreeMap to synchronize it
    // -- it is shared among multiple virtual nodes
    pastryNodesSorted = Collections.synchronizedSortedMap(pastryNodesSorted);

    factory = DistPastryNodeFactory.getFactory(new IPNodeIdFactory(InetAddress.getLocalHost(), port, env),
        protocol, port, env);

    try {
      bsaddress = new InetSocketAddress(bshost, bsport);
    } catch (Exception e) {
      System.out.println("ERROR (init): " + e);
    }
  }

  /**
   * Gets a handle to a bootstrap node.
   * 
   * @param firstNode true if bootstraping the first virtual node on this host
   * @return handle to bootstrap node, or null.
   */
  protected NodeHandle getBootstrap(boolean firstNode) {
    if (firstNode)
      return ((SocketPastryNodeFactory) factory).getNodeHandle(bsaddress);
    else {
      InetSocketAddress addr = null;
      try {
        addr = new InetSocketAddress(InetAddress.getLocalHost().getHostName(),
            port);
      } catch (UnknownHostException e) {
        System.out.println(e);
      }
      return ((SocketPastryNodeFactory) factory).getNodeHandle(addr);
    }
  }

  /**
   * process command line args, set the RMI security manager, and start the RMI
   * registry. Standard gunk that has to be done for all Dist apps.
   */
  private static void doInitstuff(String args[]) {
    // process command line arguments

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-help")) {
        System.out
            .println("Usage: DistPastryRegrTest [-port p] [-protocol (rmi|wire|socket)] [-nodes n] [-bootstrap host[:port]] [-help]");
        System.exit(1);
      }
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-port") && i + 1 < args.length) {
        int p = Integer.parseInt(args[i + 1]);
        if (p > 0)
          port = p;
        break;
      }
    }

    bsport = port; // make sure bsport = port, if no -bootstrap argument is
                   // provided
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-bootstrap") && i + 1 < args.length) {
        String str = args[i + 1];
        int index = str.indexOf(':');
        if (index == -1) {
          bshost = str;
          bsport = port;
        } else {
          bshost = str.substring(0, index);
          bsport = Integer.parseInt(str.substring(index + 1));
          if (bsport <= 0)
            bsport = port;
        }
        break;
      }
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-nodes") && i + 1 < args.length) {
        int n = Integer.parseInt(args[i + 1]);
        if (n > 0)
          numnodes = n;
        break;
      }
    }

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-protocol") && i + 1 < args.length) {
        String s = args[i + 1];

        //          if (s.equalsIgnoreCase("wire"))
        //            protocol = DistPastryNodeFactory.PROTOCOL_WIRE;
        //          else if (s.equalsIgnoreCase("rmi"))
        //            protocol = DistPastryNodeFactory.PROTOCOL_RMI;
        //          else
        if (s.equalsIgnoreCase("socket"))
          protocol = DistPastryNodeFactory.PROTOCOL_SOCKET;
        else
          System.out.println("ERROR: Unsupported protocol: " + s);

        break;
      }
    }
  }

  /**
   * wire protocol specific handling of the application object e.g., RMI may
   * launch a new thread
   * 
   * @param pn pastry node
   * @param app newly created application
   */
  protected void registerapp(PastryNode pn, RegrTestApp app) {
  }

  // do nothing in the DIST world
  public boolean simulate() {
    return false;
  }

  public synchronized void pause(int ms) {
    System.out.println("Waiting " + ms + "ms...");
    try {
      wait(ms);
    } catch (InterruptedException e) {
    }
  }

  public boolean isReallyAlive(NodeHandle nh) {
    // xxx
    return false;
  }

  protected void killNode(PastryNode pn) {
    pn.destroy();
    pause(50000);
  }

  /**
   * Usage: DistRegrPastryTest [-port p] [-protocol (wire|rmi)] [-nodes n]
   * [-bootstrap host[:port]] [-help]
   */

  public static void main(String args[]) throws IOException {
    doInitstuff(args);
    DistPastryRegrTest pt = new DistPastryRegrTest(new Environment());
    mainfunc(pt, args, numnodes /* n */, 1 /* d */, 1/* k */, 20/* m */, 4/* conc */);
  }
}
