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
/*
 * Created on Jun 8, 2006
 */
package rice.pastry.socket.nat;

import java.io.IOException;
import java.net.InetAddress;

/**
 * This is supposed to open a hole in the Firewall, usually using UPnP.
 * 
 * @author Jeff Hoye
 */
public interface NATHandler {

  /**
   * Search for the firewall on the NIC specified by the bindAddress
   * 
   * @param bindAddress the network to find the firewall on
   * @return
   * @throws IOException
   */
  public InetAddress findFireWall(InetAddress bindAddress) throws IOException;
  
  /**
   * The neame of the firewall's external address.  null if there is no firewall.
   * 
   * @return
   */
  InetAddress getFireWallExternalAddress();

  /**
   * Search for an available port forwarding, starting with the external address specified.  The internal 
   * one is given so you can detect that the rule was already in place.
   * 
   * @param internal
   * @param external
   * @return
   * @throws IOException
   */
  public int findAvailableFireWallPort(int internal, int external, int tries, String appName) throws IOException;
  public void openFireWallPort(int local, int external, String appName) throws IOException;

}
