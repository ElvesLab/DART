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
package org.mpisws.p2p.transport.networkinfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;

import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;

import rice.Continuation;
import rice.Destructable;
import rice.p2p.commonapi.Cancellable;

/**
 * First, call getMyInetAddress to find the external address.  
 * Second, try to use UPnP or user configured portForwarding to configure the NAT
 * Third, call verifyConnectivity to make sure it all worked
 * 
 * @author Jeff Hoye
 *
 */
public interface InetSocketAddressLookup extends Destructable {

  /**
   * find nodes outside of our firewall so I can boot
   * 
   * @param target
   * @param continuation
   * @param options
   * @return
   */
  public Cancellable getExternalNodes(InetSocketAddress bootstrap,
      Continuation<Collection<InetSocketAddress>, IOException> c,
      Map<String, Object> options);


  
  /**
   * Returns the local node's InetSocketAddress
   * 
   * @param bootstrap who to ask
   * @param c where the return value is delivered
   * @param options can be null
   * @return you can cancel the operation
   */
  public Cancellable getMyInetAddress(InetSocketAddress bootstrap, 
      Continuation<InetSocketAddress, IOException> c, 
      Map<String, Object> options);
  
  /** 
   * Verify that I have connectivity by using a third party.
   * 
   * Opens a socket to probeAddress.
   * probeAddress calls ProbeStrategy.requestProbe()
   * probeStrategy forwards the request to another node "Carol"
   * Carol probes local
   * 
   * @param bootstrap
   * @param proxyAddr
   * @return
   */
  public Cancellable verifyConnectivity(MultiInetSocketAddress local, 
      InetSocketAddress probeAddresses, 
      ConnectivityResult deliverResultToMe, 
      Map<String, Object> options);  
}
