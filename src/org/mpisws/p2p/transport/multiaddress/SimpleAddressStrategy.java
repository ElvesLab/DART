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
package org.mpisws.p2p.transport.multiaddress;

import java.net.InetSocketAddress;

public class SimpleAddressStrategy implements AddressStrategy {

  /**
   * Method which returns the address of this address
   *
   * @return The address
   */
  public InetSocketAddress getAddress(MultiInetSocketAddress local, MultiInetSocketAddress remote) {   
    // start from the outside address, and return the first one not equal to the local address (sans port)
    
    try {
      for (int ctr = 0; ctr < remote.address.length; ctr++) {
        if (!local.address[ctr].getAddress().equals(remote.address[ctr].getAddress())) {
          return remote.address[ctr];
        }
      }
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      // same computer
      return remote.getInnermostAddress();
    }
    return remote.address[remote.address.length-1]; // the last address if we are on the same computer
  }
  
  /**
   * This is for hairpinning support.  The Node is advertising many different 
   * InetSocketAddresses that it could be contacted on.  In a typical NAT situation 
   * this will be 2: the NAT's external address, and the Node's non-routable 
   * address on the Lan.  
   * 
   * The algorithm sees if the external address matches its own external 
   * address.  If it doesn't then the node is on a different lan, use the external.
   * If the external address matches then both nodes are on the same Lan, and 
   * it uses the internal address because the NAT may not support hairpinning.  
   * 
   * @param local my sorted list of InetAddress
   * @return the address I should use to contact the node
   */
//  public InetSocketAddress getAddress(InetAddress[] local) {   
//    // start from the outside address, and return the first one not equal to the local address (sans port)
//    try {
//      for (int ctr = 0; ctr < remote.address.length; ctr++) {
//        if (!remote.address[ctr].getAddress().equals(local[ctr])) {
//          return remote.address[ctr];
//        }
//      }
//    } catch (ArrayIndexOutOfBoundsException aioobe) {
//      String s = "";
//      for (int ctr = 0; ctr < local.length; ctr++) {
//        s+=local[ctr];
//        if (ctr < local.length-1) s+=":";  
//      }
//      throw new RuntimeException("ArrayIndexOutOfBoundsException in "+this+".getAddress("+local.length+")",aioobe);
//    }
//    return remote.address[remote.address.length-1]; // the last address if we are on the same computer
//  }

}
