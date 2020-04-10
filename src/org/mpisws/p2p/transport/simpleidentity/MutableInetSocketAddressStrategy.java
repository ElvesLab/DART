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
package org.mpisws.p2p.transport.simpleidentity;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import rice.p2p.util.rawserialization.SimpleOutputBuffer;

/**
 * This is used if you are binding to 0.0.0.0
 * It will see if your local address changed before returning the serialized version.
 * 
 * @author Jeff Hoye
 *
 * @param <InetSocketAddress>
 */
public class MutableInetSocketAddressStrategy implements LocalIdentifierStrategy<InetSocketAddress> {
  int port;  
  Serializer<InetSocketAddress> serializer;
  
  InetAddress lastLocalAddress;
  byte[] lastBytes;
    
  public MutableInetSocketAddressStrategy(int port, Serializer<InetSocketAddress> serializer) {
    this.serializer = serializer;
    this.port = port;
  }
  
  public synchronized byte[] getLocalIdentifierBytes() throws IOException {
    InetAddress newAddr = getLocalAddress();
    if (lastLocalAddress == null || newAddr.equals(lastLocalAddress)) {      
      lastLocalAddress = newAddr;
      lastBytes = serializeAddress(new InetSocketAddress(newAddr, port));
    }
    return lastBytes;
  }

  protected byte[] serializeAddress(InetSocketAddress addr) throws IOException {    
    System.out.println("Address changed to "+addr);
    SimpleOutputBuffer sob = new SimpleOutputBuffer();
    serializer.serialize(addr, sob);
    return sob.getBytes();      
  }
  
  /**
   * Can be overridden to do something special in the case of having multiple NICs
   * 
   * @return the current address to advertise
   */
  protected InetAddress getLocalAddress() throws IOException {
    return InetAddress.getLocalHost();
  }
}
