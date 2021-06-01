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
import java.util.Map;

import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 * 
 * Serialized version
 * byte IPversion (4 or 6)
 * byte[] address (4 or 16 bytes)
 * short port
 * 
 * @author Jeff Hoye
 *
 */
public class InetSocketAddressSerializer implements Serializer<InetSocketAddress>, org.mpisws.p2p.transport.util.Serializer<InetSocketAddress> {
  public static final byte IPV4 = 4;
  public static final byte IPV6 = 6;
  public static final int IPV4_BYTES = 4;
  public static final int IPV6_BYTES = 16;

  public InetSocketAddress deserialize(InputBuffer b, InetSocketAddress i,
      Map<String, Object> options) throws IOException {
    return deserializeAddress(b);
  }

  public void serialize(InetSocketAddress i, OutputBuffer b) throws IOException {
    serializeAddress(i,b);
  }

  public static InetSocketAddress deserializeAddress(InputBuffer b) throws IOException {
    byte version = b.readByte();
    byte[] addr;
    
    switch(version) {
    case IPV4:
      addr = new byte[IPV4_BYTES];
      break;
    case IPV6:
      addr = new byte[IPV6_BYTES];
      break;      
    default:
      throw new IOException("Incorrect IP version, expecting 4 or 6, got "+version);
    }
    b.read(addr);
    short port = b.readShort();    
    return new InetSocketAddress(InetAddress.getByAddress(addr),0xFFFF & port);
  }

  public static void serializeAddress(InetSocketAddress i, OutputBuffer b) throws IOException {
    byte[] addr = i.getAddress().getAddress();
    // write version
    switch (addr.length) {
    case IPV4_BYTES:
      b.writeByte(IPV4);
      break;
    case IPV6_BYTES:
      b.writeByte(IPV6);
      break;
    default:
      throw new IOException("Incorrect number of bytes for IPaddress, expecting 4 or 16, got "+addr.length);
    }
    // write addr
    b.write(addr,0,addr.length);
    b.writeShort((short)i.getPort());    
  }
  
  public int getSerializedLength(InetSocketAddress i) {
    return i.getAddress().getAddress().length+2+1;  // address+port+header
  }

  public InetSocketAddress deserialize(InputBuffer buf) throws IOException {
    return deserializeAddress(buf);
  }

}
