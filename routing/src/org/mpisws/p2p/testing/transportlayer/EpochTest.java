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
package org.mpisws.p2p.testing.transportlayer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.multiaddress.MultiInetAddressTransportLayerImpl;
import org.mpisws.p2p.transport.multiaddress.MultiInetSocketAddress;
import org.mpisws.p2p.transport.wire.WireTransportLayerImpl;
import org.mpisws.p2p.transport.wire.magicnumber.MagicNumberTransportLayer;

import rice.environment.Environment;
import rice.environment.logging.CloneableLogManager;

public class EpochTest extends TLTest<MultiInetSocketAddress>{
  /**
   * Goes to Alice/Bob
   */
  public static final byte[] GOOD_HDR = {(byte)0xDE,(byte)0xAD,(byte)0xBE,(byte)0xEF};

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TLTest.setUpBeforeClass();
    int startPort = START_PORT;
    logger = env.getLogManager().getLogger(MagicNumberTest.class, null);
    InetAddress addr = InetAddress.getLocalHost();    
//    InetAddress addr = InetAddress.getByName("10.0.0.10");
    
    alice = buildTL("alice", addr, startPort, env);
    bob = buildTL("bob", addr, startPort+1, env);
  }
  
  private static TransportLayer buildTL(String name, InetAddress addr, int port, Environment env) throws IOException {
    Environment env_a = new Environment(
        env.getSelectorManager(), 
        env.getProcessor(), 
        env.getRandomSource(), 
        env.getTimeSource(), 
        ((CloneableLogManager) env.getLogManager()).clone(name),
        env.getParameters(), 
        env.getExceptionStrategy());    
    env.addDestructable(env_a);    
    InetSocketAddress addr_a = new InetSocketAddress(addr,port);
    return
        new MultiInetAddressTransportLayerImpl(new MultiInetSocketAddress(addr_a),
          new MagicNumberTransportLayer<InetSocketAddress>(
            new WireTransportLayerImpl(addr_a,env_a, null),
          env_a, null,GOOD_HDR, 2000),
        env_a, null, null);
  }

  /**
   * To fool eclips's testing harness.
   *
   */
  @Test
  public void bogus() {} 
  
  @Test
  public void testFalseAddress() {
    // have carol open up a connection to alice
    // watch alice get an error b/c the EISA doesn't match carol's IP address
  }
  
  public void testFalseAddressBypass() {
    // disable address verification on alice
    // have carol open up a connection to alice
    // watch alice _not_ get an error b/c the EISA doesn't match carol's IP address
    // enable address verification on alice
  }

  @Override
  public MultiInetSocketAddress getBogusIdentifier(MultiInetSocketAddress local) throws IOException {
    return new MultiInetSocketAddress(new InetSocketAddress(InetAddress.getLocalHost(), START_PORT-2));
  }
}
