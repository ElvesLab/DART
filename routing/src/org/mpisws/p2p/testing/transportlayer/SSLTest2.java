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

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;

public class SSLTest2 {
  public static void main(String[] argz) throws Exception {
    /*
 // Create/initialize the SSLContext with key material

    char[] passphrase = "passphrase".toCharArray();

    // First initialize the key and trust material.
    KeyStore ksKeys = KeyStore.getInstance("JKS");
    ksKeys.load(new FileInputStream("testKeys"), passphrase);
    KeyStore ksTrust = KeyStore.getInstance("JKS");
    ksTrust.load(new FileInputStream("testTrust"), passphrase);

    // KeyManager's decide which key material to use.
    KeyManagerFactory kmf =
        KeyManagerFactory.getInstance("SunX509");
    kmf.init(ksKeys, passphrase);

    // TrustManager's decide whether to allow connections.
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance("SunX509");
    tmf.init(ksTrust);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
        kmf.getKeyManagers(), tmf.getTrustManagers(), null);

    // We're ready for the engine.
    SSLEngine engine = sslContext.createSSLengine(hostname, port);

    // Use as client
    engine.setUseClientMode(true);

    
 // Create a non-blocking socket channel
    SocketChannel socketChannel = SocketChannel.open();
    socketChannel.configureBlocking(false);
    socketChannel.connect(new InetSocketAddress(hostname, port));

    // Complete connection
    while (!socketChannel.finishedConnect()) {
        // do something until connect completed
    }

    // Create byte buffers to use for holding application and encoded data
    SSLSession session = engine.getSession();
    ByteBuffer myAppData = ByteBuffer.allocate(session.getApplicationBufferSize());
    ByteBuffer myNetData = ByteBuffer.allocate(session.getPacketBufferSize());
    ByteBuffer peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize());
    ByteBuffer peerNetData = ByteBuffer.allocate(session.getPacketBufferSize());

    // Do initial handshake
    doHandshake(socketChannel, engine, myNetData, peerNetData);

    myAppData.put("hello".getBytes());
    myAppData.flip();

    while (myAppData.hasRemaining()) {
        // Generate SSL/TLS encoded data (handshake or application data)
        SSLEngineResult res = engine.wrap(myAppData, myNetData);

        // Process status of call
        if (res.getStatus() == SSLEngineResult.Status.OK) {
            myAppData.compact();

            // Send SSL/TLS encoded data to peer
            while(myNetData.hasRemaining()) {
                int num = socketChannel.write(myNetData);
          if (num == -1) {
              // handle closed channel
          } else if (num == 0) {
              // no bytes written; try again later
          }
            }
        }

        // Handle other status:  BUFFER_OVERFLOW, CLOSED
        //...
    }

*/
  }
}
