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
package org.mpisws.p2p.transport.ssl;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreSpi;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;

/**
 * Does not encrypt UDP messages
 * The server authenticates to the client via a CACert
 * 
 * 
 * @author Jeff Hoye
 *
 * @param <Identifier>
 */
public class SSLTransportLayerImpl<Identifier, MessageType> implements SSLTransportLayer<Identifier, MessageType> {
  protected TransportLayer<Identifier, MessageType> tl;
  protected TransportLayerCallback<Identifier, MessageType> callback;
  protected ErrorHandler<Identifier> errorHandler;
  protected Logger logger;
  protected Environment environment;

  protected SSLContext context;
  
  KeyPair keyPair;
  private int clientAuth;
  
  public SSLTransportLayerImpl(TransportLayer<Identifier, MessageType> tl, KeyStore keyStore, KeyStore trustStore, Environment env) throws IOException {
    this(tl,keyStore,trustStore,CLIENT_AUTH_REQUIRED,env);
  }
  
  /**
   * 
   * @param tl
   * @param ks set a cert on the client, and optionally a keypair (if want clientauth), on the server, need a keypair, cert if want clientauth
   * @param clientAuth NO, OPTIONAL, REQUIRED // on the server side
   * @param env
   * @throws Exception
   */
  public SSLTransportLayerImpl(TransportLayer<Identifier, MessageType> tl, KeyStore keyStore, KeyStore trustStore, int clientAuth, Environment env) throws IOException {
    if (clientAuth != CLIENT_AUTH_NONE && clientAuth != CLIENT_AUTH_REQUIRED) throw new IllegalArgumentException("clientAuth type:"+clientAuth+" not supported.");
    
    this.environment = env;
    this.logger = env.getLogManager().getLogger(SSLTransportLayerImpl.class, null);
    this.tl = tl;
    errorHandler = new DefaultErrorHandler<Identifier>(logger, Logger.WARNING);
    this.clientAuth = clientAuth;

    try {
      this.context = SSLContext.getInstance("TLS");
    char[] passphrase = "".toCharArray();
    
    KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
    kmf.init(keyStore, passphrase);

    TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
    tmf.init(trustStore);    
    
    TrustManager[] tms = tmf.getTrustManagers();
    if (clientAuth == CLIENT_AUTH_NONE) tms = null;
    context.init(kmf.getKeyManagers(), tms, null);

    tl.setCallback(this);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception nsae) {
      throw new RuntimeException(nsae);
    }
    
  }
  
  public SocketRequestHandle<Identifier> openSocket(Identifier i,
      final SocketCallback<Identifier> deliverSocketToMe, Map<String, Object> options) {
    final SocketRequestHandleImpl<Identifier> ret = new SocketRequestHandleImpl<Identifier>(i,options,logger);
    ret.setSubCancellable(tl.openSocket(i, new SocketCallback<Identifier>() {

      public void receiveException(SocketRequestHandle<Identifier> s,
          Exception ex) {
        deliverSocketToMe.receiveException(s, ex);
      }

      public void receiveResult(SocketRequestHandle<Identifier> cancellable,
          P2PSocket<Identifier> sock) {
        getSocketManager(SSLTransportLayerImpl.this, sock, new Continuation<SSLSocketManager<Identifier>, Exception>() {

          public void receiveException(Exception exception) {
            deliverSocketToMe.receiveException(ret, exception);
          }

          public void receiveResult(SSLSocketManager<Identifier> result) {
            deliverSocketToMe.receiveResult(ret, result);
          }}, false, clientAuth != CLIENT_AUTH_NONE);
      }
    
    }, options));
    return ret;
  }

  /**
   * TODO: support resuming
   */
  public void incomingSocket(final P2PSocket<Identifier> s) throws IOException {
    getSocketManager(this,s,new Continuation<SSLSocketManager<Identifier>, Exception>() {

      public void receiveException(Exception exception) {
        errorHandler.receivedException(s.getIdentifier(), exception);
      }

      public void receiveResult(SSLSocketManager<Identifier> result) {
        try {
          callback.incomingSocket(result);
        } catch (IOException ioe) {
          result.close();
          errorHandler.receivedException(s.getIdentifier(), ioe);
        }
      }},true, clientAuth != CLIENT_AUTH_NONE);
  }

  public void setCallback(TransportLayerCallback<Identifier, MessageType> callback) {
    this.callback = callback;
  }

  public void acceptMessages(boolean b) {
    tl.acceptMessages(b);
  }

  public void acceptSockets(boolean b) {
    tl.acceptSockets(b);
  }

  public void destroy() {
    tl.destroy();
  }

  public Identifier getLocalIdentifier() {
    return tl.getLocalIdentifier();
  }

  public MessageRequestHandle<Identifier, MessageType> sendMessage(Identifier i,
      MessageType m, MessageCallback<Identifier, MessageType> deliverAckToMe,
      Map<String, Object> options) {
    return tl.sendMessage(i, m, deliverAckToMe, options);
  }

  public void setErrorHandler(ErrorHandler<Identifier> handler) {
    this.errorHandler = handler;
  }

  public void messageReceived(Identifier i, MessageType m,
      Map<String, Object> options) throws IOException {
    callback.messageReceived(i, m, options);
  }

  protected SSLSocketManager<Identifier> getSocketManager(SSLTransportLayerImpl<Identifier, ?> sslTL,
      P2PSocket<Identifier> s,
      Continuation<SSLSocketManager<Identifier>, Exception> c, boolean server, boolean useClientAuth) {
    return new SSLSocketManager<Identifier>(sslTL,s,c,server,useClientAuth);
  }
  
}
