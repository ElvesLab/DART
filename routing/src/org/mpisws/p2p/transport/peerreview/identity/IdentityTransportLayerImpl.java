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
package org.mpisws.p2p.transport.peerreview.identity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.mpisws.p2p.pki.x509.X509Serializer;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.P2PSocketReceiver;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.table.TableStore;
import org.mpisws.p2p.transport.table.TableTransprotLayerImpl;
import org.mpisws.p2p.transport.util.BufferReader;
import org.mpisws.p2p.transport.util.BufferWriter;
import org.mpisws.p2p.transport.util.DefaultErrorHandler;
import org.mpisws.p2p.transport.util.Serializer;
import org.mpisws.p2p.transport.util.SocketInputBuffer;
import org.mpisws.p2p.transport.util.SocketRequestHandleImpl;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.p2p.util.tuples.Tuple3;

/**
 * TODO: make it store known certs to a file, make it periodically check the revocation server.
 * 
 * Extends the TableTLi, but uses Certs rather than general objects.
 * Also, sends huge messages by opening temp tcp-socket.
 * 
 * @author Jeff Hoye
 *
 */
public class IdentityTransportLayerImpl<Identifier, I> extends 
     TableTransprotLayerImpl<Identifier, I, X509Certificate> 
     implements IdentityTransport<Identifier, I> {
    
  public static final String DEFAULT_SIGNATURE_ALGORITHM = "SHA1withRSA";
  public static final short DEFAULT_SIGNATURE_SIZE = 96;
  String signatureAlgorithm = DEFAULT_SIGNATURE_ALGORITHM;
  String signatureImpl = "BC";
  
  Signature signer;
  
  // TODO: handle memory problems
  Map<I, Signature> verifiers = new HashMap<I, Signature>();
  
  HashProvider hasher;
  private Environment environment;
  
  /**
   * TODO: Use a param to load the store from a file.
   * @param localCert
   * @param params
   * @return
   */
  static <H, I> TableStore<I, X509Certificate> getTableStore(I localId, X509Certificate localCert, Serializer<I> iSerializer, X509Serializer cSerializer, InputBuffer buf) {
    MyStore<H, I> ret = new MyStore<H, I>();
    ret.put(localId, localCert);
    if (buf != null) {
      // load store from the file
      throw new RuntimeException("Persistent version not implemented.");
    }
    
    return ret;
  }
  
  static class MyStore<H, I> extends HashMap<I, X509Certificate> implements TableStore<I, X509Certificate> {
    IdentityTransportCallback<H, I> callback;
    @Override
    public X509Certificate put(I key, X509Certificate value) {
      X509Certificate ret = super.put(key, value);     
      if (ret == null && callback != null) {
        callback.notifyCertificateAvailable(key);
      }
      return ret;
    }}
  
  public IdentityTransportLayerImpl(
      Serializer<I> iSerializer, X509Serializer cSerializer, I localId, 
      X509Certificate localCert, PrivateKey localPrivate, TransportLayer<Identifier, ByteBuffer> tl, 
      HashProvider hasher, Environment env) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchProviderException {
    super(iSerializer, cSerializer, getTableStore(localId, localCert, iSerializer, cSerializer, null), tl, env);
    this.tl = tl;
    tl.setCallback(this);
    this.hasher = hasher;
    this.environment = env;
    this.logger = env.getLogManager().getLogger(IdentityTransportLayerImpl.class, null);
    this.errorHandler = new DefaultErrorHandler<Identifier>(this.logger);
    
    signer = Signature.getInstance(DEFAULT_SIGNATURE_ALGORITHM,"BC");
    signer.initSign(localPrivate);    
  }
  
  @Override
  public void setCallback(
      TransportLayerCallback<Identifier, ByteBuffer> callback) {
    ((MyStore<Identifier, I>)knownValues).callback = (IdentityTransportCallback<Identifier, I>)callback;
    super.setCallback(callback);
  }

  /**
   * CERT_REQUEST, int requestId, Identifier
   */
  public Cancellable requestCertificate(final Identifier source,
      final I principal, final Continuation<X509Certificate, Exception> c,
      Map<String, Object> options) {
    return super.requestValue(source, principal, c, options);
  }
  
  public boolean hasCertificate(I i) {
    return super.hasKey(i);
  }
  
  public byte[] sign(byte[] bytes) {
    try {
      signer.update(bytes);
      byte[] ret = signer.sign();
      if (logger.level <= Logger.FINEST) logger.log("Signature of "+MathUtils.toBase64(bytes)+" was "+MathUtils.toBase64(ret));
      return ret;
    } catch (SignatureException se) {
      RuntimeException throwMe = new RuntimeException("Couldn't sign "+bytes);
      throwMe.initCause(se);
      throw throwMe;
    }
  }

  public int verify(I id, byte[] msg, byte[] signature) {
    if (logger.level <= Logger.FINEST) logger.log("Verify:"+id+" "+msg.length+" "+MathUtils.toBase64(msg)+" == "+signature.length+" "+MathUtils.toBase64(signature));
    Signature verifier = getVerifier(id);
    if (verifier == null) return NO_CERTIFICATE; //throw new UnknownCertificateException(getLocalIdentifier(),id);
//    msg.array()[0] = 55;
//    System.out.println("Verifiying of "+MathUtils.toBase64(msg.array())+" was "+MathUtils.toBase64(signature.array()));
//    System.out.println("Verifiying of "+msg+" was "+signature);

    try {
      synchronized(verifier) {      
        verifier.update(msg);
        if (verifier.verify(signature)) {
          return SIGNATURE_OK;
  //        throw new SignatureException("Signature by "+id+" failed.");
        }
      }
    } catch (SignatureException se) {
      throw new RuntimeException(se);
    }
//    System.out.println("Signature success by "+id);   
    return SIGNATURE_BAD;
  }
  
  /**
   * Returns null if we don't know the cert for the identifier.
   * 
   * @param i
   * @return
   * @throws NoSuchAlgorithmException
   * @throws NoSuchProviderException
   * @throws InvalidKeyException
   */
  public Signature getVerifier(I i) {
    Signature ret = verifiers.get(i);
    try {
      if (ret == null) {
        if (knownValues.containsKey(i)) {
          X509Certificate cert = knownValues.get(i);
          ret = Signature.getInstance(DEFAULT_SIGNATURE_ALGORITHM, "BC");
          ret.initVerify(cert);
          verifiers.put(i, ret);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return ret;
  }

  public short getSignatureSizeBytes() {
    return DEFAULT_SIGNATURE_SIZE;
  }

  public byte[] getEmptyHash() {
    return hasher.getEmptyHash();
  }

  public short getHashSizeBytes() {
    return hasher.getHashSizeBytes();
  }

  public byte[] hash(long seq, short type, byte[] nodeHash, byte[] contentHash) {
    return hasher.hash(seq, type, nodeHash, contentHash);
  }

  public byte[] hash(ByteBuffer... hashMe) {
    return hasher.hash(hashMe);
  }

  public Environment getEnvironment() {
    return environment;
  }  
}

