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
package org.mpisws.p2p.testing.transportlayer.peerreview;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.security.spec.RSAKeyGenParameterSpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.mpisws.p2p.pki.x509.CATool;
import org.mpisws.p2p.pki.x509.CAToolImpl;
import org.mpisws.p2p.pki.x509.X509Serializer;
import org.mpisws.p2p.pki.x509.X509SerializerImpl;
import org.mpisws.p2p.transport.ErrorHandler;
import org.mpisws.p2p.transport.MessageCallback;
import org.mpisws.p2p.transport.MessageRequestHandle;
import org.mpisws.p2p.transport.P2PSocket;
import org.mpisws.p2p.transport.SocketCallback;
import org.mpisws.p2p.transport.SocketRequestHandle;
import org.mpisws.p2p.transport.TransportLayer;
import org.mpisws.p2p.transport.TransportLayerCallback;
import org.mpisws.p2p.transport.peerreview.IdentifierExtractor;
import org.mpisws.p2p.transport.peerreview.PeerReview;
import org.mpisws.p2p.transport.peerreview.PeerReviewCallback;
import org.mpisws.p2p.transport.peerreview.PeerReviewImpl;
import org.mpisws.p2p.transport.peerreview.WitnessListener;
import org.mpisws.p2p.transport.peerreview.commitment.Authenticator;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializer;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorSerializerImpl;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStore;
import org.mpisws.p2p.transport.peerreview.commitment.CommitmentProtocol;
import org.mpisws.p2p.transport.peerreview.commitment.CommitmentProtocolImpl;
import org.mpisws.p2p.transport.peerreview.evidence.EvidenceSerializerImpl;
import org.mpisws.p2p.transport.peerreview.history.HashProvider;
import org.mpisws.p2p.transport.peerreview.history.SecureHistory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactoryImpl;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA1HashProvider;
import org.mpisws.p2p.transport.peerreview.history.stub.NullHashProvider;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransport;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransportCallback;
import org.mpisws.p2p.transport.peerreview.identity.IdentityTransportLayerImpl;
import org.mpisws.p2p.transport.peerreview.identity.UnknownCertificateException;
import org.mpisws.p2p.transport.peerreview.infostore.Evidence;
import org.mpisws.p2p.transport.peerreview.infostore.IdStrTranslator;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStore;
import org.mpisws.p2p.transport.peerreview.infostore.StatusChangeListener;
import org.mpisws.p2p.transport.peerreview.message.PeerReviewMessage;
import org.mpisws.p2p.transport.peerreview.replay.Verifier;
import org.mpisws.p2p.transport.peerreview.replay.record.RecordLayer;
import org.mpisws.p2p.transport.table.UnknownValueException;
import org.mpisws.p2p.transport.util.MessageRequestHandleImpl;
import org.mpisws.p2p.transport.util.Serializer;

import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.p2p.commonapi.Cancellable;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawSerializable;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.selector.TimerTask;

public class PRRegressionTest {
  public static final byte[] EMPTY_ARRAY = new byte[0];
  static class IdExtractor implements IdentifierExtractor<HandleImpl, IdImpl> {

    public IdImpl extractIdentifier(HandleImpl h) {
      return h.id;
    }
    
  }
  
  static class HandleSerializer implements Serializer<HandleImpl> {

    public HandleImpl deserialize(InputBuffer buf) throws IOException {
      return HandleImpl.build(buf);
    }

    public void serialize(HandleImpl i, OutputBuffer buf) throws IOException {
      i.serialize(buf);
    }
    
  }
  
  static class IdSerializer implements Serializer<IdImpl> {

    public IdImpl deserialize(InputBuffer buf) throws IOException {
      return IdImpl.build(buf);
    }

    public void serialize(IdImpl i, OutputBuffer buf) throws IOException {
      i.serialize(buf);
    }
    
  }
  
  static class HandleImpl implements RawSerializable {
    String name;
    IdImpl id;
    
    public HandleImpl(String s, IdImpl id) {
      this.name = s;
      this.id = id;
    }
    
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeUTF(name);
      id.serialize(buf);
    }
    
    public static HandleImpl build(InputBuffer buf) throws IOException {
      return new HandleImpl(buf.readUTF(), IdImpl.build(buf));
    }
    
    public String toString() {
      return "HandleImpl<"+name+">";
    }
    
    public int hashCode() {
      return id.hashCode()^name.hashCode();
    }
    
    public boolean equals(Object o) {
      HandleImpl that = (HandleImpl)o;
      if (!id.equals(that.id)) return false;
      return name.equals(that.name);
    }
  }
  
  static class IdImpl implements RawSerializable {
    int id;
    public IdImpl(int id) {
      this.id = id;
    }
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeInt(id);
    }    
    
    public static IdImpl build(InputBuffer buf) throws IOException {
      return new IdImpl(buf.readInt());
    }
    
    public String toString() {
      return "Id<"+id+">";
    }
    
    public int hashCode() {
      return id;      
    }
    
    public boolean equals(Object o) {
      IdImpl that = (IdImpl)o;
      return (this.id == that.id);
    }
  }
  
  
  static class BogusTransport implements TransportLayer<HandleImpl, ByteBuffer> {
    public static Map<HandleImpl, BogusTransport> peerTable = new HashMap<HandleImpl, BogusTransport>();
    
    HandleImpl localIdentifier;
    Environment env;
    
    public BogusTransport(HandleImpl handle, X509Certificate cert, Environment env) {
      peerTable.put(handle, this);
      this.localIdentifier = handle;
      this.env = env;
    }
    
    public void acceptMessages(boolean b) {
      throw new RuntimeException("implement");
    }

    public void acceptSockets(boolean b) {
      throw new RuntimeException("implement");
    }

    public HandleImpl getLocalIdentifier() {
      return localIdentifier;
    }

    public SocketRequestHandle<HandleImpl> openSocket(HandleImpl i,
        SocketCallback<HandleImpl> deliverSocketToMe,
        Map<String, Object> options) {
      throw new RuntimeException("implement");
    }

    public MessageRequestHandle<HandleImpl, ByteBuffer> sendMessage(
        HandleImpl i, ByteBuffer m,
        MessageCallback<HandleImpl, ByteBuffer> deliverAckToMe,
        Map<String, Object> options) {      
      peerTable.get(i).receiveMessage(localIdentifier, m);      
//      if (deliverAckToMe != null) deliverAckToMe.ack(new Message)
      return null;
    }

    TransportLayerCallback<HandleImpl, ByteBuffer> callback;
    
    private void receiveMessage(final HandleImpl i, final ByteBuffer m) {
//      System.out.println(Thread.currentThread()+" invoking onto "+env.getSelectorManager());
      env.getSelectorManager().invoke(new Runnable() {      
        public void run() {
          try {                
            callback.messageReceived(i, m, null);
          } catch (IOException ioe) {
            ioe.printStackTrace();
            System.exit(0);
          }
        }      
      });
//      System.out.println(Thread.currentThread()+" invoked onto "+env.getSelectorManager());
    }

    public void setCallback(
        TransportLayerCallback<HandleImpl, ByteBuffer> callback) {
      this.callback = callback;
    }

    public void setErrorHandler(ErrorHandler<HandleImpl> handler) {
      throw new RuntimeException("implement");
    }

    public void destroy() {
      throw new RuntimeException("implement");
    }
  }
  
  class BogusApp implements PeerReviewCallback<HandleImpl, IdImpl> {

    Logger logger;
    Player player;
    Environment env;
    Random rand;
    long nextSendTime = 0;
    private HandleImpl dest;
    
    private TransportLayer<HandleImpl, ByteBuffer> tl;
        
    public BogusApp(Player player, TransportLayer<HandleImpl, ByteBuffer> tl, Environment env) {
      super();
      this.player = player;
      this.tl = tl;
      this.env = env;
      this.logger = env.getLogManager().getLogger(BogusApp.class, null);
      logger.log("new bogus app "+player);
      dest = player.destHandle;
      tl.setCallback(this);      
    }
    

    public void init() {
//      logger.log("init()");
      rand = new Random();
      if (player.localHandle.id.id == 1) {
        // need to give the other nodes a second to boot, otherwise this message will go nowhere
        scheduleMessageToBeSent(env.getTimeSource().currentTimeMillis()+1000, true);
      }
    }

    public void scheduleMessageToBeSent() {
      scheduleMessageToBeSent(env.getTimeSource().currentTimeMillis()+rand.nextInt(1999)+1, true);
    }
    
    public void scheduleMessageToBeSent(long time, final boolean reschedule) {
      nextSendTime = time;
      if (logger.level <= Logger.FINE) logger.log("scheduling message to be sent at:"+time);
      env.getSelectorManager().schedule(new TimerTask() {
        public String toString() {
          return "SendMessageTask "+scheduledExecutionTime();
        }
        
        @Override
        public void run() {
          sendMessage();
          if (reschedule) scheduleMessageToBeSent();
        }      
      }, nextSendTime-env.getTimeSource().currentTimeMillis());      
    }
    
    public void sendMessage() {
      byte[] msg = generateMessage();
      
      logger.log("sending message "+msg.length+" "+MathUtils.toBase64(msg));
      if (logger.level <= Logger.INFO) logger.log("sending message "+msg.length+" "+MathUtils.toBase64(msg));
//      HandleImpl dest = bob.localHandle;
      try {
      tl.sendMessage(dest, ByteBuffer.wrap(msg), new MessageCallback<HandleImpl, ByteBuffer>() {
        
        public void sendFailed(MessageRequestHandle<HandleImpl, ByteBuffer> msg,
            Exception reason) {
          logger.log("sendFailed("+msg+")");
//          System.out.println("sendFailed("+msg+")");
//          reason.printStackTrace();
        }
      
        public void ack(MessageRequestHandle<HandleImpl, ByteBuffer> msg) {
          alice.logger.log("ack("+msg+") "+Thread.currentThread());
          if (logger.level <= Logger.FINE) alice.logger.log("ack("+msg+")");
        }
      
      }, null);      
      } catch (NullPointerException npe) {
        logger.log("tl:"+tl+" "+dest);
        throw npe;
      }
    }

    protected byte[] generateMessage() {
      byte[] msg = new byte[rand.nextInt(31)+1];
      rand.nextBytes(msg);
      return msg;
    }
    
    public void storeCheckpoint(OutputBuffer buffer) throws IOException {
      if (logger.level <= Logger.FINER) logger.log("storeCheckpoint "+nextSendTime);
      buffer.writeInt(31173);
      buffer.writeLong(nextSendTime);
      buffer.writeBoolean(dest != null);
      if (dest != null) dest.serialize(buffer);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      new ObjectOutputStream(baos).writeObject(rand);
      byte[] bytes = baos.toByteArray();
      buffer.writeInt(bytes.length);      
      buffer.write(bytes, 0, bytes.length);
      if (logger.level <= Logger.FINEST) logger.log("storeCheckpoint:"+Arrays.toString(((SimpleOutputBuffer)buffer).getBytes()));
    }

    public boolean loadCheckpoint(InputBuffer buffer) throws IOException {
      if (buffer.readInt() != 31173) throw new RuntimeException("invalid checkpoint");
      nextSendTime = buffer.readLong();
      if (buffer.readBoolean()) {
        dest = new HandleSerializer().deserialize(buffer);
      }
      if (logger.level <= Logger.FINER) logger.log("loadCheckpoint "+nextSendTime);
      byte[] bytes = new byte[buffer.readInt()];
      buffer.read(bytes);
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      try {
        rand = (Random)new ObjectInputStream(bais).readObject();
      } catch (ClassNotFoundException cnfe) {
        IOException ioe = new IOException("Error reading random number from checkpoint");
        ioe.initCause(cnfe);
        throw ioe;
      }
      if (nextSendTime > 0) {
        scheduleMessageToBeSent(nextSendTime, true);
      }
      return true;
    }
    
    public void destroy() {
      throw new RuntimeException("implement");
    }

    public void notifyCertificateAvailable(IdImpl id) {
      throw new RuntimeException("implement");
    }

    public void receive(HandleImpl source, boolean datagram, ByteBuffer msg) {
      throw new RuntimeException("implement");
    }

    public void sendComplete(long id) {
      throw new RuntimeException("implement");
    }

    public void incomingSocket(P2PSocket<HandleImpl> s) throws IOException {
      throw new RuntimeException("implement");
    }

    public void messageReceived(HandleImpl i, ByteBuffer m,
        Map<String, Object> options) throws IOException {
      if (logger.level <= Logger.INFO) logger.log("Message received: "+MathUtils.toBase64(m.array()));
    }

    public void getWitnesses(IdImpl subject,
        WitnessListener<HandleImpl, IdImpl> callback) {
      callback.notifyWitnessSet(subject, Collections.singletonList(carol.localHandle));
    }

    public void notifyStatusChange(
        IdImpl id,
        int newStatus) {
      if (logger.level <= Logger.INFO) logger.log("notifyStatusChange("+id+","+PeerReviewImpl.getStatusString(newStatus)+")");
      if (newStatus != STATUS_TRUSTED) {
        logger.log("Failure, Node not trusted: "+id+" at "+player.localHandle);
        System.exit(1);
      }
      addStatusNotification(this.player.localHandle,id,newStatus);
    }

    public Collection<HandleImpl> getMyWitnessedNodes() {
      return player.witnessed;
    }

    public PeerReviewCallback<HandleImpl, IdImpl> getReplayInstance(Verifier<HandleImpl> v) {
      BogusApp ret = new BogusApp(playerTable.get(v.getLocalIdentifier()),v,v.getEnvironment());
      return ret;
    }
  }

  static Map<HandleImpl, IdentityTransportLayerImpl<HandleImpl, IdImpl>> idTLTable = new HashMap<HandleImpl, IdentityTransportLayerImpl<HandleImpl,IdImpl>>();

  CATool caTool;
  KeyPairGenerator keyPairGen;

  Map<HandleImpl,Player> playerTable = new HashMap<HandleImpl, Player>();

  protected PeerReviewImpl<HandleImpl, IdImpl> getPeerReview(Player player, MyIdTL transport, Environment env) {
    return new PeerReviewImpl<HandleImpl, IdImpl>(transport, env, new HandleSerializer(), new IdSerializer(), new IdExtractor(), getIdStrTranslator()
//        ,new AuthenticatorSerializerImpl(20,96), new EvidenceSerializerImpl<HandleImpl, IdImpl>(new HandleSerializer(),new IdSerializer(),transport.getHashSizeBytes(),transport.getSignatureSizeBytes())
        );
  }
  
  
  class Player {    
    Logger logger;
    HandleImpl localHandle;
    
    PeerReview<HandleImpl, IdImpl> pr;
    MyIdTL transport;
    public Collection<HandleImpl> witnessed = new ArrayList<HandleImpl>();
    KeyPair pair;    
    X509Certificate cert;
    BogusTransport t1;
    Environment env;

//    int id;
    
    BogusApp app;
    HandleImpl destHandle;
    
    public Player(HandleImpl localHandle, HandleImpl dstHandle, final Environment env2) throws Exception {
      super();
      this.destHandle = dstHandle;
//      this.id = id;
      env = cloneEnvironment(env2, localHandle.name, localHandle.id.id);
      
      this.logger = env.getLogManager().getLogger(Player.class, null);
      
      File f = new File(localHandle.name);
      if (f.exists()) {
        File f2 = new File(f,"peers");
        File[] foo = f2.listFiles();
        if (foo != null) {
          for (int c = 0; c < foo.length; c++) {
            foo[c].delete();
          }
        }
        
        foo = f.listFiles();
        if (foo != null) {
          for (int c = 0; c < foo.length; c++) {
            foo[c].delete();
          }
        }
        
//        System.out.println("Delete "+f+","+f.delete());        
      }
//      File f = new File(name+".data");
//      if (f.exists()) f.delete();
//      f = new File(name+".index");
//      if (f.exists()) f.delete();
      
      this.localHandle = localHandle;
      playerTable.put(localHandle, this);
      
      pair = keyPairGen.generateKeyPair();    
      cert = caTool.sign(localHandle.name,pair.getPublic());

      t1 = getTL();
            
      transport = getIdTransport();
        
      idTLTable.put(localHandle, transport);
      pr = getPeerReview(this, transport, env);
      app = getApp();
      pr.setApp(app);
      env.getSelectorManager().invoke(new Runnable() {
        public void run() {
          try {
            pr.init(Player.this.localHandle.name);
          } catch (IOException ioe) {
            ioe.printStackTrace();
            env2.destroy();
          }
        }
      });
    }
    
    public BogusApp getApp() {
      return getBogusApp(this, pr, env);
//      return new BogusApp(this,pr,env);
    }
        
    // to be overridden
    public Environment cloneEnvironment(Environment env2, String name, int id) {
      return env2.cloneEnvironment(name);    
    }


    public void buildPlayerCryptoStuff() {
      
    }

    public BogusTransport getTL() throws Exception {
      return new BogusTransport(localHandle, cert, env); 
    }
    
    public MyIdTL getIdTransport() throws Exception {
      return new MyIdTL(
          new IdSerializer(), new X509SerializerImpl(),this.localHandle.id,
          cert,pair.getPrivate(),t1,new SHA1HashProvider(),env) {      
      };
    }
    
    
  }

  public IdStrTranslator<IdImpl> getIdStrTranslator() {
    return new IdStrTranslator<IdImpl>(){

      public IdImpl readIdentifierFromString(String s) {
        return new IdImpl(Integer.parseInt(s));
      }

      public String toString(IdImpl id) {
        return Integer.toString(id.id);
      }};
  }

  class MyIdTL extends IdentityTransportLayerImpl<HandleImpl, IdImpl> {
    public MyIdTL(Serializer<IdImpl> serializer, X509Serializer serializer2,
        IdImpl localId, X509Certificate localCert, PrivateKey localPrivate,
        TransportLayer<HandleImpl, ByteBuffer> tl, HashProvider hasher,
        Environment env) throws InvalidKeyException, NoSuchAlgorithmException,
        NoSuchProviderException {
      super(serializer, serializer2, localId, localCert, localPrivate, tl, hasher,
          env);
    }

    @Override
    public Cancellable requestCertificate(final HandleImpl source, final IdImpl certHolder,
        final Continuation<X509Certificate, Exception> c, Map<String, Object> options) {
      //logger.log("requestCert("+certHolder+" from "+source+")");
      return idTLTable.get(source).requestValue(source, certHolder, new Continuation<X509Certificate, Exception>() {
      
        public void receiveResult(X509Certificate result) {
          //logger.log("delivering cert for ("+certHolder+") c:"+c);
          knownValues.put(certHolder, result);
          if (c != null) c.receiveResult(result);
        }
      
        public void receiveException(Exception exception) {
          //logger.logException("exception when requesting cert for ("+certHolder+") c:"+c,exception);
          if (c != null) c.receiveException(exception);
        }
      
      }, options);
    }            
  }
  
  private HashProvider hasher;
  private SecureHistoryFactory historyFactory;
  
  Player alice;
  Player bob;
  Player carol;
  
  HandleImpl aliceHandle = new HandleImpl("alice", new IdImpl(1));
  HandleImpl bobHandle = new HandleImpl("bob", new IdImpl(2));
  HandleImpl carolHandle = new HandleImpl("carol", new IdImpl(3));


  public void setLoggingParams(Environment env) {
    env.getParameters().setInt("org.mpisws.p2p.testing.transportlayer.peerreview_loglevel", Logger.INFO);
//    env.getParameters().setInt("org.mpisws.p2p.transport.peerreview.audit_loglevel", Logger.FINEST);    
  }

  protected BogusApp getBogusApp(Player player, PeerReview<HandleImpl, IdImpl> pr,
      Environment env) {
    return new BogusApp(player,pr,env);
  }

  public void buildCryptoMaterial(Environment env) throws Exception {
    SecureRandom random = new SecureRandom();
    caTool = CAToolImpl.getCATool("CommitmentTest","foo".toCharArray());
    
    // make a KeyPair
    keyPairGen =
      KeyPairGenerator.getInstance("RSA", "BC");
    keyPairGen.initialize(
        new RSAKeyGenParameterSpec(768,
            RSAKeyGenParameterSpec.F4),
        random);    
    hasher = new SHA1HashProvider();
    historyFactory = new SecureHistoryFactoryImpl(hasher,env);
  }

  public void buildPlayers(Environment env) throws Exception {    
    alice = new Player(aliceHandle, bobHandle, env);
    bob = new Player(bobHandle, aliceHandle, env);    
    carol = new Player(carolHandle, null, env);  
//    System.out.println("here");
  }

  public void setupWitnesses() {
    carol.witnessed.add(alice.localHandle);
    carol.witnessed.add(bob.localHandle);    
  }
  
  protected Map<HandleImpl, Map<IdImpl, Integer>> recordedStatus = new HashMap<HandleImpl, Map<IdImpl,Integer>>();
  protected void addStatusNotification(HandleImpl localHandle, IdImpl id,
      int newStatus) {
    Map<IdImpl, Integer> foo = recordedStatus.get(localHandle);
    if (foo == null) {
      foo = new HashMap<IdImpl, Integer>();
      recordedStatus.put(localHandle,foo);
    }
    foo.put(id,newStatus);
  }

  public void finish() {
    for (Entry<HandleImpl, Map<IdImpl, Integer>> foo : recordedStatus.entrySet()) {
      for (Entry<IdImpl, Integer> i : foo.getValue().entrySet()) {
        if (i.getValue() != StatusChangeListener.STATUS_TRUSTED) {
          logger.log("Fail: "+foo.getKey()+" found "+i.getKey()+" "+i.getValue());
          System.exit(1);
        }
      }
    }
    logger.log("Success");
    System.exit(1);
  }
  
  Environment env;
  Logger logger;
  
  /**
   * @param millisToFinish Call finish() after this amount of time
   * @throws Exception
   */
  public PRRegressionTest(int millisToFinish) throws Exception {

    
    env = RecordLayer.generateEnvironment(); //new Environment();
    env.getSelectorManager().schedule(new TimerTask() {
      
      @Override
      public void run() {
        finish();
      }
    },millisToFinish);

    logger = env.getLogManager().getLogger(PRRegressionTest.class, null);
    setLoggingParams(env);
    
    buildCryptoMaterial(env);
        
    env.getSelectorManager().invoke(new Runnable() {
      
      public void run() {
        try {
          buildPlayers(env);
          
          setupWitnesses();
        } catch (Exception ioe) {
          env.destroy();      
          throw new RuntimeException(ioe);
        }
      }
    });          
  }
  
  public static void main(String[] agrs) throws Exception {
    new PRRegressionTest(45000);
  }
}
