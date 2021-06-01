/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.sink;

import com.google.common.base.Charsets;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.api.RpcClient;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.AvroSource;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class TestAvroSink {

  private static final Logger logger = LoggerFactory
      .getLogger(TestAvroSink.class);
  private static final String hostname = "127.0.0.1";
  private static final Integer port;

  static {
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    } catch (IOException e) {
      throw new AssertionError("Cannot find free port", e);
    }
  }

  private AvroSink sink;
  private Channel channel;


  public void setUp() {
    setUp("none", 0);
  }

  public void setUp(String compressionType, int compressionLevel) {
    if (sink != null) {
      throw new RuntimeException("double setup");
    }
    sink = new AvroSink();
    channel = new MemoryChannel();

    Context context = createBaseContext();

    if (compressionType.equals("deflate")) {
      context.put("compression-type", compressionType);
      context.put("compression-level", Integer.toString(compressionLevel));
    }

    sink.setChannel(channel);

    Configurables.configure(sink, context);
    Configurables.configure(channel, context);
  }

  private Context createBaseContext() {
    Context context = new Context();

    context.put("hostname", hostname);
    context.put("port", String.valueOf(port));
    context.put("batch-size", String.valueOf(2));
    context.put("connect-timeout", String.valueOf(2000L));
    context.put("request-timeout", String.valueOf(3000L));

    return context;
  }

  private Server createServer(AvroSourceProtocol protocol)
      throws IllegalAccessException, InstantiationException {

    Server server = new NettyServer(new SpecificResponder(
        AvroSourceProtocol.class, protocol), new InetSocketAddress(
        hostname, port));

    return server;
  }

  @Test
  public void testLifecycle() throws InterruptedException,
      InstantiationException, IllegalAccessException {
    setUp();
    Server server = createServer(new MockAvroServer());

    server.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));

    server.close();
  }

  @Test
  public void testProcess() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {
    setUp();

    Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);
    Server server = createServer(new MockAvroServer());

    server.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    Transaction transaction = channel.getTransaction();

    transaction.begin();
    for (int i = 0; i < 10; i++) {
      channel.put(event);
    }
    transaction.commit();
    transaction.close();

    for (int i = 0; i < 5; i++) {
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
    }

    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));

    server.close();
  }

  @Test
  public void testChannelException() throws InterruptedException,
          EventDeliveryException, InstantiationException, IllegalAccessException {
    setUp();

    Server server = createServer(new MockAvroServer());
    server.start();
    sink.start();
    Channel channel = Mockito.mock(Channel.class);
    Mockito.when(channel.take()).thenThrow(new ChannelException("dummy"));
    Transaction transaction = Mockito.mock(BasicTransactionSemantics.class);
    Mockito.when(channel.getTransaction()).thenReturn(transaction);
    sink.setChannel(channel);

    Sink.Status status = sink.process();

    sink.stop();
    server.close();

    SinkCounter sinkCounter = (SinkCounter) Whitebox.getInternalState(sink, "sinkCounter");
    Assert.assertEquals(1, sinkCounter.getChannelReadFail());
  }

  @Test
  public void testTimeout() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {
    setUp();
    Event event = EventBuilder.withBody("foo", Charsets.UTF_8);
    AtomicLong delay = new AtomicLong();
    Server server = createServer(new DelayMockAvroServer(delay));
    server.start();
    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 4; i++) {
      channel.put(event);
    }
    txn.commit();
    txn.close();

    // should throw EventDeliveryException due to connect timeout
    delay.set(3000L); // because connect-timeout = 2000
    boolean threw = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      logger.info("Correctly threw due to connect timeout. Exception follows.",
          ex);
      threw = true;
    }

    Assert.assertTrue("Must throw due to connect timeout", threw);

    // now, allow the connect handshake to occur
    delay.set(0);
    sink.process();

    // should throw another EventDeliveryException due to request timeout
    delay.set(4000L); // because request-timeout = 3000
    threw = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      logger.info("Correctly threw due to request timeout. Exception follows.",
          ex);
      threw = true;
    }

    Assert.assertTrue("Must throw due to request timeout", threw);

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));
    server.close();

    SinkCounter sinkCounter = (SinkCounter) Whitebox.getInternalState(sink, "sinkCounter");
    Assert.assertEquals(2, sinkCounter.getEventWriteFail());
  }

  @Test
  public void testFailedConnect() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {

    setUp();
    Event event = EventBuilder.withBody("test event 1",
        Charset.forName("UTF8"));
    Server server = createServer(new MockAvroServer());

    server.start();
    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    Thread.sleep(500L); // let socket startup
    server.close();
    Thread.sleep(500L); // sleep a little to allow close occur

    Transaction transaction = channel.getTransaction();

    transaction.begin();
    for (int i = 0; i < 10; i++) {
      channel.put(event);
    }
    transaction.commit();
    transaction.close();

    for (int i = 0; i < 5; i++) {
      boolean threwException = false;
      try {
        sink.process();
      } catch (EventDeliveryException e) {
        threwException = true;
      }
      Assert.assertTrue("Must throw EventDeliveryException if disconnected",
          threwException);
    }

    server = createServer(new MockAvroServer());
    server.start();

    for (int i = 0; i < 5; i++) {
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
    }

    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));
    server.close();

    SinkCounter sinkCounter = (SinkCounter) Whitebox.getInternalState(sink, "sinkCounter");
    Assert.assertEquals(5, sinkCounter.getEventWriteFail());
    Assert.assertEquals(4, sinkCounter.getConnectionFailedCount());
  }

  @Test
  public void testReset() throws Exception {

    setUp();
    Server server = createServer(new MockAvroServer());

    server.start();

    Context context = new Context();

    context.put("hostname", hostname);
    context.put("port", String.valueOf(port));
    context.put("batch-size", String.valueOf(2));
    context.put("connect-timeout", String.valueOf(2000L));
    context.put("request-timeout", String.valueOf(3000L));
    context.put("reset-connection-interval", String.valueOf("5"));

    sink.setChannel(channel);
    Configurables.configure(sink, context);
    sink.start();
    RpcClient firstClient = sink.getUnderlyingClient();
    Thread.sleep(6000);
    Transaction t = channel.getTransaction();
    t.begin();
    channel.put(EventBuilder.withBody("This is a test", Charset.defaultCharset()));
    t.commit();
    t.close();
    sink.process();
    // Make sure they are not the same object, connection should be reset
    Assert.assertFalse(firstClient == sink.getUnderlyingClient());
    sink.stop();

    context.put("hostname", hostname);
    context.put("port", String.valueOf(port));
    context.put("batch-size", String.valueOf(2));
    context.put("connect-timeout", String.valueOf(2000L));
    context.put("request-timeout", String.valueOf(3000L));
    context.put("reset-connection-interval", String.valueOf("0"));

    sink.setChannel(channel);
    Configurables.configure(sink, context);
    sink.start();
    firstClient = sink.getUnderlyingClient();
    Thread.sleep(6000);
    // Make sure they are the same object, since connection should not be reset
    Assert.assertTrue(firstClient == sink.getUnderlyingClient());
    sink.stop();

    context.clear();
    context.put("hostname", hostname);
    context.put("port", String.valueOf(port));
    context.put("batch-size", String.valueOf(2));
    context.put("connect-timeout", String.valueOf(2000L));
    context.put("request-timeout", String.valueOf(3000L));

    sink.setChannel(channel);
    Configurables.configure(sink, context);
    sink.start();
    firstClient = sink.getUnderlyingClient();
    Thread.sleep(6000);
    // Make sure they are the same object, since connection should not be reset
    Assert.assertTrue(firstClient == sink.getUnderlyingClient());
    sink.stop();
    server.close();
  }

  @Test
  public void testSslProcessTrustAllCerts() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {
    setUp();

    Context context = createBaseContext();
    context.put("ssl", String.valueOf(true));
    context.put("trust-all-certs", String.valueOf(true));

    Configurables.configure(sink, context);

    doTestSslProcess();
  }

  @Test
  public void testSslProcessWithComponentTruststore() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {
    setUp();

    Context context = createBaseContext();
    context.put("ssl", String.valueOf(true));
    context.put("truststore", "src/test/resources/truststore.jks");
    context.put("truststore-password", "password");

    Configurables.configure(sink, context);

    doTestSslProcess();
  }

  @Test
  public void testSslProcessWithComponentTruststoreNoPassword() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {
    setUp();

    Context context = createBaseContext();
    context.put("ssl", String.valueOf(true));
    context.put("truststore", "src/test/resources/truststore.jks");

    Configurables.configure(sink, context);

    doTestSslProcess();
  }

  @Test
  public void testSslProcessWithGlobalTruststore() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {
    setUp();

    System.setProperty("javax.net.ssl.trustStore", "src/test/resources/truststore.jks");
    System.setProperty("javax.net.ssl.trustStorePassword", "password");

    Context context = createBaseContext();
    context.put("ssl", String.valueOf(true));

    Configurables.configure(sink, context);

    doTestSslProcess();

    System.clearProperty("javax.net.ssl.trustStore");
    System.clearProperty("javax.net.ssl.trustStorePassword");
  }

  @Test
  public void testSslProcessWithGlobalTruststoreNoPassword() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {
    setUp();

    System.setProperty("javax.net.ssl.trustStore", "src/test/resources/truststore.jks");

    Context context = createBaseContext();
    context.put("ssl", String.valueOf(true));

    Configurables.configure(sink, context);

    doTestSslProcess();

    System.clearProperty("javax.net.ssl.trustStore");
  }

  private void doTestSslProcess() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {
    Server server = createSslServer(new MockAvroServer());
    server.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);
    for (int i = 0; i < 10; i++) {
      channel.put(event);
    }

    transaction.commit();
    transaction.close();

    for (int i = 0; i < 5; i++) {
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
    }

    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));

    server.close();
  }

  @Test
  public void testSslWithCompression() throws InterruptedException,
      EventDeliveryException, InstantiationException, IllegalAccessException {
    setUp("deflate", 6);

    boolean bound = false;

    AvroSource source;
    Channel sourceChannel;
    int selectedPort;

    source = new AvroSource();
    sourceChannel = new MemoryChannel();

    Configurables.configure(sourceChannel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(sourceChannel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));

    Context context = new Context();
    context.put("port", port.toString());
    context.put("bind", hostname);
    context.put("threads", "50");
    context.put("compression-type", "deflate");
    context.put("ssl", String.valueOf(true));
    context.put("keystore", "src/test/resources/server.p12");
    context.put("keystore-password", "password");
    context.put("keystore-type", "PKCS12");

    Configurables.configure(source, context);

    source.start();

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());


    Event event = EventBuilder.withBody("Hello avro",
        Charset.forName("UTF8"));

    context = createBaseContext();
    context.put("ssl", String.valueOf(true));
    context.put("trust-all-certs", String.valueOf(true));
    context.put("compression-type", "deflate");
    context.put("compression-level", Integer.toString(6));

    Configurables.configure(sink, context);
    sink.start();

    Transaction sickTransaction = channel.getTransaction();

    sickTransaction.begin();
    for (int i = 0; i < 10; i++) {
      channel.put(event);
    }
    sickTransaction.commit();
    sickTransaction.close();

    for (int i = 0; i < 5; i++) {
      Sink.Status status = sink.process();
      logger.debug("Calling Process " + i + " times:" + status);
      Assert.assertEquals(Sink.Status.READY, status);
    }

    sink.stop();

    Transaction sourceTransaction = sourceChannel.getTransaction();
    sourceTransaction.begin();

    Event sourceEvent = sourceChannel.take();
    Assert.assertNotNull(sourceEvent);
    Assert.assertEquals("Channel contained our event", "Hello avro",
        new String(sourceEvent.getBody()));
    sourceTransaction.commit();
    sourceTransaction.close();

    logger.debug("Round trip event:{}", sourceEvent);

    source.stop();
    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }

  @Test
  public void testSslSinkWithNonSslServer() throws InterruptedException,
      InstantiationException, IllegalAccessException {
    setUp();

    Server server = createServer(new MockAvroServer());
    server.start();

    Context context = createBaseContext();
    context.put("ssl", String.valueOf(true));
    context.put("trust-all-certs", String.valueOf(true));

    Configurables.configure(sink, context);

    boolean failed = doRequestWhenFailureExpected();

    server.close();

    if (!failed) {
      Assert.fail("SSL-enabled sink successfully connected to a non-SSL-enabled server, " +
          "that's wrong.");
    }

    SinkCounter sinkCounter = (SinkCounter) Whitebox.getInternalState(sink, "sinkCounter");
    Assert.assertEquals(1, sinkCounter.getEventWriteFail());
  }

  @Test
  public void testSslSinkWithNonTrustedCert() throws InterruptedException,
      InstantiationException, IllegalAccessException {
    setUp();

    Server server = createSslServer(new MockAvroServer());
    server.start();

    Context context = createBaseContext();
    context.put("ssl", String.valueOf(true));

    Configurables.configure(sink, context);

    boolean failed = doRequestWhenFailureExpected();

    server.close();

    if (!failed) {
      Assert.fail("SSL-enabled sink successfully connected to a server with an " +
          "untrusted certificate when it should have failed");
    }
    SinkCounter sinkCounter = (SinkCounter) Whitebox.getInternalState(sink, "sinkCounter");
    Assert.assertEquals(1, sinkCounter.getEventWriteFail());
  }

  private boolean doRequestWhenFailureExpected()
      throws InterruptedException {
    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.START_OR_ERROR, 5000));

    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);
    channel.put(event);

    transaction.commit();
    transaction.close();

    boolean failed;
    try {
      sink.process();
      failed = false;
    } catch (EventDeliveryException ex) {
      logger.info("Correctly failed to send event", ex);
      failed = true;
    }

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink,
        LifecycleState.STOP_OR_ERROR, 5000));

    return failed;
  }

  @Test
  public void testRequestWithNoCompression()
      throws InterruptedException, IOException, EventDeliveryException {
    doRequest(false, false, 6);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel0()
      throws InterruptedException, IOException, EventDeliveryException {
    doRequest(true, true, 0);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel1()
      throws InterruptedException, IOException, EventDeliveryException {
    doRequest(true, true, 1);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel6()
      throws InterruptedException, IOException, EventDeliveryException {
    doRequest(true, true, 6);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel9()
      throws InterruptedException, IOException, EventDeliveryException {
    doRequest(true, true, 9);
  }

  private void doRequest(boolean serverEnableCompression, boolean clientEnableCompression,
                         int compressionLevel)
      throws InterruptedException, IOException, EventDeliveryException {
    if (clientEnableCompression) {
      setUp("deflate", compressionLevel);
    } else {
      setUp("none", compressionLevel);
    }

    boolean bound = false;

    AvroSource source;
    Channel sourceChannel;
    int selectedPort;

    source = new AvroSource();
    sourceChannel = new MemoryChannel();

    Configurables.configure(sourceChannel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(sourceChannel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));

    Context context = new Context();
    context.put("port", port.toString());
    context.put("bind", hostname);
    context.put("threads", "50");
    if (serverEnableCompression) {
      context.put("compression-type", "deflate");
    } else {
      context.put("compression-type", "none");
    }

    Configurables.configure(source, context);

    source.start();

    Assert.assertTrue("Reached start or error",
                      LifecycleController.waitForOneOf(source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started",
                        LifecycleState.START, source.getLifecycleState());

    Event event = EventBuilder.withBody("Hello avro", Charset.forName("UTF8"));

    sink.start();

    Transaction sickTransaction = channel.getTransaction();

    sickTransaction.begin();
    for (int i = 0; i < 10; i++) {
      channel.put(event);
    }
    sickTransaction.commit();
    sickTransaction.close();

    for (int i = 0; i < 5; i++) {
      Sink.Status status = sink.process();
      logger.debug("Calling Process " + i + " times:" + status);
      Assert.assertEquals(Sink.Status.READY, status);
    }

    sink.stop();


    Transaction sourceTransaction = sourceChannel.getTransaction();
    sourceTransaction.begin();

    Event sourceEvent = sourceChannel.take();
    Assert.assertNotNull(sourceEvent);
    Assert.assertEquals("Channel contained our event", "Hello avro",
        new String(sourceEvent.getBody()));
    sourceTransaction.commit();
    sourceTransaction.close();

    logger.debug("Round trip event:{}", sourceEvent);

    source.stop();
    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }

  private static class MockAvroServer implements AvroSourceProtocol {

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.debug("Received event:{}", event);
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events)
        throws AvroRemoteException {
      logger.debug("Received event batch:{}", events);
      return Status.OK;
    }

  }

  private static class DelayMockAvroServer implements AvroSourceProtocol {

    private final AtomicLong delay;

    public DelayMockAvroServer(AtomicLong delay) {
      this.delay = delay;
    }

    private void sleep() throws AvroRemoteException {
      try {
        Thread.sleep(delay.get());
      } catch (InterruptedException e) {
        throw new AvroRemoteException("Interrupted while sleeping", e);
      }
    }

    @Override
    public Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.debug("Received event:{}; delaying for {}ms", event, delay);
      sleep();
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events)
        throws AvroRemoteException {
      logger.debug("Received event batch:{}; delaying for {}ms", events, delay);
      sleep();
      return Status.OK;
    }

  }

  private Server createSslServer(AvroSourceProtocol protocol)
      throws IllegalAccessException, InstantiationException {
    Server server = new NettyServer(new SpecificResponder(
        AvroSourceProtocol.class, protocol), new InetSocketAddress(hostname, port),
            new NioServerSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool()),
            new SSLChannelPipelineFactory(),
            null);

    return server;
  }

  /**
   * Factory of SSL-enabled server worker channel pipelines
   * Copied from Avro's org.apache.avro.ipc.TestNettyServerWithSSL test
   */
  private class SSLChannelPipelineFactory
      implements ChannelPipelineFactory {

    String keystore = "src/test/resources/server.p12";
    String keystorePassword = "password";
    String keystoreType = "PKCS12";

    public SSLChannelPipelineFactory() {
    }

    public SSLChannelPipelineFactory(String keystore, String keystorePassword,
                                     String keystoreType) {
      this.keystore = keystore;
      this.keystorePassword = keystorePassword;
      this.keystoreType = keystoreType;
    }

    private SSLContext createServerSSLContext() {
      try {
        KeyStore ks = KeyStore.getInstance(keystoreType);
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray());

        // Set up key manager factory to use our key store
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(getAlgorithm());
        kmf.init(ks, keystorePassword.toCharArray());

        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(kmf.getKeyManagers(), null, null);
        return serverContext;
      } catch (Exception e) {
        throw new Error("Failed to initialize the server-side SSLContext", e);
      }
    }

    private String getAlgorithm() {
      String algorithm = Security.getProperty(
          "ssl.KeyManagerFactory.algorithm");
      if (algorithm == null) {
        algorithm = "SunX509";
      }
      return algorithm;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      SSLEngine sslEngine = createServerSSLContext().createSSLEngine();
      sslEngine.setUseClientMode(false);
      pipeline.addLast("ssl", new SslHandler(sslEngine));
      return pipeline;
    }
  }
}
