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
package org.apache.flume.clients.log4jappender;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AvroSource;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.Status;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestLoadBalancingLog4jAppender {

  private final List<CountingAvroSource> sources = Lists.newArrayList();
  private Channel ch;
  private ChannelSelector rcs;
  private Logger fixture;
  private boolean slowDown = false;

  private static List<Integer> getFreePorts(int numberOfPorts) throws IOException {
    List<Integer> ports = new ArrayList<>(numberOfPorts);
    for (int index = 0; index < numberOfPorts; ++index) {
      try (ServerSocket socket = new ServerSocket(0)) {
        ports.add(socket.getLocalPort());
      }
    }
    return ports;
  }

  private static String toHostList(List<Integer> ports) {
    List<String> addresses = new ArrayList<String>(ports.size());
    for (Integer port : ports) {
      addresses.add("localhost:" + port);
    }
    String hostList = StringUtils.join(addresses, " ");
    return hostList;
  }

  @Before
  public void initiate() throws InterruptedException {
    ch = new MemoryChannel();
    configureChannel();

  }

  private void configureChannel() {
    Configurables.configure(ch, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(ch);

    rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);
  }

  @After
  public void cleanUp() {
    for (Source source : sources) {
      source.stop();
    }
  }

  @Test
  public void testLog4jAppenderRoundRobin() throws IOException {
    int numberOfMsgs = 1000;
    int expectedPerSource = 500;
    String propertiesFile = "flume-loadbalancinglog4jtest.properties";
    startSources(propertiesFile, false, getFreePorts(2));

    sendAndAssertMessages(numberOfMsgs);

    for (CountingAvroSource source : sources) {
      Assert.assertEquals(expectedPerSource, source.appendCount.get());
    }
  }

  @Test
  public void testLog4jAppenderRandom() throws IOException {
    int numberOfMsgs = 1000;
    String propertiesFile = "flume-loadbalancing-rnd-log4jtest.properties";
    startSources(propertiesFile, false, getFreePorts(10));

    sendAndAssertMessages(numberOfMsgs);

    int total = 0;
    Set<Integer> counts = new HashSet<Integer>();
    for (CountingAvroSource source : sources) {
      total += source.appendCount.intValue();
      counts.add(source.appendCount.intValue());
    }
    // We are not testing distribution this is tested in the client
    Assert.assertTrue("Very unusual distribution " + counts.size(), counts.size() > 2);
    Assert.assertTrue("Missing events", total == numberOfMsgs);
  }

  @Test
  public void testRandomBackoff() throws Exception {
    String propertiesFile = "flume-loadbalancing-backoff-log4jtest.properties";
    startSources(propertiesFile, false, getFreePorts(3));

    sources.get(0).setFail();
    sources.get(2).setFail();

    sendAndAssertMessages(50);

    Assert.assertEquals(50, sources.get(1).appendCount.intValue());
    Assert.assertEquals(0, sources.get(0).appendCount.intValue());
    Assert.assertEquals(0, sources.get(2).appendCount.intValue());
    sources.get(0).setOk();
    sources.get(1).setFail(); // s0 should still be backed off
    try {
      send(1);
      // nothing should be able to process right now
      Assert.fail("Expected EventDeliveryException");
    } catch (FlumeException e) {
      Assert.assertTrue(e.getCause() instanceof EventDeliveryException);
    }
    Thread.sleep(2500); // wait for s0 to no longer be backed off

    sendAndAssertMessages(50);

    Assert.assertEquals(50, sources.get(0).appendCount.intValue());
    Assert.assertEquals(50, sources.get(1).appendCount.intValue());
    Assert.assertEquals(0, sources.get(2).appendCount.intValue());
  }

  @Test
  public void testRandomBackoffUnsafeMode() throws Exception {
    String propertiesFile = "flume-loadbalancing-backoff-log4jtest.properties";
    startSources(propertiesFile, true, getFreePorts(3));

    sources.get(0).setFail();
    sources.get(1).setFail();
    sources.get(2).setFail();
    sendAndAssertFail();

  }

  @Test (expected = EventDeliveryException.class)
  public void testTimeout() throws Throwable {
    String propertiesFile = "flume-loadbalancinglog4jtest.properties";

    ch = new TestLog4jAppender.SlowMemoryChannel(2000);
    configureChannel();
    slowDown = true;
    startSources(propertiesFile, false, getFreePorts(3));
    int level = 20000;
    String msg = "This is log message number" + String.valueOf(level);
    try {
      fixture.log(Level.toLevel(level), msg);
    } catch (FlumeException ex) {
      throw ex.getCause();
    }
  }

  @Test(expected = EventDeliveryException.class)
  public void testRandomBackoffNotUnsafeMode() throws Throwable {
    String propertiesFile = "flume-loadbalancing-backoff-log4jtest.properties";
    startSources(propertiesFile, false, getFreePorts(3));

    sources.get(0).setFail();
    sources.get(1).setFail();
    sources.get(2).setFail();
    try {
      sendAndAssertFail();
    } catch (FlumeException ex) {
      throw ex.getCause();
    }
  }

  private void send(int numberOfMsgs) throws EventDeliveryException {
    for (int count = 0; count < numberOfMsgs; count++) {
      int level = count % 5;
      String msg = "This is log message number" + String.valueOf(count);
      fixture.log(Level.toLevel(level), msg);
    }
  }

  private void sendAndAssertFail() throws IOException {
    int level = 20000;
    String msg = "This is log message number" + String.valueOf(level);
    fixture.log(Level.toLevel(level), msg);

    Transaction transaction = ch.getTransaction();
    transaction.begin();
    Event event = ch.take();
    Assert.assertNull(event);

    transaction.commit();
    transaction.close();

  }

  private void sendAndAssertMessages(int numberOfMsgs) throws IOException {
    for (int count = 0; count < numberOfMsgs; count++) {
      int level = count % 5;
      String msg = "This is log message number" + String.valueOf(count);
      fixture.log(Level.toLevel(level), msg);

      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      Assert.assertEquals(new String(event.getBody(), "UTF8"), msg);

      Map<String, String> hdrs = event.getHeaders();

      Assert.assertNotNull(hdrs.get(Log4jAvroHeaders.TIMESTAMP.toString()));

      Assert.assertNotNull(hdrs.get(Log4jAvroHeaders.ADDRESS.toString()));

      Assert.assertEquals(Level.toLevel(level),
          Level.toLevel(hdrs.get(Log4jAvroHeaders.LOG_LEVEL.toString())));

      Assert.assertEquals(fixture.getName(),
          hdrs.get(Log4jAvroHeaders.LOGGER_NAME.toString()));

      Assert.assertEquals("UTF8",
          hdrs.get(Log4jAvroHeaders.MESSAGE_ENCODING.toString()));
      // To confirm on console we actually got the body
      System.out.println("Got body: " + new String(event.getBody(), "UTF8"));
      transaction.commit();
      transaction.close();
    }

  }

  private void startSources(String log4jProps, boolean unsafeMode, List<Integer> ports)
      throws IOException {
    for (int port : ports) {
      CountingAvroSource source = new CountingAvroSource(port);
      Context context = new Context();
      context.put("port", String.valueOf(port));
      context.put("bind", "0.0.0.0");
      Configurables.configure(source, context);
      sources.add(source);
      source.setChannelProcessor(new ChannelProcessor(rcs));
    }

    for (Source source : sources) {
      source.start();
    }

    // The properties file having Avro port info should be loaded only
    // after the test begins, else log4j tries to connect to the source
    // before the source has started up in the above function, since
    // log4j setup is completed before the @Before calls also.
    // This will cause the test to fail even before it starts!

    Reader reader = new InputStreamReader(getClass().getResourceAsStream("/" + log4jProps));
    Properties props = new Properties();
    props.load(reader);
    props.setProperty("log4j.appender.out2.Hosts", toHostList(ports));
    props.setProperty("log4j.appender.out2.UnsafeMode",
        String.valueOf(unsafeMode));
    if (slowDown) {
      props.setProperty("log4j.appender.out2.Timeout", String.valueOf(1000));
    }
    PropertyConfigurator.configure(props);
    fixture = LogManager.getLogger(TestLoadBalancingLog4jAppender.class);
  }

  static class CountingAvroSource extends AvroSource {
    AtomicInteger appendCount = new AtomicInteger();
    volatile boolean isFail = false;
    private final int port2;

    public CountingAvroSource(int port) {
      port2 = port;
    }

    public void setOk() {
      this.isFail = false;
    }

    public void setFail() {
      this.isFail = true;
    }

    @Override
    public String getName() {
      return "testing..." + port2;
    }

    @Override
    public Status append(AvroFlumeEvent avroEvent) {
      if (isFail) {
        return Status.FAILED;
      }
      appendCount.incrementAndGet();
      return super.append(avroEvent);
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) {
      if (isFail) {
        return Status.FAILED;
      }
      appendCount.addAndGet(events.size());
      return super.appendBatch(events);
    }
  }
}
