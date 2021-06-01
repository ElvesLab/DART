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
package org.apache.flume.source;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.DatagramSocket;
import com.google.common.base.Charsets;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class TestNetcatUdpSource {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TestNetcatUdpSource.class);
  private NetcatUdpSource source;
  private Channel channel;
  private static final int TEST_NETCAT_PORT = 0;
  private final String shortString = "Lorem ipsum dolor sit amet.";
  private final String mediumString = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
                                      "Nunc maximus rhoncus viverra. Nunc a metus.";

  private void init() {
    source = new NetcatUdpSource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
    Context context = new Context();
    context.put("port", String.valueOf(TEST_NETCAT_PORT));

    source.configure(context);

  }

  /** Tests the keepFields configuration parameter (enabled or disabled)
   using SyslogUDPSource.*/

  private void runUdpTest(String data1) throws IOException {
    init();
    source.start();
    // Write some message to the port
    DatagramSocket socket;
    DatagramPacket datagramPacket;
    datagramPacket = new DatagramPacket(data1.getBytes(),
      data1.getBytes().length,
      InetAddress.getLocalHost(), source.getSourcePort());
    for (int i = 0; i < 10 ; i++) {
      socket = new DatagramSocket();
      socket.send(datagramPacket);
      socket.close();
    }

    List<Event> channelEvents = new ArrayList<Event>();
    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 10; i++) {
      Event e = channel.take();
      Assert.assertNotNull(e);
      channelEvents.add(e);
    }

    try {
      txn.commit();
    } catch (Throwable t) {
      txn.rollback();
    } finally {
      txn.close();
    }

    source.stop();
    for (Event e : channelEvents) {
      Assert.assertNotNull(e);
      String str = new String(e.getBody(), Charsets.UTF_8);
      logger.info(str);
      Assert.assertArrayEquals(data1.getBytes(),
            e.getBody());

    }
  }

  @Test
  public void testLargePayload() throws Exception {
    init();
    source.start();
    // Write some message to the netcat port

    byte[] largePayload = getPayload(1000).getBytes();

    DatagramSocket socket;
    DatagramPacket datagramPacket;
    datagramPacket = new DatagramPacket(largePayload,
            1000,
            InetAddress.getLocalHost(), source.getSourcePort());
    for (int i = 0; i < 10 ; i++) {
      socket = new DatagramSocket();
      socket.send(datagramPacket);
      socket.close();
    }

    List<Event> channelEvents = new ArrayList<Event>();
    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 10; i++) {
      Event e = channel.take();
      Assert.assertNotNull(e);
      channelEvents.add(e);
    }

    try {
      txn.commit();
    } catch (Throwable t) {
      txn.rollback();
    } finally {
      txn.close();
    }

    source.stop();
    for (Event e : channelEvents) {
      Assert.assertNotNull(e);
      Assert.assertArrayEquals(largePayload, e.getBody());
    }
  }

  @Test
  public void testShortString() throws IOException {
    runUdpTest(shortString);
  }

  @Test
  public void testMediumString() throws IOException {
    runUdpTest(mediumString);
  }

  private String getPayload(int length) {
    StringBuilder payload = new StringBuilder(length);
    for (int n = 0; n < length; ++n) {
      payload.append("x");
    }
    return payload.toString();
  }
}

