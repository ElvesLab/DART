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

import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AvroSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLog4jAppenderWithAvro {
  private AvroSource source;
  private Channel ch;
  private Properties props;
  private int port;

  private static int getFreePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @Before
  public void setUp() throws Exception {
    URL schemaUrl = getClass().getClassLoader().getResource("myrecord.avsc");
    Files.copy(Resources.newInputStreamSupplier(schemaUrl),
        new File("/tmp/myrecord.avsc"));

    port = getFreePort();
    source = new AvroSource();
    ch = new MemoryChannel();
    Configurables.configure(ch, new Context());

    Context context = new Context();
    context.put("port", String.valueOf(port));
    context.put("bind", "localhost");
    Configurables.configure(source, context);

    List<Channel> channels = new ArrayList<>();
    channels.add(ch);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));

    source.start();
  }

  private void loadProperties(String file) throws IOException {
    File TESTFILE = new File(TestLog4jAppenderWithAvro.class.getClassLoader()
            .getResource(file).getFile());
    FileReader reader = new FileReader(TESTFILE);
    props = new Properties();
    props.load(reader);
    reader.close();
  }

  @Test
  public void testAvroGeneric() throws IOException {
    loadProperties("flume-log4jtest-avro-generic.properties");
    props.put("log4j.appender.out2.Port", String.valueOf(port));
    PropertyConfigurator.configure(props);
    Logger logger = LogManager.getLogger(TestLog4jAppenderWithAvro.class);
    String msg = "This is log message number " + String.valueOf(0);

    Schema schema = new Schema.Parser().parse(
        getClass().getClassLoader().getResource("myrecord.avsc").openStream());
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    GenericRecord record = builder.set("message", msg).build();

    logger.info(record);

    Transaction transaction = ch.getTransaction();
    transaction.begin();
    Event event = ch.take();
    Assert.assertNotNull(event);

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(event.getBody(), null);
    GenericRecord recordFromEvent = reader.read(null, decoder);
    Assert.assertEquals(msg, recordFromEvent.get("message").toString());

    Map<String, String> hdrs = event.getHeaders();

    Assert.assertNull(hdrs.get(Log4jAvroHeaders.MESSAGE_ENCODING.toString()));

    Assert.assertEquals("Schema URL should be set",
        "file:///tmp/myrecord.avsc", hdrs.get(Log4jAvroHeaders.AVRO_SCHEMA_URL.toString()));
    Assert.assertNull("Schema string should not be set",
        hdrs.get(Log4jAvroHeaders.AVRO_SCHEMA_LITERAL.toString()));

    transaction.commit();
    transaction.close();
  }

  @Test
  public void testAvroReflect() throws IOException {
    loadProperties("flume-log4jtest-avro-reflect.properties");
    props.put("log4j.appender.out2.Port", String.valueOf(port));
    PropertyConfigurator.configure(props);
    Logger logger = LogManager.getLogger(TestLog4jAppenderWithAvro.class);
    String msg = "This is log message number " + String.valueOf(0);

    AppEvent appEvent = new AppEvent();
    appEvent.setMessage(msg);

    logger.info(appEvent);

    Transaction transaction = ch.getTransaction();
    transaction.begin();
    Event event = ch.take();
    Assert.assertNotNull(event);

    Schema schema = ReflectData.get().getSchema(appEvent.getClass());

    ReflectDatumReader<AppEvent> reader = new ReflectDatumReader<>(AppEvent.class);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(event.getBody(), null);
    AppEvent recordFromEvent = reader.read(null, decoder);
    Assert.assertEquals(msg, recordFromEvent.getMessage());

    Map<String, String> hdrs = event.getHeaders();

    Assert.assertNull(hdrs.get(Log4jAvroHeaders.MESSAGE_ENCODING.toString()));

    Assert.assertNull("Schema URL should not be set",
        hdrs.get(Log4jAvroHeaders.AVRO_SCHEMA_URL.toString()));
    Assert.assertEquals("Schema string should be set", schema.toString(),
        hdrs.get(Log4jAvroHeaders.AVRO_SCHEMA_LITERAL.toString()));

    transaction.commit();
    transaction.close();
  }

  @Test
  public void testDifferentEventTypesInBatchWithAvroReflect() throws IOException {
    loadProperties("flume-log4jtest-avro-reflect.properties");
    props.put("log4j.appender.out2.Port", String.valueOf(port));
    PropertyConfigurator.configure(props);
    Logger logger = LogManager.getLogger(getClass());
    List<Object> events = Arrays.asList("string", new AppEvent("appEvent"));
    logger.info(events);

    Transaction transaction = ch.getTransaction();
    transaction.begin();

    for (Object o : events) {
      Event e = ch.take();
      Assert.assertNotNull(e);
      ReflectDatumReader<?> reader = new ReflectDatumReader<>(o.getClass());
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(e.getBody(), null);
      Object readObject = reader.read(null, decoder);
      Assert.assertEquals(o, readObject);

      Map<String, String> hdrs = e.getHeaders();
      Assert.assertNull(hdrs.get(Log4jAvroHeaders.MESSAGE_ENCODING.toString()));
      Assert.assertNull("Schema URL should not be set",
          hdrs.get(Log4jAvroHeaders.AVRO_SCHEMA_URL.toString()));
      Assert.assertEquals("Schema string should be set",
          ReflectData.get().getSchema(readObject.getClass()).toString(),
          hdrs.get(Log4jAvroHeaders.AVRO_SCHEMA_LITERAL.toString()));
    }
    Assert.assertNull("There should be no more events in the channel", ch.take());
  }

  @Test
  public void testDifferentEventTypesInBatchWithAvroGeneric() throws IOException {
    loadProperties("flume-log4jtest-avro-generic.properties");
    props.put("log4j.appender.out2.Port", String.valueOf(port));
    PropertyConfigurator.configure(props);
    Logger logger = LogManager.getLogger(getClass());
    String msg = "Avro log message";

    Schema schema = new Schema.Parser().parse(
        getClass().getClassLoader().getResource("myrecord.avsc").openStream());
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    GenericRecord record = builder.set("message", msg).build();

    List<Object> events = Arrays.asList("string", record);
    logger.info(events);

    Transaction transaction = ch.getTransaction();
    transaction.begin();

    Event event = ch.take();
    Assert.assertNotNull(event);
    Assert.assertEquals("string", new String(event.getBody()));

    event = ch.take();
    Assert.assertNotNull(event);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(event.getBody(), null);
    GenericRecord recordFromEvent = reader.read(null, decoder);
    Assert.assertEquals(msg, recordFromEvent.get("message").toString());

    Map<String, String> hdrs = event.getHeaders();
    Assert.assertNull(hdrs.get(Log4jAvroHeaders.MESSAGE_ENCODING.toString()));

    Assert.assertEquals("Schema URL should be set",
        "file:///tmp/myrecord.avsc", hdrs.get(Log4jAvroHeaders.AVRO_SCHEMA_URL.toString()));
    Assert.assertNull("Schema string should not be set",
        hdrs.get(Log4jAvroHeaders.AVRO_SCHEMA_LITERAL.toString()));

    transaction.commit();
    transaction.close();
  }

  @After
  public void cleanUp() {
    source.stop();
    ch.stop();
    props.clear();
  }

  public static class AppEvent {
    private String message;

    public AppEvent() {
    }

    public AppEvent(String message) {
      this.message = message;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AppEvent appEvent = (AppEvent) o;
      return message != null ? message.equals(appEvent.message) : appEvent.message == null;
    }

    @Override
    public int hashCode() {
      return message != null ? message.hashCode() : 0;
    }
  }

}
