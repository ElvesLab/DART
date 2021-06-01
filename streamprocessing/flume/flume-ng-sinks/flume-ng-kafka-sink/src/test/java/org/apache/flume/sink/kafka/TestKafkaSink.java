/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.flume.sink.kafka;

import com.google.common.base.Charsets;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.shared.kafka.test.KafkaPartitionTestUtil;
import org.apache.flume.shared.kafka.test.PartitionOption;
import org.apache.flume.shared.kafka.test.PartitionTestScenario;
import org.apache.flume.sink.kafka.util.TestUtil;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flume.sink.kafka.KafkaSinkConstants.AVRO_EVENT;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.BATCH_SIZE;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.BROKER_LIST_FLUME_KEY;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_KEY_SERIALIZER;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_TOPIC;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.KAFKA_PREFIX;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.KAFKA_PRODUCER_PREFIX;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.OLD_BATCH_SIZE;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.REQUIRED_ACKS_FLUME_KEY;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.TOPIC_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for Kafka Sink
 */
public class TestKafkaSink {

  private static final TestUtil testUtil = TestUtil.getInstance();
  private final Set<String> usedTopics = new HashSet<String>();

  @BeforeClass
  public static void setup() {
    testUtil.prepare();
    List<String> topics = new ArrayList<String>(3);
    topics.add(DEFAULT_TOPIC);
    topics.add(TestConstants.STATIC_TOPIC);
    topics.add(TestConstants.CUSTOM_TOPIC);
    topics.add(TestConstants.HEADER_1_VALUE + "-topic");
    testUtil.initTopicList(topics);
  }

  @AfterClass
  public static void tearDown() {
    testUtil.tearDown();
  }

  @Test
  public void testKafkaProperties() {

    KafkaSink kafkaSink = new KafkaSink();
    Context context = new Context();
    context.put(KAFKA_PREFIX + TOPIC_CONFIG, "");
    context.put(KAFKA_PRODUCER_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "override.default.serializer");
    context.put("kafka.producer.fake.property", "kafka.property.value");
    context.put("kafka.bootstrap.servers", "localhost:9092,localhost:9092");
    context.put("brokerList", "real-broker-list");
    Configurables.configure(kafkaSink, context);

    Properties kafkaProps = kafkaSink.getKafkaProps();

    //check that we have defaults set
    assertEquals(kafkaProps.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG),
                 DEFAULT_KEY_SERIALIZER);
    //check that kafka properties override the default and get correct name
    assertEquals(kafkaProps.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG),
                 "override.default.serializer");
    //check that any kafka-producer property gets in
    assertEquals(kafkaProps.getProperty("fake.property"),
                 "kafka.property.value");
    //check that documented property overrides defaults
    assertEquals(kafkaProps.getProperty("bootstrap.servers"),
                 "localhost:9092,localhost:9092");
  }

  @Test
  public void testOldProperties() {
    KafkaSink kafkaSink = new KafkaSink();
    Context context = new Context();
    context.put("topic", "test-topic");
    context.put(OLD_BATCH_SIZE, "300");
    context.put(BROKER_LIST_FLUME_KEY, "localhost:9092,localhost:9092");
    context.put(REQUIRED_ACKS_FLUME_KEY, "all");
    Configurables.configure(kafkaSink, context);

    Properties kafkaProps = kafkaSink.getKafkaProps();

    assertEquals(kafkaSink.getTopic(), "test-topic");
    assertEquals(kafkaSink.getBatchSize(), 300);
    assertEquals(kafkaProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
                 "localhost:9092,localhost:9092");
    assertEquals(kafkaProps.getProperty(ProducerConfig.ACKS_CONFIG), "all");

  }

  @Test
  public void testDefaultTopic() {
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    String msg = "default-topic-test";
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody(msg.getBytes());
    memoryChannel.put(event);
    tx.commit();
    tx.close();

    try {
      Sink.Status status = kafkaSink.process();
      if (status == Sink.Status.BACKOFF) {
        fail("Error Occurred");
      }
    } catch (EventDeliveryException ex) {
      // ignore
    }

    checkMessageArrived(msg, DEFAULT_TOPIC);
  }

  private void checkMessageArrived(String msg, String topic) {
    ConsumerRecords recs = pollConsumerRecords(topic);
    assertNotNull(recs);
    assertTrue(recs.count() > 0);
    ConsumerRecord consumerRecord = (ConsumerRecord) recs.iterator().next();
    assertEquals(msg, consumerRecord.value());
  }

  @Test
  public void testStaticTopic() {
    Context context = prepareDefaultContext();
    // add the static topic
    context.put(TOPIC_CONFIG, TestConstants.STATIC_TOPIC);
    String msg = "static-topic-test";

    try {
      Sink.Status status = prepareAndSend(context, msg);
      if (status == Sink.Status.BACKOFF) {
        fail("Error Occurred");
      }
    } catch (EventDeliveryException ex) {
      // ignore
    }

    checkMessageArrived(msg, TestConstants.STATIC_TOPIC);
  }

  @Test
  public void testTopicAndKeyFromHeader() {
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    String msg = "test-topic-and-key-from-header";
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("topic", TestConstants.CUSTOM_TOPIC);
    headers.put("key", TestConstants.CUSTOM_KEY);
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody(msg.getBytes(), headers);
    memoryChannel.put(event);
    tx.commit();
    tx.close();

    try {
      Sink.Status status = kafkaSink.process();
      if (status == Sink.Status.BACKOFF) {
        fail("Error Occurred");
      }
    } catch (EventDeliveryException ex) {
      // ignore
    }

    checkMessageArrived(msg, TestConstants.CUSTOM_TOPIC);
  }

  /**
   * Tests that a message will be produced to a topic as specified by a
   * custom topicHeader parameter (FLUME-3046).
   */
  @Test
  public void testTopicFromConfHeader() {
    String customTopicHeader = "customTopicHeader";
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    context.put(KafkaSinkConstants.TOPIC_OVERRIDE_HEADER, customTopicHeader);
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    String msg = "test-topic-from-config-header";
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(customTopicHeader, TestConstants.CUSTOM_TOPIC);
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody(msg.getBytes(), headers);
    memoryChannel.put(event);
    tx.commit();
    tx.close();

    try {
      Sink.Status status = kafkaSink.process();
      if (status == Sink.Status.BACKOFF) {
        fail("Error Occurred");
      }
    } catch (EventDeliveryException ex) {
      // ignore
    }

    checkMessageArrived(msg, TestConstants.CUSTOM_TOPIC);
  }

  /**
   * Tests that the topicHeader parameter will be ignored if the allowTopicHeader
   * parameter is set to false (FLUME-3046).
   */
  @Test
  public void testTopicNotFromConfHeader() {
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    context.put(KafkaSinkConstants.ALLOW_TOPIC_OVERRIDE_HEADER, "false");
    context.put(KafkaSinkConstants.TOPIC_OVERRIDE_HEADER, "foo");

    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    String msg = "test-topic-from-config-header";
    Map<String, String> headers = new HashMap<String, String>();
    headers.put(KafkaSinkConstants.DEFAULT_TOPIC_OVERRIDE_HEADER, TestConstants.CUSTOM_TOPIC);
    headers.put("foo", TestConstants.CUSTOM_TOPIC);
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody(msg.getBytes(), headers);
    memoryChannel.put(event);
    tx.commit();
    tx.close();

    try {
      Sink.Status status = kafkaSink.process();
      if (status == Sink.Status.BACKOFF) {
        fail("Error Occurred");
      }
    } catch (EventDeliveryException ex) {
      // ignore
    }

    checkMessageArrived(msg, DEFAULT_TOPIC);
  }

  @Test
  public void testReplaceSubStringOfTopicWithHeaders() {
    String topic = TestConstants.HEADER_1_VALUE + "-topic";
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    context.put(TOPIC_CONFIG, TestConstants.HEADER_TOPIC);
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    String msg = "test-replace-substring-of-topic-with-headers";
    Map<String, String> headers = new HashMap<>();
    headers.put(TestConstants.HEADER_1_KEY, TestConstants.HEADER_1_VALUE);
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody(msg.getBytes(), headers);
    memoryChannel.put(event);
    tx.commit();
    tx.close();

    try {
      Sink.Status status = kafkaSink.process();
      if (status == Sink.Status.BACKOFF) {
        fail("Error Occurred");
      }
    } catch (EventDeliveryException ex) {
      // ignore
    }

    checkMessageArrived(msg, topic);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testAvroEvent() throws IOException {
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    context.put(AVRO_EVENT, "true");
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    String msg = "test-avro-event";

    Map<String, String> headers = new HashMap<String, String>();
    headers.put("topic", TestConstants.CUSTOM_TOPIC);
    headers.put("key", TestConstants.CUSTOM_KEY);
    headers.put(TestConstants.HEADER_1_KEY, TestConstants.HEADER_1_VALUE);
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody(msg.getBytes(), headers);
    memoryChannel.put(event);
    tx.commit();
    tx.close();

    try {
      Sink.Status status = kafkaSink.process();
      if (status == Sink.Status.BACKOFF) {
        fail("Error Occurred");
      }
    } catch (EventDeliveryException ex) {
      // ignore
    }

    String topic = TestConstants.CUSTOM_TOPIC;

    ConsumerRecords<String, String> recs = pollConsumerRecords(topic);
    assertNotNull(recs);
    assertTrue(recs.count() > 0);
    ConsumerRecord<String, String> consumerRecord = recs.iterator().next();
    ByteArrayInputStream in = new ByteArrayInputStream(consumerRecord.value().getBytes());
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
    SpecificDatumReader<AvroFlumeEvent> reader = new SpecificDatumReader<>(AvroFlumeEvent.class);

    AvroFlumeEvent avroevent = reader.read(null, decoder);

    String eventBody = new String(avroevent.getBody().array(), Charsets.UTF_8);
    Map<CharSequence, CharSequence> eventHeaders = avroevent.getHeaders();

    assertEquals(msg, eventBody);
    assertEquals(TestConstants.CUSTOM_KEY, consumerRecord.key());

    assertEquals(TestConstants.HEADER_1_VALUE,
                 eventHeaders.get(new Utf8(TestConstants.HEADER_1_KEY)).toString());
    assertEquals(TestConstants.CUSTOM_KEY, eventHeaders.get(new Utf8("key")).toString());
  }

  private ConsumerRecords<String, String> pollConsumerRecords(String topic) {
    return pollConsumerRecords(topic, 20);
  }

  private ConsumerRecords<String, String> pollConsumerRecords(String topic, int maxIter) {
    ConsumerRecords<String, String> recs = null;
    for (int i = 0; i < maxIter; i++) {
      recs = testUtil.getNextMessageFromConsumer(topic);
      if (recs.count() > 0) break;
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException e) {
        //
      }
    }
    return recs;
  }

  @Test
  public void testEmptyChannel() throws EventDeliveryException {
    Sink kafkaSink = new KafkaSink();
    Context context = prepareDefaultContext();
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    Sink.Status status = kafkaSink.process();
    if (status != Sink.Status.BACKOFF) {
      fail("Error Occurred");
    }
    ConsumerRecords recs = pollConsumerRecords(DEFAULT_TOPIC, 2);
    assertNotNull(recs);
    assertEquals(recs.count(), 0);
  }

  @Test
  public void testPartitionHeaderSet() throws Exception {
    doPartitionHeader(PartitionTestScenario.PARTITION_ID_HEADER_ONLY);
  }

  @Test
  public void testPartitionHeaderNotSet() throws Exception {
    doPartitionHeader(PartitionTestScenario.NO_PARTITION_HEADERS);
  }

  @Test
  public void testStaticPartitionAndHeaderSet() throws Exception {
    doPartitionHeader(PartitionTestScenario.STATIC_HEADER_AND_PARTITION_ID);
  }

  @Test
  public void testStaticPartitionHeaderNotSet() throws Exception {
    doPartitionHeader(PartitionTestScenario.STATIC_HEADER_ONLY);
  }

  @Test
  public void testPartitionHeaderMissing() throws Exception {
    doPartitionErrors(PartitionOption.NOTSET);
  }

  @Test
  public void testPartitionHeaderOutOfRange() throws Exception {
    Sink kafkaSink = new KafkaSink();
    try {
      doPartitionErrors(PartitionOption.VALIDBUTOUTOFRANGE, kafkaSink);
      fail();
    } catch (EventDeliveryException e) {
      //
    }
    SinkCounter sinkCounter = (SinkCounter) Whitebox.getInternalState(kafkaSink, "counter");
    assertEquals(1, sinkCounter.getEventWriteFail());
  }

  @Test(expected = org.apache.flume.EventDeliveryException.class)
  public void testPartitionHeaderInvalid() throws Exception {
    doPartitionErrors(PartitionOption.NOTANUMBER);
  }

  /**
   * Tests that sub-properties (kafka.producer.*) apply correctly across multiple invocations
   * of configure() (fix for FLUME-2857).
   */
  @Test
  public void testDefaultSettingsOnReConfigure() {
    String sampleProducerProp = "compression.type";
    String sampleProducerVal = "snappy";

    Context context = prepareDefaultContext();
    context.put(KafkaSinkConstants.KAFKA_PRODUCER_PREFIX + sampleProducerProp, sampleProducerVal);

    KafkaSink kafkaSink = new KafkaSink();

    Configurables.configure(kafkaSink, context);

    Assert.assertEquals(sampleProducerVal,
        kafkaSink.getKafkaProps().getProperty(sampleProducerProp));

    context = prepareDefaultContext();
    Configurables.configure(kafkaSink, context);

    Assert.assertNull(kafkaSink.getKafkaProps().getProperty(sampleProducerProp));

  }

  /**
   * This function tests three scenarios:
   * 1. PartitionOption.VALIDBUTOUTOFRANGE: An integer partition is provided,
   *    however it exceeds the number of partitions available on the topic.
   *    Expected behaviour: ChannelException thrown.
   *
   * 2. PartitionOption.NOTSET: The partition header is not actually set.
   *    Expected behaviour: Exception is not thrown because the code avoids an NPE.
   *
   * 3. PartitionOption.NOTANUMBER: The partition header is set, but is not an Integer.
   *    Expected behaviour: ChannelException thrown.
   *
   */
  private void doPartitionErrors(PartitionOption option) throws Exception {
    doPartitionErrors(option, new KafkaSink());
  }

  private void doPartitionErrors(PartitionOption option, Sink kafkaSink) throws Exception {
    Context context = prepareDefaultContext();
    context.put(KafkaSinkConstants.PARTITION_HEADER_NAME, "partition-header");

    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    String topic = findUnusedTopic();
    createTopic(topic, 5);

    Transaction tx = memoryChannel.getTransaction();
    tx.begin();

    Map<String, String> headers = new HashMap<String, String>();
    headers.put("topic", topic);
    switch (option) {
      case VALIDBUTOUTOFRANGE:
        headers.put("partition-header", "9");
        break;
      case NOTSET:
        headers.put("wrong-header", "2");
        break;
      case NOTANUMBER:
        headers.put("partition-header", "not-a-number");
        break;
      default:
        break;
    }

    Event event = EventBuilder.withBody(String.valueOf(9).getBytes(), headers);

    memoryChannel.put(event);
    tx.commit();
    tx.close();

    Sink.Status status = kafkaSink.process();
    assertEquals(Sink.Status.READY, status);

    deleteTopic(topic);

  }

  /**
   * This method tests both the default behavior (usePartitionHeader=false)
   * and the behaviour when the partitionId setting is used.
   * Under the default behaviour, one would expect an even distribution of
   * messages to partitions, however when partitionId is used we manually create
   * a large skew to some partitions and then verify that this actually happened
   * by reading messages directly using a Kafka Consumer.
   *
   */
  private void doPartitionHeader(PartitionTestScenario scenario) throws Exception {
    final int numPtns = 5;
    final int numMsgs = numPtns * 10;
    final Integer staticPtn = 3;

    String topic = findUnusedTopic();
    createTopic(topic, numPtns);
    Context context = prepareDefaultContext();
    context.put(BATCH_SIZE, "100");

    if (scenario == PartitionTestScenario.PARTITION_ID_HEADER_ONLY ||
        scenario == PartitionTestScenario.STATIC_HEADER_AND_PARTITION_ID) {
      context.put(KafkaSinkConstants.PARTITION_HEADER_NAME,
                  KafkaPartitionTestUtil.PARTITION_HEADER);
    }
    if (scenario == PartitionTestScenario.STATIC_HEADER_AND_PARTITION_ID ||
        scenario == PartitionTestScenario.STATIC_HEADER_ONLY) {
      context.put(KafkaSinkConstants.STATIC_PARTITION_CONF, staticPtn.toString());
    }
    Sink kafkaSink = new KafkaSink();

    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    //Create a map of PartitionId:List<Messages> according to the desired distribution
    Map<Integer, List<Event>> partitionMap = new HashMap<Integer, List<Event>>(numPtns);
    for (int i = 0; i < numPtns; i++) {
      partitionMap.put(i, new ArrayList<Event>());
    }
    Transaction tx = memoryChannel.getTransaction();
    tx.begin();

    List<Event> orderedEvents = KafkaPartitionTestUtil.generateSkewedMessageList(scenario, numMsgs,
                                                                 partitionMap, numPtns, staticPtn);

    for (Event event : orderedEvents) {
      event.getHeaders().put("topic", topic);
      memoryChannel.put(event);
    }

    tx.commit();
    tx.close();

    Sink.Status status = kafkaSink.process();
    assertEquals(Sink.Status.READY, status);

    Properties props = new Properties();
    props.put("bootstrap.servers", testUtil.getKafkaServerUrl());
    props.put("group.id", "group_1");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("auto.offset.reset", "earliest");
    Map<Integer, List<byte[]>> resultsMap =
        KafkaPartitionTestUtil.retrieveRecordsFromPartitions(topic, numPtns, props);

    KafkaPartitionTestUtil.checkResultsAgainstSkew(scenario, partitionMap, resultsMap, staticPtn,
                                                   numMsgs);

    memoryChannel.stop();
    kafkaSink.stop();
    deleteTopic(topic);

  }

  private Context prepareDefaultContext() {
    // Prepares a default context with Kafka Server Properties
    Context context = new Context();
    context.put(BOOTSTRAP_SERVERS_CONFIG, testUtil.getKafkaServerUrl());
    context.put(BATCH_SIZE, "1");
    return context;
  }

  private Sink.Status prepareAndSend(Context context, String msg)
      throws EventDeliveryException {
    Sink kafkaSink = new KafkaSink();
    Configurables.configure(kafkaSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    kafkaSink.setChannel(memoryChannel);
    kafkaSink.start();

    Transaction tx = memoryChannel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody(msg.getBytes());
    memoryChannel.put(event);
    tx.commit();
    tx.close();

    return kafkaSink.process();
  }

  private void createTopic(String topicName, int numPartitions) {
    testUtil.createTopics(Collections.singletonList(topicName), numPartitions);
  }

  private void deleteTopic(String topicName) {
    testUtil.deleteTopic(topicName);
  }

  private String findUnusedTopic() {
    String newTopic = null;
    boolean topicFound = false;
    while (!topicFound) {
      newTopic = RandomStringUtils.randomAlphabetic(8);
      if (!usedTopics.contains(newTopic)) {
        usedTopics.add(newTopic);
        topicFound = true;
      }
    }
    return newTopic;
  }

}