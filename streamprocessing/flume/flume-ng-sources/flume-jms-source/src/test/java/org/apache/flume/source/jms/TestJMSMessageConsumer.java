/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.jms;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.List;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.junit.Test;

import com.google.common.base.Optional;

public class TestJMSMessageConsumer extends JMSMessageConsumerTestBase {

  @Test(expected = FlumeException.class)
  public void testCreateConnectionFails() throws Exception {
    when(connectionFactory.createConnection(USERNAME, PASSWORD))
      .thenThrow(new JMSException(""));
    create();
  }
  @Test
  public void testCreateSessionFails() throws Exception {
    when(connection.createSession(true, Session.SESSION_TRANSACTED))
      .thenThrow(new JMSException(""));
    try {
      create();
      fail("Expected exception: org.apache.flume.FlumeException");
    } catch (FlumeException e) {
      verify(connection).close();
    }
  }
  @Test
  public void testCreateQueueFails() throws Exception {
    when(session.createQueue(destinationName))
      .thenThrow(new JMSException(""));
    try {
      create();
      fail("Expected exception: org.apache.flume.FlumeException");
    } catch (FlumeException e) {
      verify(session).close();
      verify(connection).close();
    }
  }
  @Test
  public void testCreateTopicFails() throws Exception {
    destinationType = JMSDestinationType.TOPIC;
    when(session.createTopic(destinationName))
      .thenThrow(new JMSException(""));
    try {
      create();
      fail("Expected exception: org.apache.flume.FlumeException");
    } catch (FlumeException e) {
      verify(session).close();
      verify(connection).close();
    }
  }
  @Test
  public void testCreateConsumerFails() throws Exception {
    when(session.createConsumer(any(Destination.class), anyString()))
      .thenThrow(new JMSException(""));
    try {
      create();
      fail("Expected exception: org.apache.flume.FlumeException");
    } catch (FlumeException e) {
      verify(session).close();
      verify(connection).close();
    }
  }
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBatchSizeZero() throws Exception {
    batchSize = 0;
    create();
  }
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPollTime() throws Exception {
    pollTimeout = -1L;
    create();
  }
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBatchSizeNegative() throws Exception {
    batchSize = -1;
    create();
  }

  @Test
  public void testQueue() throws Exception {
    destinationType = JMSDestinationType.QUEUE;
    when(session.createQueue(destinationName)).thenReturn(queue);
    consumer = create();
    List<Event> events = consumer.take();
    assertEquals(batchSize, events.size());
    assertBodyIsExpected(events);
    verify(session, never()).createTopic(anyString());
  }
  @Test
  public void testTopic() throws Exception {
    destinationType = JMSDestinationType.TOPIC;
    when(session.createTopic(destinationName)).thenReturn(topic);
    consumer = create();
    List<Event> events = consumer.take();
    assertEquals(batchSize, events.size());
    assertBodyIsExpected(events);
    verify(session, never()).createQueue(anyString());
  }
  @Test
  public void testUserPass() throws Exception {
    consumer = create();
    List<Event> events = consumer.take();
    assertEquals(batchSize, events.size());
    assertBodyIsExpected(events);
  }
  @Test
  public void testNoUserPass() throws Exception {
    userName = Optional.absent();
    when(connectionFactory.createConnection(USERNAME, PASSWORD)).thenThrow(new AssertionError());
    when(connectionFactory.createConnection()).thenReturn(connection);
    consumer = create();
    List<Event> events = consumer.take();
    assertEquals(batchSize, events.size());
    assertBodyIsExpected(events);
  }
  @Test
  public void testNoEvents() throws Exception {
    when(messageConsumer.receive(anyLong())).thenReturn(null);
    consumer = create();
    List<Event> events = consumer.take();
    assertEquals(0, events.size());
    verify(messageConsumer, times(1)).receive(anyLong());
    verifyNoMoreInteractions(messageConsumer);
  }
  @Test
  public void testSingleEvent() throws Exception {
    when(messageConsumer.receiveNoWait()).thenReturn(null);
    consumer = create();
    List<Event> events = consumer.take();
    assertEquals(1, events.size());
    assertBodyIsExpected(events);
  }
  @Test
  public void testPartialBatch() throws Exception {
    when(messageConsumer.receiveNoWait()).thenReturn(message, (Message)null);
    consumer = create();
    List<Event> events = consumer.take();
    assertEquals(2, events.size());
    assertBodyIsExpected(events);
  }
  @Test
  public void testCommit() throws Exception {
    consumer = create();
    consumer.commit();
    verify(session, times(1)).commit();
  }
  @Test
  public void testRollback() throws Exception {
    consumer = create();
    consumer.rollback();
    verify(session, times(1)).rollback();
  }
  @Test
  public void testClose() throws Exception {
    doThrow(new JMSException("")).when(session).close();
    consumer = create();
    consumer.close();
    verify(session, times(1)).close();
    verify(connection, times(1)).close();
  }

  @Test
  public void testCreateDurableSubscription() throws Exception {
    String name = "SUBSCRIPTION_NAME";
    String clientID = "CLIENT_ID";
    TopicSubscriber mockTopicSubscriber = mock(TopicSubscriber.class);
    when(session.createDurableSubscriber(any(Topic.class), anyString(), anyString(), anyBoolean()))
      .thenReturn(mockTopicSubscriber );
    when(session.createTopic(destinationName)).thenReturn(topic);
    new JMSMessageConsumer(WONT_USE, connectionFactory, destinationName, destinationLocator,
        JMSDestinationType.TOPIC, messageSelector, batchSize, pollTimeout, converter, userName,
        password, Optional.of(clientID), true, name);
    verify(connection, times(1)).setClientID(clientID);
    verify(session, times(1)).createDurableSubscriber(topic, name, messageSelector, true);
  }

  @Test(expected = JMSException.class)
  public void testTakeFailsDueToJMSExceptionFromReceive() throws JMSException {
    when(messageConsumer.receive(anyLong())).thenThrow(new JMSException(""));
    consumer = create();

    consumer.take();
  }

  @Test(expected = JMSException.class)
  public void testTakeFailsDueToRuntimeExceptionFromReceive() throws JMSException {
    when(messageConsumer.receive(anyLong())).thenThrow(new RuntimeException());
    consumer = create();

    consumer.take();
  }

  @Test(expected = JMSException.class)
  public void testTakeFailsDueToJMSExceptionFromReceiveNoWait() throws JMSException {
    when(messageConsumer.receiveNoWait()).thenThrow(new JMSException(""));
    consumer = create();

    consumer.take();
  }

  @Test(expected = JMSException.class)
  public void testTakeFailsDueToRuntimeExceptionFromReceiveNoWait() throws JMSException {
    when(messageConsumer.receiveNoWait()).thenThrow(new RuntimeException());
    consumer = create();

    consumer.take();
  }

  @Test
  public void testCommitFailsDueToJMSException() throws JMSException {
    doThrow(new JMSException("")).when(session).commit();
    consumer = create();

    consumer.commit();
  }

  @Test
  public void testCommitFailsDueToRuntimeException() throws JMSException {
    doThrow(new RuntimeException()).when(session).commit();
    consumer = create();

    consumer.commit();
  }

  @Test
  public void testRollbackFailsDueToJMSException() throws JMSException {
    doThrow(new JMSException("")).when(session).rollback();
    consumer = create();

    consumer.rollback();
  }

  @Test
  public void testRollbackFailsDueToRuntimeException() throws JMSException {
    doThrow(new RuntimeException()).when(session).rollback();
    consumer = create();

    consumer.rollback();
  }

}
