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
package org.apache.flume.source.jms;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.List;

class JMSMessageConsumer {
  private static final Logger logger = LoggerFactory
      .getLogger(JMSMessageConsumer.class);

  private final int batchSize;
  private final long pollTimeout;
  private final JMSMessageConverter messageConverter;

  private final Connection connection;
  private final Session session;
  private final Destination destination;
  private final MessageConsumer messageConsumer;

  JMSMessageConsumer(InitialContext initialContext, ConnectionFactory connectionFactory,
                     String destinationName, JMSDestinationLocator destinationLocator,
                     JMSDestinationType destinationType, String messageSelector, int batchSize,
                     long pollTimeout, JMSMessageConverter messageConverter,
                     Optional<String> userName, Optional<String> password,
                     Optional<String> clientId, boolean createDurableSubscription,
                     String durableSubscriptionName) {
    this.batchSize = batchSize;
    this.pollTimeout = pollTimeout;
    this.messageConverter = messageConverter;
    Preconditions.checkArgument(batchSize > 0, "Batch size must be greater "
        + "than zero");
    Preconditions.checkArgument(pollTimeout >= 0, "Poll timeout cannot be " +
        "negative");

    try {
      try {
        if (userName.isPresent()) {
          connection = connectionFactory.createConnection(userName.get(), password.get());
        } else {
          connection = connectionFactory.createConnection();
        }
        if (clientId.isPresent()) {
          connection.setClientID(clientId.get());
        }
        connection.start();
      } catch (JMSException e) {
        throw new FlumeException("Could not create connection to broker", e);
      }

      try {
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
      } catch (JMSException e) {
        throw new FlumeException("Could not create session", e);
      }

      try {
        if (destinationLocator.equals(JMSDestinationLocator.CDI)) {
          switch (destinationType) {
            case QUEUE:
              destination = session.createQueue(destinationName);
              break;
            case TOPIC:
              destination = session.createTopic(destinationName);
              break;
            default:
              throw new IllegalStateException(String.valueOf(destinationType));
          }
        } else {
          destination = (Destination) initialContext.lookup(destinationName);
        }
      } catch (JMSException e) {
        throw new FlumeException("Could not create destination " + destinationName, e);
      } catch (NamingException e) {
        throw new FlumeException("Could not find destination " + destinationName, e);
      }

      try {
        if (createDurableSubscription) {
          messageConsumer = session.createDurableSubscriber(
                  (Topic) destination, durableSubscriptionName,
                  messageSelector.isEmpty() ? null : messageSelector, true);
        } else {
          messageConsumer = session.createConsumer(destination,
                  messageSelector.isEmpty() ? null : messageSelector);
        }
      } catch (JMSException e) {
        throw new FlumeException("Could not create consumer", e);
      }
      String startupMsg = String.format("Connected to '%s' of type '%s' with " +
                      "user '%s', batch size '%d', selector '%s' ", destinationName,
              destinationType, userName.isPresent() ? userName.get() : "null",
              batchSize, messageSelector.isEmpty() ? null : messageSelector);
      logger.info(startupMsg);
    } catch (Exception e) {
      close();
      throw e;
    }
  }

  List<Event> take() throws JMSException {
    List<Event> result = new ArrayList<Event>(batchSize);
    Message message;
    message = receive();
    if (message != null) {
      result.addAll(messageConverter.convert(message));
      int max = batchSize - 1;
      for (int i = 0; i < max; i++) {
        message = receiveNoWait();
        if (message == null) {
          break;
        }
        result.addAll(messageConverter.convert(message));
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("Took batch of %s from %s", result.size(), destination));
    }
    return result;
  }

  private Message receive() throws JMSException {
    try {
      return messageConsumer.receive(pollTimeout);
    } catch (RuntimeException runtimeException) {
      JMSException jmsException = new JMSException("JMS provider has thrown runtime exception: "
              + runtimeException.getMessage());
      jmsException.setLinkedException(runtimeException);
      throw jmsException;
    }
  }

  private Message receiveNoWait() throws JMSException {
    try {
      return messageConsumer.receiveNoWait();
    } catch (RuntimeException runtimeException) {
      JMSException jmsException = new JMSException("JMS provider has thrown runtime exception: "
              + runtimeException.getMessage());
      jmsException.setLinkedException(runtimeException);
      throw jmsException;
    }
  }

  void commit() {
    try {
      session.commit();
    } catch (JMSException jmsException) {
      logger.warn("JMS Exception processing commit", jmsException);
    } catch (RuntimeException runtimeException) {
      logger.warn("Runtime Exception processing commit", runtimeException);
    }
  }

  void rollback() {
    try {
      session.rollback();
    } catch (JMSException jmsException) {
      logger.warn("JMS Exception processing rollback", jmsException);
    } catch (RuntimeException runtimeException) {
      logger.warn("Runtime Exception processing rollback", runtimeException);
    }
  }

  void close() {
    try {
      if (session != null) {
        session.close();
      }
    } catch (JMSException e) {
      logger.error("Could not destroy session", e);
    }
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (JMSException e) {
      logger.error("Could not destroy connection", e);
    }
  }
}
