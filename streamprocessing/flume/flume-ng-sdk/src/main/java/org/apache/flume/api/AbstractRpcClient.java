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
package org.apache.flume.api;

import java.util.List;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRpcClient implements RpcClient {
  private static Logger logger = LoggerFactory.getLogger(AbstractRpcClient.class);

  protected int batchSize =
      RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE;
  protected long connectTimeout =
      RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS;
  protected long requestTimeout =
      RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS;

  @Override
  public int getBatchSize() {
    return batchSize;
  }
  @Override
  public abstract void append(Event event) throws EventDeliveryException;

  @Override
  public abstract void appendBatch(List<Event> events)
      throws EventDeliveryException;

  @Override
  public abstract boolean isActive();

  @Override
  public abstract void close() throws FlumeException;


  /**
   * Configure the client using the given properties object.
   * @param properties
   * @throws FlumeException if the client can not be configured using this
   * method, or if the client was already configured once.
   */
  protected abstract void configure(Properties properties)
      throws FlumeException;

  /**
   * This is to parse the batch size config for rpc clients
   * @param properties config
   * @return batch size
   */
  public static int parseBatchSize(Properties properties) {
    String strBatchSize = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_BATCH_SIZE);
    logger.debug("Batch size string = " + strBatchSize);
    int batchSize = RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE;
    if (strBatchSize != null && !strBatchSize.isEmpty()) {
      try {
        int parsedBatch = Integer.parseInt(strBatchSize);
        if (parsedBatch < 1) {
          logger.warn("Invalid value for batchSize: {}; Using default value.", parsedBatch);
        } else {
          batchSize = parsedBatch;
        }
      } catch (NumberFormatException e) {
        logger.warn("Batchsize is not valid for RpcClient: " + strBatchSize +
            ". Default value assigned.", e);
      }
    }

    return batchSize;
  }

}
