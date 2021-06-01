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
package org.apache.flume.conf.sink;

import org.apache.flume.conf.ComponentWithClassName;

public enum SinkProcessorType implements ComponentWithClassName {
  /**
   * Place holder for custom sinks not part of this enumeration.
   */
  OTHER(null),

  /**
   * Failover processor
   *
   * @see org.apache.flume.sink.FailoverSinkProcessor
   */
  FAILOVER("org.apache.flume.sink.FailoverSinkProcessor"),

  /**
   * Standard processor
   *
   * @see org.apache.flume.sink.DefaultSinkProcessor
   */
  DEFAULT("org.apache.flume.sink.DefaultSinkProcessor"),


  /**
   * Load balancing processor
   *
   * @see org.apache.flume.sink.LoadBalancingSinkProcessor
   */
  LOAD_BALANCE("org.apache.flume.sink.LoadBalancingSinkProcessor");

  private final String processorClassName;

  private SinkProcessorType(String processorClassName) {
    this.processorClassName = processorClassName;
  }

  @Deprecated
  public String getSinkProcessorClassName() {
    return processorClassName;
  }

  @Override
  public String getClassName() {
    return processorClassName;
  }
}
