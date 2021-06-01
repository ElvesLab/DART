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

package org.apache.flume.source.http;

/**
 *
 */
public class HTTPSourceConfigurationConstants {

  public static final String CONFIG_PORT = "port";
  public static final String CONFIG_HANDLER = "handler";
  public static final String CONFIG_HANDLER_PREFIX =
          CONFIG_HANDLER + ".";

  public static final String CONFIG_BIND = "bind";

  public static final String DEFAULT_BIND = "0.0.0.0";

  public static final String DEFAULT_HANDLER =
          "org.apache.flume.source.http.JSONHandler";

  @Deprecated
  public static final String SSL_KEYSTORE = "keystore";
  @Deprecated
  public static final String SSL_KEYSTORE_PASSWORD = "keystorePassword";
  @Deprecated
  public static final String SSL_ENABLED = "enableSSL";
  @Deprecated
  public static final String EXCLUDE_PROTOCOLS = "excludeProtocols";

}
