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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.ipfilter.IpFilterRule;
import org.jboss.netty.handler.ipfilter.IpFilterRuleHandler;
import org.jboss.netty.handler.ipfilter.PatternRule;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * <p>
 * A {@link Source} implementation that receives Avro events from clients that
 * implement {@link AvroSourceProtocol}.
 * </p>
 * <p>
 * This source forms one half of Flume's tiered collection support. Internally,
 * this source uses Avro's <tt>NettyTransceiver</tt> to listen for, and handle
 * events. It can be paired with the builtin <tt>AvroSink</tt> to create tiered
 * collection topologies. Of course, nothing prevents one from using this source
 * to receive data from other custom built infrastructure that uses the same
 * Avro protocol (specifically {@link AvroSourceProtocol}).
 * </p>
 * <p>
 * Events may be received from the client either singly or in batches.Generally,
 * larger batches are far more efficient, but introduce a slight delay (measured
 * in millis) in delivery. A batch submitted to the configured {@link Channel}
 * atomically (i.e. either all events make it into the channel or none).
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>bind</tt></td>
 * <td>The hostname or IP to which the source will bind.</td>
 * <td>Hostname or IP / String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which the source will bind and listen for events.</td>
 * <td>TCP port / int</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>threads</tt></td>
 * <td>Max number of threads assigned to thread pool, 0 being unlimited</td>
 * <td>Count / int</td>
 * <td>0(optional)</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class AvroSource extends SslContextAwareAbstractSource implements EventDrivenSource,
    Configurable, AvroSourceProtocol {

  private static final String THREADS = "threads";

  private static final Logger logger = LoggerFactory
      .getLogger(AvroSource.class);

  private static final String PORT_KEY = "port";
  private static final String BIND_KEY = "bind";
  private static final String COMPRESSION_TYPE = "compression-type";
  private static final String IP_FILTER_KEY = "ipFilter";
  private static final String IP_FILTER_RULES_KEY = "ipFilterRules";
  private int port;
  private String bindAddress;
  private String compressionType;
  private boolean enableIpFilter;
  private String patternRuleConfigDefinition;

  private NioServerSocketChannelFactory socketChannelFactory;
  private Server server;
  private SourceCounter sourceCounter;

  private int maxThreads;
  private ScheduledExecutorService connectionCountUpdater;

  private List<IpFilterRule> rules;

  @Override
  public void configure(Context context) {
    configureSsl(context);
    Configurables.ensureRequiredNonNull(context, PORT_KEY, BIND_KEY);

    port = context.getInteger(PORT_KEY);
    bindAddress = context.getString(BIND_KEY);
    compressionType = context.getString(COMPRESSION_TYPE, "none");

    try {
      maxThreads = context.getInteger(THREADS, 0);
    } catch (NumberFormatException e) {
      logger.warn("AVRO source\'s \"threads\" property must specify an integer value.",
              context.getString(THREADS));
    }

    enableIpFilter = context.getBoolean(IP_FILTER_KEY, false);
    if (enableIpFilter) {
      patternRuleConfigDefinition = context.getString(IP_FILTER_RULES_KEY);
      if (patternRuleConfigDefinition == null ||
          patternRuleConfigDefinition.trim().isEmpty()) {
        throw new FlumeException(
          "ipFilter is configured with true but ipFilterRules is not defined:" +
            " ");
      }
      String[] patternRuleDefinitions = patternRuleConfigDefinition.split(
        ",");
      rules = new ArrayList<IpFilterRule>(patternRuleDefinitions.length);
      for (String patternRuleDefinition : patternRuleDefinitions) {
        rules.add(generateRule(patternRuleDefinition));
      }
    }

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public void start() {
    logger.info("Starting {}...", this);

    try {
      Responder responder = new SpecificResponder(AvroSourceProtocol.class, this);

      socketChannelFactory = initSocketChannelFactory();

      ChannelPipelineFactory pipelineFactory = initChannelPipelineFactory();

      server = new NettyServer(responder, new InetSocketAddress(bindAddress, port),
              socketChannelFactory, pipelineFactory, null);
    } catch (org.jboss.netty.channel.ChannelException nce) {
      logger.error("Avro source {} startup failed. Cannot initialize Netty server", getName(), nce);
      stop();
      throw new FlumeException("Failed to set up server socket", nce);
    }

    connectionCountUpdater = Executors.newSingleThreadScheduledExecutor();
    server.start();
    sourceCounter.start();
    super.start();
    final NettyServer srv = (NettyServer)server;
    connectionCountUpdater.scheduleWithFixedDelay(
        () -> sourceCounter.setOpenConnectionCount(Long.valueOf(srv.getNumActiveConnections())),
        0, 60, TimeUnit.SECONDS);

    logger.info("Avro source {} started.", getName());
  }

  private NioServerSocketChannelFactory initSocketChannelFactory() {
    NioServerSocketChannelFactory socketChannelFactory;
    if (maxThreads <= 0) {
      socketChannelFactory = new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
              "Avro " + NettyTransceiver.class.getSimpleName() + " Boss-%d").build()),
          Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
              "Avro " + NettyTransceiver.class.getSimpleName() + "  I/O Worker-%d").build()));
    } else {
      socketChannelFactory = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
            "Avro " + NettyTransceiver.class.getSimpleName() + " Boss-%d").build()),
        Executors.newFixedThreadPool(maxThreads, new ThreadFactoryBuilder().setNameFormat(
            "Avro " + NettyTransceiver.class.getSimpleName() + "  I/O Worker-%d").build()));
    }
    return socketChannelFactory;
  }

  private ChannelPipelineFactory initChannelPipelineFactory() {
    ChannelPipelineFactory pipelineFactory;
    boolean enableCompression = compressionType.equalsIgnoreCase("deflate");
    if (enableCompression || isSslEnabled() || enableIpFilter) {
      pipelineFactory = new AdvancedChannelPipelineFactory(
        enableCompression, enableIpFilter,
        patternRuleConfigDefinition, getSslEngineSupplier(false));
    } else {
      pipelineFactory = Channels::pipeline;
    }
    return pipelineFactory;
  }

  @Override
  public void stop() {
    logger.info("Avro source {} stopping: {}", getName(), this);

    if (server != null) {
      server.close();
      try {
        server.join();
        server = null;
      } catch (InterruptedException e) {
        logger.info("Avro source " + getName() + ": Interrupted while waiting " +
                "for Avro server to stop. Exiting. Exception follows.", e);
        Thread.currentThread().interrupt();
      }
    }

    if (socketChannelFactory != null) {
      socketChannelFactory.releaseExternalResources();
      socketChannelFactory = null;
    }

    sourceCounter.stop();
    if (connectionCountUpdater != null) {
      connectionCountUpdater.shutdownNow();
      connectionCountUpdater = null;
    }

    super.stop();
    logger.info("Avro source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  @Override
  public String toString() {
    return "Avro source " + getName() + ": { bindAddress: " + bindAddress +
        ", port: " + port + " }";
  }

  /**
   * Helper function to convert a map of CharSequence to a map of String.
   */
  private static Map<String, String> toStringMap(
      Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap =
        new HashMap<String, String>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }

  @Override
  public Status append(AvroFlumeEvent avroEvent) {
    if (logger.isDebugEnabled()) {
      if (LogPrivacyUtil.allowLogRawData()) {
        logger.debug("Avro source {}: Received avro event: {}", getName(), avroEvent);
      } else {
        logger.debug("Avro source {}: Received avro event", getName());
      }
    }

    sourceCounter.incrementAppendReceivedCount();
    sourceCounter.incrementEventReceivedCount();

    Event event = EventBuilder.withBody(avroEvent.getBody().array(),
        toStringMap(avroEvent.getHeaders()));

    try {
      getChannelProcessor().processEvent(event);
    } catch (ChannelException ex) {
      logger.warn("Avro source " + getName() + ": Unable to process event. " +
          "Exception follows.", ex);
      sourceCounter.incrementChannelWriteFail();
      return Status.FAILED;
    }

    sourceCounter.incrementAppendAcceptedCount();
    sourceCounter.incrementEventAcceptedCount();

    return Status.OK;
  }

  @Override
  public Status appendBatch(List<AvroFlumeEvent> events) {
    logger.debug("Avro source {}: Received avro event batch of {} events.",
        getName(), events.size());
    sourceCounter.incrementAppendBatchReceivedCount();
    sourceCounter.addToEventReceivedCount(events.size());

    List<Event> batch = new ArrayList<Event>();

    for (AvroFlumeEvent avroEvent : events) {
      Event event = EventBuilder.withBody(avroEvent.getBody().array(),
          toStringMap(avroEvent.getHeaders()));

      batch.add(event);
    }

    try {
      getChannelProcessor().processEventBatch(batch);
    } catch (Throwable t) {
      logger.error("Avro source " + getName() + ": Unable to process event " +
          "batch. Exception follows.", t);
      sourceCounter.incrementChannelWriteFail();
      if (t instanceof Error) {
        throw (Error) t;
      }
      return Status.FAILED;
    }

    sourceCounter.incrementAppendBatchAcceptedCount();
    sourceCounter.addToEventAcceptedCount(events.size());

    return Status.OK;
  }

  private PatternRule generateRule(String patternRuleDefinition) throws FlumeException {
    patternRuleDefinition = patternRuleDefinition.trim();
    //first validate the format
    int firstColonIndex = patternRuleDefinition.indexOf(":");
    if (firstColonIndex == -1) {
      throw new FlumeException(
          "Invalid ipFilter patternRule '" + patternRuleDefinition +
          "' should look like <'allow'  or 'deny'>:<'ip' or " +
          "'name'>:<pattern>");
    } else {
      String ruleAccessFlag = patternRuleDefinition.substring(0, firstColonIndex);
      int secondColonIndex = patternRuleDefinition.indexOf(":", firstColonIndex + 1);
      if ((!ruleAccessFlag.equals("allow") && !ruleAccessFlag.equals("deny")) ||
          secondColonIndex == -1) {
        throw new FlumeException(
            "Invalid ipFilter patternRule '" + patternRuleDefinition +
            "' should look like <'allow'  or 'deny'>:<'ip' or " +
            "'name'>:<pattern>");
      }

      String patternTypeFlag = patternRuleDefinition.substring(
          firstColonIndex + 1, secondColonIndex);
      if ((!patternTypeFlag.equals("ip") && !patternTypeFlag.equals("name"))) {
        throw new FlumeException(
            "Invalid ipFilter patternRule '" + patternRuleDefinition +
            "' should look like <'allow'  or 'deny'>:<'ip' or " +
            "'name'>:<pattern>");
      }

      boolean isAllow = ruleAccessFlag.equals("allow");
      String patternRuleString = (patternTypeFlag.equals("ip") ? "i" : "n")
          + ":" + patternRuleDefinition.substring(secondColonIndex + 1);
      logger.info("Adding ipFilter PatternRule: "
          + (isAllow ? "Allow" : "deny") + " " + patternRuleString);
      return new PatternRule(isAllow, patternRuleString);
    }
  }

  /**
   * Factory of SSL-enabled server worker channel pipelines
   * Copied from Avro's org.apache.avro.ipc.TestNettyServerWithSSL test
   */
  private class AdvancedChannelPipelineFactory
      implements ChannelPipelineFactory {

    private boolean enableCompression;

    private boolean enableIpFilter;
    private String patternRuleConfigDefinition;
    private Supplier<Optional<SSLEngine>> sslEngineSupplier;

    public AdvancedChannelPipelineFactory(boolean enableCompression, boolean enableIpFilter,
        String patternRuleConfigDefinition, Supplier<Optional<SSLEngine>> sslEngineSupplier) {
      this.enableCompression = enableCompression;
      this.enableIpFilter = enableIpFilter;
      this.patternRuleConfigDefinition = patternRuleConfigDefinition;
      this.sslEngineSupplier = sslEngineSupplier;
    }


    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      if (enableCompression) {
        ZlibEncoder encoder = new ZlibEncoder(6);
        pipeline.addFirst("deflater", encoder);
        pipeline.addFirst("inflater", new ZlibDecoder());
      }

      sslEngineSupplier.get().ifPresent(sslEngine -> {
        logger.info("SSLEngine protocols enabled: " +
            Arrays.asList(sslEngine.getEnabledProtocols()));
        // addFirst() will make SSL handling the first stage of decoding
        // and the last stage of encoding this must be added after
        // adding compression handling above
        pipeline.addFirst("ssl", new SslHandler(sslEngine));
      });

      if (enableIpFilter) {

        logger.info("Setting up ipFilter with the following rule definition: " +
                    patternRuleConfigDefinition);
        IpFilterRuleHandler ipFilterHandler = new IpFilterRuleHandler();
        ipFilterHandler.addAll(rules);
        logger.info("Adding ipFilter with " + ipFilterHandler.size() + " rules");

        pipeline.addFirst("ipFilter", ipFilterHandler);
      }

      return pipeline;
    }
  }
}
