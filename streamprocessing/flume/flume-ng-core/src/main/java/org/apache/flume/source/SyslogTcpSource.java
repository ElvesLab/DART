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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.net.ssl.SSLEngine;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated use {@link MultiportSyslogTCPSource} instead.
 */
@Deprecated
public class SyslogTcpSource extends SslContextAwareAbstractSource
    implements EventDrivenSource, Configurable {
  private static final Logger logger = LoggerFactory.getLogger(SyslogTcpSource.class);

  private int port;
  private String host = null;
  private Channel nettyChannel;
  private Integer eventSize;
  private Map<String, String> formaterProp;
  private SourceCounter sourceCounter;
  private Set<String> keepFields;
  private String clientIPHeader;
  private String clientHostnameHeader;

  public class syslogTcpHandler extends SimpleChannelHandler {

    private SyslogUtils syslogUtils = new SyslogUtils();
    private String clientIPHeader;
    private String clientHostnameHeader;

    public void setEventSize(int eventSize) {
      syslogUtils.setEventSize(eventSize);
    }

    public void setKeepFields(Set<String> keepFields) {
      syslogUtils.setKeepFields(keepFields);
    }

    public void setFormater(Map<String, String> prop) {
      syslogUtils.addFormats(prop);
    }

    public void setClientIPHeader(String clientIPHeader) {
      this.clientIPHeader = clientIPHeader;
    }

    public void setClientHostnameHeader(String clientHostnameHeader) {
      this.clientHostnameHeader = clientHostnameHeader;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent mEvent) {
      ChannelBuffer buff = (ChannelBuffer) mEvent.getMessage();
      while (buff.readable()) {
        Event e = syslogUtils.extractEvent(buff);
        if (e == null) {
          logger.debug("Parsed partial event, event will be generated when " +
              "rest of the event is received.");
          continue;
        }

        if (clientIPHeader != null) {
          e.getHeaders().put(clientIPHeader,
              SyslogUtils.getIP(ctx.getChannel().getRemoteAddress()));
        }

        if (clientHostnameHeader != null) {
          e.getHeaders().put(clientHostnameHeader,
              SyslogUtils.getHostname(ctx.getChannel().getRemoteAddress()));
        }

        sourceCounter.incrementEventReceivedCount();

        try {
          getChannelProcessor().processEvent(e);
          sourceCounter.incrementEventAcceptedCount();
        } catch (ChannelException ex) {
          logger.error("Error writting to channel, event dropped", ex);
          sourceCounter.incrementChannelWriteFail();
        } catch (RuntimeException ex) {
          logger.error("Error parsing event from syslog stream, event dropped", ex);
          sourceCounter.incrementEventReadFail();
          return;
        }
      }

    }
  }

  @Override
  public void start() {
    ChannelFactory factory = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

    ServerBootstrap serverBootstrap = new ServerBootstrap(factory);

    serverBootstrap.setPipelineFactory(new PipelineFactory(
        eventSize, formaterProp, keepFields, clientIPHeader, clientHostnameHeader,
        getSslEngineSupplier(false)
    ));
    logger.info("Syslog TCP Source starting...");

    if (host == null) {
      nettyChannel = serverBootstrap.bind(new InetSocketAddress(port));
    } else {
      nettyChannel = serverBootstrap.bind(new InetSocketAddress(host, port));
    }

    sourceCounter.start();
    super.start();
  }

  @Override
  public void stop() {
    logger.info("Syslog TCP Source stopping...");
    logger.info("Metrics: {}", sourceCounter);

    if (nettyChannel != null) {
      nettyChannel.close();
      try {
        nettyChannel.getCloseFuture().await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("netty server stop interrupted", e);
      } finally {
        nettyChannel = null;
      }
    }

    sourceCounter.stop();
    super.stop();
  }

  @Override
  public void configure(Context context) {
    configureSsl(context);
    Configurables.ensureRequiredNonNull(context,
        SyslogSourceConfigurationConstants.CONFIG_PORT);
    port = context.getInteger(SyslogSourceConfigurationConstants.CONFIG_PORT);
    host = context.getString(SyslogSourceConfigurationConstants.CONFIG_HOST);
    eventSize = context.getInteger("eventSize", SyslogUtils.DEFAULT_SIZE);
    formaterProp = context.getSubProperties(
        SyslogSourceConfigurationConstants.CONFIG_FORMAT_PREFIX);
    keepFields = SyslogUtils.chooseFieldsToKeep(
        context.getString(
            SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS,
            SyslogSourceConfigurationConstants.DEFAULT_KEEP_FIELDS));
    clientIPHeader =
        context.getString(SyslogSourceConfigurationConstants.CONFIG_CLIENT_IP_HEADER);
    clientHostnameHeader =
        context.getString(SyslogSourceConfigurationConstants.CONFIG_CLIENT_HOSTNAME_HEADER);

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @VisibleForTesting
  InetSocketAddress getBoundAddress() {
    SocketAddress localAddress = nettyChannel.getLocalAddress();
    if (!(localAddress instanceof InetSocketAddress)) {
      throw new IllegalArgumentException("Not bound to an internet address");
    }
    return (InetSocketAddress) localAddress;
  }


  @VisibleForTesting
  SourceCounter getSourceCounter() {
    return sourceCounter;
  }

  private class PipelineFactory implements ChannelPipelineFactory {
    private final Integer eventSize;
    private final Map<String, String> formaterProp;
    private final Set<String> keepFields;
    private String clientIPHeader;
    private String clientHostnameHeader;
    private Supplier<Optional<SSLEngine>> sslEngineSupplier;

    public PipelineFactory(Integer eventSize, Map<String, String> formaterProp,
        Set<String> keepFields, String clientIPHeader, String clientHostnameHeader,
        Supplier<Optional<SSLEngine>> sslEngineSupplier) {
      this.eventSize = eventSize;
      this.formaterProp = formaterProp;
      this.keepFields = keepFields;
      this.clientIPHeader = clientIPHeader;
      this.clientHostnameHeader = clientHostnameHeader;
      this.sslEngineSupplier = sslEngineSupplier;
    }

    @Override
    public ChannelPipeline getPipeline() {
      syslogTcpHandler handler = new syslogTcpHandler();
      handler.setEventSize(eventSize);
      handler.setFormater(formaterProp);
      handler.setKeepFields(keepFields);
      handler.setClientIPHeader(clientIPHeader);
      handler.setClientHostnameHeader(clientHostnameHeader);

      ChannelPipeline pipeline = Channels.pipeline(handler);

      sslEngineSupplier.get().ifPresent(sslEngine -> {
        pipeline.addFirst("ssl", new SslHandler(sslEngine));
      });

      return pipeline;
    }
  }
}
