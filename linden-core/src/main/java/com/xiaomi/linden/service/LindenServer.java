// Copyright 2016 Xiaomi, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.xiaomi.linden.service;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;

import com.xiaomi.linden.cluster.ClusterAnnouncer;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.LindenConfigBuilder;
import com.xiaomi.linden.core.LindenSchemaBuilder;
import com.xiaomi.linden.service.admin.LindenAdmin;
import com.xiaomi.linden.thrift.common.LindenSchema;

public class LindenServer {
  private static Logger LOGGER = LoggerFactory.getLogger(LindenServer.class);
  private static final String LINDEN_PROPERTIES = "linden.properties";
  private static final String SCHEMA_XML = "schema.xml";
  private static final String LOG4j2_XML = "log4j2.xml";
  private static final String LOG_PATH = "log.path";
  private static final String LOG4J_SHUTDOWN_HOOK_ENABLED = "log4j.shutdownHookEnabled";
  private static final String LOG4J_CONTEXT_SELECTOR = "Log4jContextSelector";

  private int port;
  private LindenConfig lindenConf;
  private CoreLindenServiceImpl impl;
  private ListeningServer server;
  private ClusterAnnouncer clusterAnnouncer;
  private Thread adminThread;

  public LindenServer(String conf) throws Exception {
    File lindenProperties = new File(FilenameUtils.concat(conf, LINDEN_PROPERTIES));
    File schemaXml = new File(FilenameUtils.concat(conf, SCHEMA_XML));
    Preconditions.checkArgument(lindenProperties.exists(), "can not find linden.properties.");

    lindenConf = LindenConfigBuilder.build(lindenProperties);
    if (schemaXml.exists()) {
      LindenSchema schema = LindenSchemaBuilder.build(schemaXml);
      lindenConf.setSchema(schema);
    } else {
      throw new Exception("schema.xml not found.");
    }
    port = lindenConf.getPort();
    Preconditions.checkNotNull(lindenConf.getLogPath(), "log path can not be null.");
    System.setProperty(LOG_PATH, lindenConf.getLogPath());
    System.setProperty(LOG4J_SHUTDOWN_HOOK_ENABLED, "false");
    System.setProperty(LOG4J_CONTEXT_SELECTOR, "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    ctx.setConfigLocation(new File(FilenameUtils.concat(conf, LOG4j2_XML)).toURI());
    ctx.reconfigure();
    LOGGER = LoggerFactory.getLogger(LindenServer.class);
  }

  public void startServer() throws Exception {
    impl = new CoreLindenServiceImpl(lindenConf);
    server = Thrift.serveIface(new InetSocketAddress(port), impl);

    LOGGER.info("LindenServer registered to cluster.");
    clusterAnnouncer = new ClusterAnnouncer(lindenConf);
    clusterAnnouncer.announce();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
          close();
      }
    });
    LOGGER.info("LindenServer started.");

    // start admin server.
    if (lindenConf.getAdminPort() > 0) {
      adminThread = new Thread() {
        @Override
        public void run() {
          try {
            LindenAdmin admin = new LindenAdmin(impl, lindenConf.getAdminPort(), lindenConf.getWebapp());
            admin.start();
          } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Admin start failed : {}", Throwables.getStackTraceAsString(e));
          }
        }
      };
      adminThread.start();
    }
    Await.ready(server);
  }

  public void refresh() throws IOException {
    impl.refresh();
  }

  public void close() {
    try {
      if (clusterAnnouncer != null)
        clusterAnnouncer.unannounce();
      if (impl != null)
        impl.close();
      if (server != null)
        server.close();
      if (adminThread != null)
        adminThread.interrupt();
      LOGGER.info("Linden shutdown.");
    } catch (Exception e) {
      LOGGER.info("LindenServer shutdown error : {}", Throwables.getStackTraceAsString(e));
    }
  }

  public static void main(String[] args) throws IOException {
    Preconditions.checkArgument(args.length != 0, "need conf dir");
    String conf = args[0];
    LindenServer server = null;
    try {
      server = new LindenServer(conf);
      server.startServer();
    } catch (Exception e) {
      if (server != null)
        try {
          server.close();
        } catch (Exception e1) {
          e1.printStackTrace();
        }
      e.printStackTrace();
      LOGGER.error("Server start failed : {}", Throwables.getStackTraceAsString(e));
    }
  }
}
