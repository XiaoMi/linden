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

package com.xiaomi.linden.service.admin;

import java.io.IOException;

import com.google.common.base.Throwables;
import com.xiaomi.linden.common.util.CommonUtils;
import com.xiaomi.linden.service.CoreLindenServiceImpl;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LindenAdmin {

  private static Logger LOGGER = LoggerFactory.getLogger(LindenAdmin.class);
  private String webappPath;
  private final Server server;
  private static CoreLindenServiceImpl service;
  private int port;

  public LindenAdmin(CoreLindenServiceImpl service, Integer port, String webappPath) throws IOException {
    LindenAdmin.service = service;
    this.webappPath = webappPath;
    this.port = port;
    server = new Server(port);
  }

  public void start() throws Exception {
    WebAppContext webAppContext = new WebAppContext();
    webAppContext.setContextPath("/");
    webAppContext.setWar(webappPath);

    webAppContext.setServer(server);
    server.setHandler(webAppContext);

    String host = CommonUtils.getLocalHostIp();
    System.out.printf("Linden admin started: http://%s:%d\n", host, port);
    LOGGER.info("Linden admin started: http://{}:{}", host, port);
    server.start();
    server.join();
  }

  public static CoreLindenServiceImpl getService() {
    return service;
  }

  public void close() {
    try {
      server.stop();
    } catch (Exception e) {
      LOGGER.error("Linden admin stopped failed : {}", Throwables.getStackTraceAsString(e));
    }
  }

  public static void main(String[] args) throws Exception {
    int port = 10000;
    String webappPath = "/myclient/git/linden/linden-core/src/main/webapp";
    LindenAdmin admin = new LindenAdmin(null, port,webappPath);
    admin.start();
  }
}
