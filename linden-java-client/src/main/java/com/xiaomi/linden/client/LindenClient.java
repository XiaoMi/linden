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

package com.xiaomi.linden.client;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSONObject;
import com.github.zkclient.ZkClient;
import com.twitter.finagle.Thrift;
import com.twitter.thrift.ServiceInstance;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.common.LindenZKListener;
import com.xiaomi.linden.common.ZKClientFactory;
import com.xiaomi.linden.common.util.LindenZKPathManager;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenServiceInfo;
import com.xiaomi.linden.thrift.common.Response;
import com.xiaomi.linden.thrift.service.LindenService;

public class LindenClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(LindenClient.class);

  private static final int DEFAULT_TIMEOUT = 3000;

  private final Map<String, LindenService.ServiceIface> clients = new ConcurrentHashMap<>();
  private volatile ClientInterface clientIface;
  private Integer timeout;
  private Duration duration;
  private ZkClient zkClient;
  private String nodesPath;
  private LindenZKListener zkListener;

  private interface ClientInterface {
    LindenService.ServiceIface get();
  }

  private class RoundRobinClient implements ClientInterface {

    private final LindenService.ServiceIface[] serviceIfaces;
    private final Random rand  = new Random();

    public RoundRobinClient(Collection<LindenService.ServiceIface> ifaces) {
      serviceIfaces = ifaces.toArray(new LindenService.ServiceIface[0]);
    }

    public LindenService.ServiceIface get() {
      return serviceIfaces[rand.nextInt(serviceIfaces.length)];
    }
  }

  private class SingleClient implements ClientInterface {

    private final LindenService.ServiceIface serviceIface;

    public SingleClient(LindenService.ServiceIface iface) {
      serviceIface = iface;
    }

    public LindenService.ServiceIface get() {
      return serviceIface;
    }
  }

  public LindenClient(String clusterUrl) {
    this(clusterUrl, DEFAULT_TIMEOUT);
  }

  public LindenClient(String clusterUrl, boolean roundRobin) { this(clusterUrl, DEFAULT_TIMEOUT, roundRobin); }

  public LindenClient(String clusterUrl, int timeout) { this(clusterUrl, timeout, true); }

  public LindenClient(String clusterUrl, int timeout, final boolean roundRobin) {
    this.timeout = timeout;
    this.duration = Duration.apply(timeout, TimeUnit.MILLISECONDS);
    LindenZKPathManager zkPathManager = new LindenZKPathManager(clusterUrl);
    zkClient = ZKClientFactory.getClient(zkPathManager.getZK());
    nodesPath = zkPathManager.getAllNodesPath();
    List<String> children = zkClient.getChildren(nodesPath);

    zkListener = new LindenZKListener(nodesPath, children) {
      @Override
      public void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted) {
        if (children == null || children.isEmpty()) {
          return;
        }

        if (!roundRobin) {
          Collections.sort(children);
          String firstNode = children.get(0);
          LindenService.ServiceIface leader = clients.get(firstNode);
          if (leader == null) {
            leader = buildClient(parent, firstNode);
            clients.put(firstNode, leader);
            LOGGER.info("Linden client [{}] is selected as leader.", parent);
          }
          clientIface = new SingleClient(leader);
          return;
        }

        Map<String, LindenService.ServiceIface> tmpClients = new ConcurrentHashMap<>();
        for (String child : children) {
          if (!clients.containsKey(child)) {
            clients.put(child, buildClient(parent, child));
            LOGGER.info("Linden client [{}] joined to the cluster.", parent);
          }
          tmpClients.put(child, clients.get(child));
        }
        for (String delete : deleted) {
          clients.remove(delete);
        }
        clientIface = new RoundRobinClient(tmpClients.values());
      }
    };
    zkClient.subscribeChildChanges(nodesPath, zkListener);
  }

  private LindenService.ServiceIface buildClient(String parent, String node) {
    String nodePath = FilenameUtils.separatorsToUnix(FilenameUtils.concat(parent, node));
    byte[] bytes = zkClient.readData(nodePath);
    ServiceInstance serviceInstance = JSONObject.parseObject(new String(bytes), ServiceInstance.class);
    String schema = String.format("%s:%s",
                                  serviceInstance.getServiceEndpoint().getHost(),
                                  serviceInstance.getServiceEndpoint().getPort());
    return Thrift.newIface(schema, LindenService.ServiceIface.class);
  }

  // Single point client
  public LindenClient(String host, int port, int timeout) {
    this.timeout = timeout;
    this.duration = Duration.apply(timeout, TimeUnit.MILLISECONDS);
    LindenService.ServiceIface iface = Thrift.newIface(String.format("%s:%d", host, port), LindenService.ServiceIface.class);
    clientIface = new SingleClient(iface);
  }

  public LindenService.ServiceIface get() {
    return clientIface.get();
  }

  public Response index(String content) throws Exception {
    Future<Response> response = get().handleClusterIndexRequest(content);
    return timeout == 0 ? Await.result(response) : Await.result(response, duration);
  }

  public LindenResult search(String bql) throws Exception {
    Future<LindenResult> result = get().handleClusterSearchRequest(bql);
    return timeout == 0 ? Await.result(result) : Await.result(result, duration);
  }

  public Response delete(String bql) throws Exception {
    Future<Response> response = get().handleClusterDeleteRequest(bql);
    return timeout == 0 ? Await.result(response) : Await.result(response, duration);
  }

  public Response executeCommand(String command) throws Exception {
    Future<Response> response = get().handleClusterCommand(command);
    return timeout == 0 ? Await.result(response) : Await.result(response, duration);
  }

  @Deprecated
  public LindenResult searchByBQL(String bql) throws Exception {
    Future<LindenResult> result = get().searchByBqlCluster(bql);
    return timeout == 0 ? Await.result(result) : Await.result(result, duration);
  }

  public LindenServiceInfo getServiceInfo() throws Exception {
    Future<LindenServiceInfo> future = get().getServiceInfo();
    return timeout == 0 ? Await.result(future) : Await.result(future, duration);
  }

  public Future<LindenServiceInfo> getFutureServiceInfo() throws Exception {
    return get().getServiceInfo();
  }

  public void close() {
    if (zkClient != null) {
      zkClient.unsubscribeChildChanges(nodesPath, zkListener);
      LOGGER.info("LindenClient [{}] closed.", nodesPath);
    }
  }
}
