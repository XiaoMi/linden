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

package com.xiaomi.linden.cluster;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;
import com.github.zkclient.ZkClient;
import com.google.common.collect.Iterators;
import com.twitter.finagle.Thrift;
import com.twitter.thrift.ServiceInstance;
import com.twitter.util.Future;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.common.LindenZKListener;
import com.xiaomi.linden.common.util.CommonUtils;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.Response;
import com.xiaomi.linden.thrift.service.LindenService;

public class ShardClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShardClient.class);
  private final Map<String, LindenService.ServiceIface> clients = new ConcurrentHashMap<>();
  private final String zk;
  private final String path;
  private final ZkClient zkClient;
  private final String host;
  private final int localPort;
  private boolean haslocalClient = false;
  private final Integer remoteShardId;
  private final LindenService.ServiceIface localClient;
  private Iterator<LindenService.ServiceIface> clientIter;
  private LindenService.ServiceIface[] clientArray;

  public ShardClient(final ZkClient zkClient, final String zk, final String path,
                     LindenService.ServiceIface localClient, int localPort, int remoteShardId) {
    this.zk = zk;
    this.path = path;
    this.zkClient = zkClient;
    this.localClient = localClient;
    this.localPort = localPort;
    this.remoteShardId = remoteShardId;

    // if the path does not exist, create it.
    // the path may be created later, so the listener will not work
    // and the isAvailable will always be false.
    if (!zkClient.exists(path)) {
      zkClient.createPersistent(path, true);
    }
    host = CommonUtils.getLocalHost();
    List<String> children = zkClient.getChildren(path);
    zkClient.subscribeChildChanges(path, new LindenZKListener(path, children) {
      @Override
      public void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted) {
        buildClient(newAdded, deleted);
      }
    });
  }

  private synchronized void buildClient(List<String> newAdded, List<String> deleted) {
    for (String node : newAdded) {
      String fullPath = FilenameUtils.separatorsToUnix(FilenameUtils.concat(path, node));
      byte[] bytes = zkClient.readData(fullPath);
      // do not use serviceInstance.getShardId()
      ServiceInstance serviceInstance = JSONObject.parseObject(new String(bytes), ServiceInstance.class);
      if (serviceInstance.getServiceEndpoint().getHost().equals(host)
          && serviceInstance.getServiceEndpoint().getPort() == localPort) {
        clients.put(node, localClient);
        haslocalClient = true;
        LOGGER.info("Linden local node [{}:{}] joined cluster.", host, localPort);
      } else {
        String schema = String.format("%s:%s",
                                      serviceInstance.getServiceEndpoint().getHost(),
                                      serviceInstance.getServiceEndpoint().getPort());
        LindenService.ServiceIface client = Thrift.newIface(schema, LindenService.ServiceIface.class);
        clients.put(node, client);
        LOGGER.info("Linden path [{}] node [{}] joined cluster.", path, node);
      }
    }

    for (String node : deleted) {
      clients.remove(node);
      LOGGER.info("Linden path [{}] node [{}] left cluster.", path, node);
    }
    clientIter = Iterators.cycle(clients.values());
    clientArray = clients.values().toArray(new LindenService.ServiceIface[clients.size()]);
    LOGGER.info("Path: [{}] clients num : [{}].", path, clients.size());
  }

  public boolean isAvailable() {
    return !clients.isEmpty();
  }

  /**
   * if there's the replica hash key, select replica by the key; or round-robin
   * @param request
   * @return
   */
  private synchronized LindenService.ServiceIface getClient(LindenSearchRequest request) {
    if (request.isSetRouteParam() && request.getRouteParam().isSetReplicaRouteKey()) {
      int index = Math.abs(request.getRouteParam().getReplicaRouteKey().hashCode() % clientArray.length);
      return clientArray[index];
    }
    if (haslocalClient) {
      return localClient;
    }
    return clientIter.next();
  }

  public Future<LindenResult> search(LindenSearchRequest request) {
    LindenService.ServiceIface client = getClient(request);
    if (client != null) {
      return client.search(request);
    } else {
      return Future.value(new LindenResult());
    }
  }

  /**
   * delete some data from indexes
   *
   * @param request
   * @return
   */
  public List<Future<Response>> delete(LindenDeleteRequest request) {
    List<Future<Response>> responses = new ArrayList<>();
    for (LindenService.ServiceIface client : clients.values()) {
      Future<Response> response = client.delete(request);
      responses.add(response);
    }
    return responses;
  }

  public Future<List<Response>> index(String content) {
    List<Future<Response>> futures = new ArrayList<>();
    for (final Map.Entry<String, LindenService.ServiceIface> entry : clients.entrySet()) {
      futures.add(entry.getValue().index(content));
    }
    return Future.collect(futures);
  }

  @Override
  public String toString() {
    return String.format("zk:%s, path:%s, clients num:%d", zk, path, clients.size());
  }

  public void close() {
  }

  public int getShardId() {
    return remoteShardId;
  }
}
