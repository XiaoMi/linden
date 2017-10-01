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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;
import com.github.zkclient.ZkClient;
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
  private static final Random random = new Random();
  private final String zk;
  private final String path;
  private final String localHostPort;
  private boolean haslocalClient = false;
  private final Integer shardId;
  private final LindenService.ServiceIface localClient;

  private volatile List<Map.Entry<String, LindenService.ServiceIface>> clients;

  public ShardClient(final ZkClient zkClient, final String zk, final String path,
                     final LindenService.ServiceIface localClient, final int localPort, final int shardId) {
    this.zk = zk;
    this.path = path;
    this.localClient = localClient;
    this.shardId = shardId;
    this.localHostPort = String.format("%s:%s", CommonUtils.getLocalHost(), localPort);

    // if the path does not exist, create it.
    // the path may be created later, so the listener will not work
    // and the isAvailable will always be false.
    if (!zkClient.exists(path)) {
      zkClient.createPersistent(path, true);
    }

    List<String> children = zkClient.getChildren(path);
    final Map<String, Map.Entry<String, LindenService.ServiceIface>> lindenClients = new ConcurrentHashMap<>();
    zkClient.subscribeChildChanges(path, new LindenZKListener(path, children) {
      @Override
      public void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted) {
        for (String node : newAdded) {
          String fullPath = FilenameUtils.separatorsToUnix(FilenameUtils.concat(path, node));
          byte[] bytes = zkClient.readData(fullPath);

          ServiceInstance instance = JSONObject.parseObject(new String(bytes), ServiceInstance.class);
          String hostPort = String.format("%s:%s", instance.getServiceEndpoint().getHost(),
                                          instance.getServiceEndpoint().getPort());
          if (localHostPort.equals(hostPort)) {
            haslocalClient = true;
            lindenClients.put(node, new AbstractMap.SimpleEntry<>(hostPort, localClient));
            LOGGER.info("Linden local node {} {} joined shard {}.", node, hostPort, shardId);
          } else {
            LindenService.ServiceIface client = Thrift.newIface(hostPort, LindenService.ServiceIface.class);
            lindenClients.put(node, new AbstractMap.SimpleEntry<>(hostPort, client));
            LOGGER.info("Linden node {} {} joined shard {}.", node, hostPort, shardId);
          }
        }
        for (String node : deleted) {
          if (lindenClients.containsKey(node)) {
            String hostPort = lindenClients.get(node).getKey();
            lindenClients.remove(node);
            LOGGER.info("Linden node {} {} left shard {}.", node, hostPort, shardId);
          }
        }

        // ensure the new node overrides the old node.
        List<String> sortedNodes = new ArrayList<>();
        for (String node : lindenClients.keySet()) {
          sortedNodes.add(node);
        }
        Collections.sort(sortedNodes, Collections.reverseOrder());

        Set<String> uniqueClients = new HashSet<>();
        for (String node : sortedNodes) {
          String hostPort = lindenClients.get(node).getKey();
          if (uniqueClients.contains(hostPort)) {
            lindenClients.remove(node);
            LOGGER.warn("Linden node {} {} is duplicated in shard {}, removed!", node, hostPort, shardId);
          } else {
            uniqueClients.add(hostPort);
          }
        }

        LOGGER.info("{} Linden node in shard {}.", lindenClients.size(), shardId);
        List<Map.Entry<String, LindenService.ServiceIface>> tempClients = new ArrayList<>();
        for (String node : lindenClients.keySet()) {
          tempClients.add(lindenClients.get(node));
          String hostPort = lindenClients.get(node).getKey();
          LOGGER.info("Linden node {} {} on service in shard {}.", node, hostPort, shardId);
        }
        clients = tempClients;
      }
    });
  }

  public boolean isAvailable() {
    return !clients.isEmpty();
  }

  /**
   * if there's the replica hash key, select replica by the key; or random one
   *
   * @param request
   * @return
   */
  private synchronized Map.Entry<String, LindenService.ServiceIface> getClient(LindenSearchRequest request) {
    if (request.isSetRouteParam() && request.getRouteParam().isSetReplicaRouteKey()) {
      int index = Math.abs(request.getRouteParam().getReplicaRouteKey().hashCode() % clients.size());
      return clients.get(index);
    }
    if (haslocalClient) {
      return new AbstractMap.SimpleEntry<>(localHostPort, localClient);
    }
    int index = random.nextInt(clients.size());
    return clients.get(index);
  }

  public Map.Entry<String, Future<LindenResult>> search(LindenSearchRequest request) {
    Map.Entry<String, LindenService.ServiceIface> client = getClient(request);
    return new AbstractMap.SimpleEntry<>(client.getKey(), client.getValue().search(request));
  }

  /**
   * delete some data from indexes
   *
   * @param request
   * @return
   */
  public List<Map.Entry<String, Future<Response>>> delete(LindenDeleteRequest request) {
    List<Map.Entry<String, Future<Response>>> hostFuturePairs = new ArrayList<>();
    for (Map.Entry<String, LindenService.ServiceIface> hostClientPair : clients) {
      Future<Response> future = hostClientPair.getValue().delete(request);
      hostFuturePairs.add(new AbstractMap.SimpleEntry<>(hostClientPair.getKey(), future));
    }
    return hostFuturePairs;
  }

  public List<Map.Entry<String, Future<Response>>> index(String content) {
    List<Map.Entry<String, Future<Response>>> hostFuturePairs = new ArrayList<>();
    for (Map.Entry<String, LindenService.ServiceIface> hostClientPair : clients) {
      Future<Response> future = hostClientPair.getValue().index(content);
      hostFuturePairs.add(new AbstractMap.SimpleEntry<>(hostClientPair.getKey(), future));
    }
    return hostFuturePairs;
  }

  public List<Map.Entry<String, Future<Response>>> executeCommand(String command) {
    List<Map.Entry<String, Future<Response>>> hostFuturePairs = new ArrayList<>();
    for (Map.Entry<String, LindenService.ServiceIface> hostClientPair : clients) {
      Future<Response> future = hostClientPair.getValue().executeCommand(command);
      hostFuturePairs.add(new AbstractMap.SimpleEntry<>(hostClientPair.getKey(), future));
    }
    return hostFuturePairs;
  }

  @Override
  public String toString() {
    return String.format("zk:%s, path:%s, clients num:%d", zk, path, clients.size());
  }

  public void close() {
  }

  public int getShardId() {
    return shardId;
  }
}
