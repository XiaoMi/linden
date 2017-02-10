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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.github.zkclient.ZkClient;
import com.google.common.collect.Iterators;
import com.twitter.finagle.Thrift;
import com.twitter.thrift.ServiceInstance;
import com.twitter.util.Future;

import com.xiaomi.linden.common.LindenZKListener;
import com.xiaomi.linden.common.util.CommonUtils;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.Response;
import com.xiaomi.linden.thrift.service.LindenService;

public class ShardClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShardClient.class);
  private final String zk;
  private final String path;
  private final String localHostPort;
  private boolean haslocalClient = false;
  private final Integer shardId;
  private final LindenService.ServiceIface localClient;

  private List<LindenService.ServiceIface> clients;
  private Iterator<LindenService.ServiceIface> clientIter;

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
          if (lindenClients.containsKey(node)) {
            String hostPort = lindenClients.get(node).getKey();
            if (uniqueClients.contains(hostPort)) {
              lindenClients.remove(node);
              LOGGER.warn("Linden node {} {} is duplicated in shard {}, removed!", node, hostPort, shardId);
            } else {
              uniqueClients.add(hostPort);
            }
          }
        }

        LOGGER.info("{} Linden node in shard {}.", lindenClients.size(), shardId);
        clients = new ArrayList<>();
        for (String node : lindenClients.keySet()) {
          clients.add(lindenClients.get(node).getValue());
          String hostPort = lindenClients.get(node).getKey();
          LOGGER.info("Linden node {} {} on service in shard {}.", node, hostPort, shardId);
        }
        clientIter = Iterators.cycle(clients);
      }
    });
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
      int index = Math.abs(request.getRouteParam().getReplicaRouteKey().hashCode() % clients.size());
      return clients.get(index);
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
    for (LindenService.ServiceIface client : clients) {
      Future<Response> response = client.delete(request);
      responses.add(response);
    }
    return responses;
  }

  public Future<List<Response>> index(String content) {
    List<Future<Response>> futures = new ArrayList<>();
    for (LindenService.ServiceIface client : clients) {
      futures.add(client.index(content));
    }
    return Future.collect(futures);
  }

  @Override
  public String toString() {
    return String.format("zk:%s, path:%s, clients num:%d", zk, path, clients.size());
  }

  public void close() {}

  public int getShardId() {
    return shardId;
  }
}
