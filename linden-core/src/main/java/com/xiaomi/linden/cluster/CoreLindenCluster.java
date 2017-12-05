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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.github.zkclient.ZkClient;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.FutureTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import com.xiaomi.linden.common.LindenZKListener;
import com.xiaomi.linden.common.ZKClientFactory;
import com.xiaomi.linden.common.util.LindenZKPathManager;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.indexing.LindenIndexRequestParser;
import com.xiaomi.linden.core.indexing.ShardingStrategy;
import com.xiaomi.linden.thrift.common.CacheInfo;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.Response;
import com.xiaomi.linden.thrift.common.ShardRouteParam;
import com.xiaomi.linden.thrift.service.LindenService;
import com.xiaomi.linden.util.ResponseUtils;

public class CoreLindenCluster extends LindenCluster {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoreLindenCluster.class);

  private final Map<Integer, ShardClient> clients = new ConcurrentHashMap<>();
  private final ZkClient zkClient;
  private final LindenConfig lindenConfig;
  private final LindenService.ServiceIface localClient;
  private final LindenZKPathManager zkPathManager;
  private final ShardingStrategy shardingStrategy;
  private LoadingCache<LindenSearchRequest, LindenResult> cache;
  private int clusterFutureAwaitTimeout;

  public CoreLindenCluster(LindenConfig lindenConf, ShardingStrategy shardingStrategy,
                           LindenService.ServiceIface localClient) {
    this.shardingStrategy = shardingStrategy;
    this.clusterFutureAwaitTimeout = lindenConf.getClusterFutureAwaitTimeout();
    this.localClient = localClient;
    this.lindenConfig = lindenConf;
    zkPathManager = new LindenZKPathManager(lindenConf.getClusterUrl());
    zkClient = ZKClientFactory.getClient(zkPathManager.getZK());

    //create path: /oakbay/v2/domain/cluster/nodes/shards/
    if (!zkClient.exists(zkPathManager.getClusterPath())) {
      zkClient.createPersistent(zkPathManager.getClusterPath(), true);
    }

    //create path: /oakbay/v2/domain/cluster/nodes/all/
    if (!zkClient.exists(zkPathManager.getAllNodesPath())) {
      zkClient.createPersistent(zkPathManager.getAllNodesPath(), true);
    }

    String path = zkPathManager.getClusterPath();
    List<String> children = zkClient.getChildren(path);
    zkClient.subscribeChildChanges(zkPathManager.getClusterPath(), new LindenZKListener(path, children) {
      @Override
      public void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted) {
        buildAllClient(parent, children, newAdded, deleted);
      }
    });

    if (lindenConfig.isEnableCache()) {
      cache = CacheBuilder.newBuilder().maximumSize(lindenConf.getCacheSize())
          .expireAfterAccess(lindenConf.getCacheDuration(), TimeUnit.SECONDS)
          .recordStats()
          .build(new CacheLoader<LindenSearchRequest, LindenResult>() {
            @Override
            public LindenResult load(LindenSearchRequest lindenRequest) throws Exception {
              return coreSearch(lindenRequest);
            }
          });
    }
  }

  private void buildAllClient(String parent, List<String> children, List<String> newAdded, List<String> deleted) {
    for (String shardId : newAdded) {
      String shardPath = zkPathManager.getShardPath(shardId);
      ShardClient client = new ShardClient(zkClient, zkPathManager.getZK(), shardPath, localClient,
                                           lindenConfig.getPort(), Integer.valueOf(shardId));
      clients.put(Integer.valueOf(shardId), client);
      LOGGER.info("Shard [{}] joined cluster.", shardId);
    }
    for (String shardId : deleted) {
      clients.remove(Integer.valueOf(shardId));
      LOGGER.info("Shard [{}] left cluster.", shardId);
    }
    LOGGER.info("Cluster client built, clients size: {}.", clients.size());
  }

  @Override
  public LindenResult search(final LindenSearchRequest request) throws IOException {
    if (cache != null) {
      try {
        LindenResult result = cache.get(request);
        if (!result.isSuccess()) {
          cache.invalidate(request);
        }
        return result;
      } catch (ExecutionException e) {
        throw new IOException(Throwables.getStackTraceAsString(e));
      }
    }
    return coreSearch(request);
  }

  static private String getHostFutureInfo(List<String> hosts, List<Future<BoxedUnit>> futures) {
    StringBuilder sb = new StringBuilder();
    sb.append("Future info:\n");
    for (int i = 0; i < futures.size(); ++i) {
      sb.append(hosts.get(i));
      sb.append(":");
      sb.append(futures.get(i));
      sb.append("\n");
    }
    return sb.toString();
  }

  @Override
  public Response delete(LindenDeleteRequest request) throws IOException {
    List<Future<BoxedUnit>> futures = new ArrayList<>();
    List<String> hosts = new ArrayList<>();
    final StringBuilder errorInfo = new StringBuilder();
    Set<Integer> routeIds = null;
    if (request.isSetRouteParam()) {
      routeIds = new HashSet<>();
      for (final ShardRouteParam routeParam : request.getRouteParam().getShardParams()) {
        routeIds.add(routeParam.getShardId());
      }
    }
    for (final Map.Entry<Integer, ShardClient> entry : clients.entrySet()) {
      if (routeIds != null && !routeIds.contains(entry.getKey())) {
        continue;
      }
      if (entry.getValue().isAvailable()) {
        List<Map.Entry<String, Future<Response>>> hostFuturePairs = entry.getValue().delete(request);
        for (final Map.Entry<String, Future<Response>> hostFuturePair : hostFuturePairs) {
          hosts.add(hostFuturePair.getKey());
          futures.add(hostFuturePair.getValue().transformedBy(new FutureTransformer<Response, BoxedUnit>() {
            @Override
            public BoxedUnit map(Response response) {
              if (!response.isSuccess()) {
                synchronized (errorInfo) {
                  LOGGER.error("Shard [{}] host [{}] failed to get delete response : {}",
                               entry.getKey(), hostFuturePair.getKey(), response.getError());
                  errorInfo
                      .append("Shard " + entry.getKey() + " host " + hostFuturePair.getKey() + ":" + response.getError()
                              + ";");

                }
              }
              return BoxedUnit.UNIT;
            }

            @Override
            public BoxedUnit handle(Throwable t) {
              LOGGER.error("Shard [{}] host [{}] failed to get delete response : {}",
                           entry.getKey(), hostFuturePair.getKey(), Throwables.getStackTraceAsString(t));
              errorInfo
                  .append("Shard " + entry.getKey() + " host " + hostFuturePair.getKey() + ":" + Throwables
                      .getStackTraceAsString(t) + ";");
              return BoxedUnit.UNIT;
            }
          }));
        }
      }
    }

    Future<List<BoxedUnit>> collected = Future.collect(futures);
    try {
      if (clusterFutureAwaitTimeout == 0) {
        Await.result(collected);
      } else {
        Await.result(collected, Duration.apply(clusterFutureAwaitTimeout, TimeUnit.MILLISECONDS));
      }
      if (errorInfo.length() > 0) {
        return ResponseUtils.buildFailedResponse("Delete failed: " + errorInfo.toString());
      }
      return ResponseUtils.SUCCESS;
    } catch (Exception e) {
      LOGGER.error("Failed to get all delete responses, exception: {}", Throwables.getStackTraceAsString(e));
      LOGGER.error(getHostFutureInfo(hosts, futures));
      return ResponseUtils.buildFailedResponse(e);
    }
  }

  public LindenResult coreSearch(final LindenSearchRequest request) throws IOException {
    List<Future<BoxedUnit>> futures = new ArrayList<>();
    List<String> hosts = new ArrayList<>();
    final List<LindenResult> resultList = new ArrayList<>();
    if (request.isSetRouteParam() && request.getRouteParam().isSetShardParams()) {
      for (final ShardRouteParam routeParam : request.getRouteParam().getShardParams()) {
        ShardClient client = clients.get(routeParam.getShardId());
        if (client != null && client.isAvailable()) {
          LindenSearchRequest subRequest = request;
          if (routeParam.isSetEarlyParam()) {
            subRequest = new LindenSearchRequest(request);
            subRequest.setEarlyParam(routeParam.getEarlyParam());
          }
          final Map.Entry<String, Future<LindenResult>> hostFuturePair = client.search(subRequest);
          hosts.add(hostFuturePair.getKey());
          futures.add(hostFuturePair.getValue().transformedBy(new FutureTransformer<LindenResult, BoxedUnit>() {
            @Override
            public BoxedUnit map(LindenResult lindenResult) {
              synchronized (resultList) {
                resultList.add(lindenResult);
                if (!lindenResult.isSuccess()) {
                  LOGGER.error("Shard [{}] host [{}] failed to get search result : {}",
                               routeParam.getShardId(), hostFuturePair.getKey(), lindenResult.getError());
                }
              }
              return BoxedUnit.UNIT;
            }

            @Override
            public BoxedUnit handle(Throwable t) {
              LOGGER.error("Shard [{}] host [{}] failed to get search result : {}",
                           routeParam.getShardId(), hostFuturePair.getKey(), Throwables.getStackTraceAsString(t));
              return BoxedUnit.UNIT;
            }
          }));
        } else {
          LOGGER.warn("Route to Shard [{}] failed.", routeParam.getShardId());
        }
      }
    } else {
      LindenSearchRequest subRequest = request;
      for (final Map.Entry<Integer, ShardClient> entry : clients.entrySet()) {
        if (entry.getValue().isAvailable()) {
          final Map.Entry<String, Future<LindenResult>> hostFuturePair = entry.getValue().search(subRequest);
          hosts.add(hostFuturePair.getKey());
          futures.add(hostFuturePair.getValue().transformedBy(new FutureTransformer<LindenResult, BoxedUnit>() {
            @Override
            public BoxedUnit map(LindenResult lindenResult) {
              synchronized (resultList) {
                resultList.add(lindenResult);
                if (!lindenResult.isSuccess()) {
                  LOGGER.error("Shard [{}] host [{}] failed to get search result : {}",
                               entry.getKey(), hostFuturePair.getKey(), lindenResult.getError());
                }
                return BoxedUnit.UNIT;
              }
            }

            @Override
            public BoxedUnit handle(Throwable t) {
              LOGGER.error("Shard [{}] host [{}] failed to get search result : {}",
                           entry.getKey(), hostFuturePair.getKey(), Throwables.getStackTraceAsString(t));
              return BoxedUnit.UNIT;
            }
          }));
        }
      }
    }

    Future<List<BoxedUnit>> collected = Future.collect(futures);
    try {
      if (clusterFutureAwaitTimeout == 0) {
        Await.result(collected);
      } else {
        Await.result(collected, Duration.apply(clusterFutureAwaitTimeout, TimeUnit.MILLISECONDS));
      }
    } catch (Exception e) {
      LOGGER.error("Failed to get all results, exception: {}", Throwables.getStackTraceAsString(e));
      LOGGER.error(getHostFutureInfo(hosts, futures));
      if (resultList.size() == 0) {
        return new LindenResult().setSuccess(false)
            .setError("Failed to get any shard result, " + Throwables.getStackTraceAsString(e));
      }
    }
    return ResultMerger.merge(request, resultList);
  }

  @Override
  public Response index(String content) throws IOException {
    List<Future<BoxedUnit>> futures = new ArrayList<>();
    List<String> hosts = new ArrayList<>();
    final StringBuilder errorInfo = new StringBuilder();
    LindenIndexRequest indexRequest = LindenIndexRequestParser.parse(lindenConfig.getSchema(), content);
    for (final Map.Entry<Integer, ShardClient> entry : clients.entrySet()) {
      ShardClient shardClient = entry.getValue();
      if (shardClient.isAvailable()) {
        if (shardingStrategy
            .accept(indexRequest.getId(), indexRequest.getRouteParam(), shardClient.getShardId())) {
          final List<Map.Entry<String, Future<Response>>> hostFuturePairs = shardClient.index(content);
          for (final Map.Entry<String, Future<Response>> hostFuturePair : hostFuturePairs) {
            hosts.add(hostFuturePair.getKey());
            futures.add(hostFuturePair.getValue().transformedBy(new FutureTransformer<Response, BoxedUnit>() {
              @Override
              public BoxedUnit map(Response response) {
                if (!response.isSuccess()) {
                  LOGGER.error("Shard [{}] host [{}] failed to get index response : {}",
                               entry.getKey(), hostFuturePair.getKey(), response.getError());
                  synchronized (errorInfo) {
                    errorInfo.append(
                        "Shard " + entry.getKey() + " host " + hostFuturePair.getKey() + ":" + response.getError()
                        + ";");
                  }
                }
                return BoxedUnit.UNIT;
              }

              @Override
              public BoxedUnit handle(Throwable t) {
                LOGGER.error("Shard [{}] host [{}] failed to get index response : {}",
                             entry.getKey(), hostFuturePair.getKey(), Throwables.getStackTraceAsString(t));
                synchronized (errorInfo) {
                  errorInfo.append("Shard " + entry.getKey() + " host " + hostFuturePair.getKey() + ":" + Throwables
                      .getStackTraceAsString(t) + ";");
                }
                return BoxedUnit.UNIT;
              }
            }));
          }
        }
      }
    }
    try {
      Future<List<BoxedUnit>> collected = Future.collect(futures);
      if (clusterFutureAwaitTimeout == 0) {
        Await.result(collected);
      } else {
        Await.result(collected, Duration.apply(clusterFutureAwaitTimeout, TimeUnit.MILLISECONDS));
      }
      if (errorInfo.length() > 0) {
        return ResponseUtils.buildFailedResponse("Index failed: " + errorInfo.toString());
      }
      return ResponseUtils.SUCCESS;
    } catch (Exception e) {
      LOGGER.error("Handle request failed, content : {} - {}", content, Throwables.getStackTraceAsString(e));
      LOGGER.error(getHostFutureInfo(hosts, futures));
      return ResponseUtils.buildFailedResponse(e);
    }
  }

  @Override
  public Response executeCommand(final String command) throws IOException {
    LOGGER.info("Receive cluster command {}", command);
    List<Future<BoxedUnit>> futures = new ArrayList<>();
    List<String> hosts = new ArrayList<>();
    final StringBuilder errorInfo = new StringBuilder();
    for (final Map.Entry<Integer, ShardClient> entry : clients.entrySet()) {
      ShardClient shardClient = entry.getValue();
      if (shardClient.isAvailable()) {
        final List<Map.Entry<String, Future<Response>>> hostFuturePairs = shardClient.executeCommand(command);
        for (final Map.Entry<String, Future<Response>> hostFuturePair : hostFuturePairs) {
          hosts.add(hostFuturePair.getKey());
          futures.add(hostFuturePair.getValue().transformedBy(new FutureTransformer<Response, BoxedUnit>() {
            @Override
            public BoxedUnit map(Response response) {
              if (!response.isSuccess()) {
                LOGGER.error("Shard [{}] host [{}] failed to execute command {} error: {}",
                             entry.getKey(), hostFuturePair.getKey(), command, response.getError());
                synchronized (errorInfo) {
                  errorInfo.append(
                      "Shard " + entry.getKey() + " host " + hostFuturePair.getKey() + ":" + response.getError() + ";");
                }
              }
              return BoxedUnit.UNIT;
            }

            @Override
            public BoxedUnit handle(Throwable t) {
              LOGGER.error("Shard [{}] host [{}] failed to execute command {} throwable: {}\"",
                           entry.getKey(), hostFuturePair.getKey(), command, Throwables.getStackTraceAsString(t));
              synchronized (errorInfo) {
                errorInfo.append("Shard " + entry.getKey() + " host " + hostFuturePair.getKey() + ":" + Throwables
                    .getStackTraceAsString(t) + ";");
              }
              return BoxedUnit.UNIT;
            }
          }));
        }
      }
    }
    try {
      Future<List<BoxedUnit>> collected = Future.collect(futures);
      if (clusterFutureAwaitTimeout == 0) {
        Await.result(collected);
      } else {
        // executeCommand may take a very long time, so set timeout to 30min specially
        Await.result(collected, Duration.apply(30, TimeUnit.MINUTES));
      }
      if (errorInfo.length() > 0) {
        return ResponseUtils.buildFailedResponse("command " + command + " failed: " + errorInfo.toString());
      }
      LOGGER.error("Cluster command {} succeeded", command);
      return ResponseUtils.SUCCESS;
    } catch (Exception e) {
      LOGGER.error("Cluster command {} failed {}", command, Throwables.getStackTraceAsString(e));
      LOGGER.error(getHostFutureInfo(hosts, futures));
      return ResponseUtils.buildFailedResponse(e);
    }
  }

  @Override
  public CacheInfo getCacheInfo() throws IOException {
    if (cache == null) {
      return null;
    }
    CacheInfo cacheInfo = new CacheInfo()
        .setAverageLoadPenalty(cache.stats().averageLoadPenalty())
        .setHitCount(cache.stats().hitCount())
        .setHitRate(cache.stats().hitRate())
        .setMissCount(cache.stats().missCount())
        .setMissRate(cache.stats().missRate())
        .setLoadExceptionCount(cache.stats().loadExceptionCount())
        .setLoadSuccessCount(cache.stats().loadSuccessCount())
        .setTotalLoadTime(cache.stats().totalLoadTime());
    return cacheInfo;
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<Integer, ShardClient> entry : clients.entrySet()) {
      entry.getValue().close();
    }
    zkClient.close();
  }
}