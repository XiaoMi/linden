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
import org.apache.commons.lang.StringUtils;
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
  private int clusterAwaitTimeout;

  public CoreLindenCluster(LindenConfig lindenConf, ShardingStrategy shardingStrategy,
                           LindenService.ServiceIface localClient) {
    this.shardingStrategy = shardingStrategy;
    this.clusterAwaitTimeout = lindenConf.getClusterAwaitTimeout();
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

  @Override
  public Response delete(LindenDeleteRequest request) throws IOException {
    final List<Response> responseList = new ArrayList<>();
    List<Future<BoxedUnit>> futures = new ArrayList<>();
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
        List<Future<Response>> list = entry.getValue().delete(request);
        for (Future<Response> res : list) {
          futures.add(res.transformedBy(new FutureTransformer<Response, BoxedUnit>() {
            @Override
            public BoxedUnit map(Response response) {
              synchronized (responseList) {
                responseList.add(response);
              }
              return BoxedUnit.UNIT;
            }

            @Override
            public BoxedUnit handle(Throwable t) {
              LOGGER.error("Shard [{}] failed to get delete response : {}",
                           entry.getKey(), Throwables.getStackTraceAsString(t));
              return BoxedUnit.UNIT;
            }
          }));
        }
      }
    }
    Future<List<BoxedUnit>> collected = Future.collect(futures);
    try {
      if (clusterAwaitTimeout == 0) {
        Await.result(collected);
      } else {
        Await.result(collected, Duration.apply(clusterAwaitTimeout, TimeUnit.MILLISECONDS));
      }
      List<String> errors = new ArrayList<>();
      for (Response response : responseList) {
        if (response.isSetError()) {
          errors.add(response.getError());
        }
      }
      if (errors.size() > 1) {
        return ResponseUtils.buildFailedResponse(StringUtils.join(errors.toArray()));
      } else {
        return new Response();
      }
    } catch (Exception e) {
      LOGGER.error("Failed to get results from all nodes, exception: {}", Throwables.getStackTraceAsString(e));
      return ResponseUtils.buildFailedResponse(e);
    }
  }

  public LindenResult coreSearch(final LindenSearchRequest request) throws IOException {
    List<Future<BoxedUnit>> futures = new ArrayList<>();
    final List<LindenResult> resultList = new ArrayList<>();

    if (request.isSetRouteParam() && request.getRouteParam().isSetShardParams()) {
      for (final ShardRouteParam routeParam : request.getRouteParam().getShardParams()) {
        ShardClient client = clients.get(routeParam.getShardId());
        if (client != null) {
          LindenSearchRequest subRequest = request;
          if (routeParam.isSetEarlyParam()) {
            subRequest = new LindenSearchRequest(request);
            subRequest.setEarlyParam(routeParam.getEarlyParam());
          }
          futures.add(client.search(subRequest).transformedBy(new FutureTransformer<LindenResult, BoxedUnit>() {
            @Override
            public BoxedUnit map(LindenResult lindenResult) {
              synchronized (resultList) {
                resultList.add(lindenResult);
              }
              return BoxedUnit.UNIT;
            }

            @Override
            public BoxedUnit handle(Throwable t) {
              LOGGER.error("Shard [{}] failed to get search response : {}", routeParam.getShardId(),
                           Throwables.getStackTraceAsString(t));
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
          futures
              .add(entry.getValue().search(subRequest).transformedBy(new FutureTransformer<LindenResult, BoxedUnit>() {
                @Override
                public BoxedUnit map(LindenResult lindenResult) {
                  synchronized (resultList) {
                    resultList.add(lindenResult);
                  }
                  return BoxedUnit.UNIT;
                }

                @Override
                public BoxedUnit handle(Throwable t) {
                  LOGGER.error("Shard [{}] failed to get search response : {}",
                               entry.getKey(), Throwables.getStackTraceAsString(t));
                  return BoxedUnit.UNIT;
                }
              }));
        }
      }
    }

    Future<List<BoxedUnit>> collected = Future.collect(futures);
    try {
      if (clusterAwaitTimeout == 0) {
        Await.result(collected);
      } else {
        Await.result(collected, Duration.apply(clusterAwaitTimeout, TimeUnit.MILLISECONDS));
      }
    } catch (Exception e) {
      LOGGER.error("Failed to get results from all nodes, exception: {}", Throwables.getStackTraceAsString(e));
      return new LindenResult().setSuccess(false)
          .setError("Failed to get results from all nodes, " + Throwables.getStackTraceAsString(e));
    }
    return ResultMerger.merge(request, resultList);
  }

  @Override
  public Response index(String content) throws IOException {
    try {
      List<Future<BoxedUnit>> futures = new ArrayList<>();
      final StringBuilder errorInfo = new StringBuilder();
      LindenIndexRequest indexRequest = LindenIndexRequestParser.parse(lindenConfig.getSchema(), content);
      for (final Map.Entry<Integer, ShardClient> entry : clients.entrySet()) {
        ShardClient shardClient = entry.getValue();
        if (shardClient.isAvailable()) {
          if (shardingStrategy.accept(indexRequest.getId(), indexRequest.getRouteParam(), shardClient.getShardId())) {
            futures
                .add(shardClient.index(content).transformedBy(new FutureTransformer<List<Response>, BoxedUnit>() {
                  @Override
                  public BoxedUnit map(List<Response> responses) {
                    synchronized (errorInfo) {
                      for (int i = 0; i < responses.size(); ++i) {
                        if (!responses.get(i).isSuccess()) {
                          errorInfo.append(entry.getKey() + ":" + responses.get(i).getError() + ";");
                        }
                      }
                    }
                    return BoxedUnit.UNIT;
                  }

                  @Override
                  public BoxedUnit handle(Throwable t) {
                    LOGGER.error("Shard [{}] failed to get index response : {}",
                                 entry.getKey(), Throwables.getStackTraceAsString(t));
                    return BoxedUnit.UNIT;
                  }
                }));
          }
        }
      }

      Future<List<BoxedUnit>> collected = Future.collect(futures);
      if (clusterAwaitTimeout == 0) {
        Await.result(collected);
      } else {
        Await.result(collected, Duration.apply(clusterAwaitTimeout, TimeUnit.MILLISECONDS));
      }
      if (errorInfo.length() > 0) {
        return ResponseUtils.buildFailedResponse("Index failed: " + errorInfo.toString());
      }
      return ResponseUtils.SUCCESS;
    } catch (Exception e) {
      LOGGER.error("Handle request failed, content : {} - {}", content, Throwables.getStackTraceAsString(e));
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