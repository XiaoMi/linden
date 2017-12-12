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

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.cluster.CoreLindenCluster;
import com.xiaomi.linden.cluster.LindenCluster;
import com.xiaomi.linden.common.util.LindenZKPathManager;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.LindenConfigBuilder;
import com.xiaomi.linden.core.indexing.DefaultShardingStrategy;
import com.xiaomi.linden.core.indexing.IndexingManager;
import com.xiaomi.linden.core.indexing.IndexingMangerUtil;
import com.xiaomi.linden.core.indexing.LindenIndexRequestParser;
import com.xiaomi.linden.core.indexing.ShardingStrategy;
import com.xiaomi.linden.core.search.HotSwapLindenCoreImpl;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.core.search.LindenCoreImpl;
import com.xiaomi.linden.core.search.MultiLindenCoreImpl;
import com.xiaomi.linden.plugin.metrics.MetricsManager;
import com.xiaomi.linden.plugin.warmer.LindenWarmer;
import com.xiaomi.linden.thrift.common.CacheInfo;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;
import com.xiaomi.linden.thrift.common.LindenRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenServiceInfo;
import com.xiaomi.linden.thrift.common.Response;
import com.xiaomi.linden.thrift.service.LindenService;
import com.xiaomi.linden.util.ResponseUtils;

public class CoreLindenServiceImpl implements LindenService.ServiceIface {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoreLindenServiceImpl.class);

  private final LindenCluster lindenCluster;
  private final BQLCompiler bqlCompiler;
  private final LindenConfig config;
  private final LindenCore lindenCore;
  private final MetricsManager metricsManager;
  private final Thread checkupThread;
  //process request for cluster searching interface
  private final ExecutorServiceFuturePool clusterExecutorPool;
  //process request for local instance searching interface
  private final ExecutorServiceFuturePool instanceExecutorPool;

  private IndexingManager indexingManager;
  private ShardingStrategy shardingStrategy;
  private LindenWarmer lindenWarmer;
  private int instanceFuturePoolWaitTimeout;
  private int clusterFuturePoolWaitTimeout;

  public CoreLindenServiceImpl(LindenConfig config) throws Exception {
    Preconditions.checkArgument(config != null, "LindenConfig can not be null.");
    this.config = config;

    MetricsManager
        tmpMetricsManager =
        config.getPluginManager().getInstance(LindenConfigBuilder.LINDEN_METRIC_FACTORY, MetricsManager.class);
    metricsManager = tmpMetricsManager == null ? MetricsManager.DEFAULT : tmpMetricsManager;
    LOGGER.info("Metrics manager : {}", metricsManager);

    LindenWarmer
        tmpLindenWarmer =
        config.getPluginManager().getInstance(LindenConfigBuilder.LINDEN_WARMER_FACTORY, LindenWarmer.class);
    lindenWarmer = tmpLindenWarmer == null ? LindenWarmer.DEFAULT : tmpLindenWarmer;
    LOGGER.info("Linden warmer : {}", lindenWarmer);

    LindenZKPathManager zkPathManager = new LindenZKPathManager(config.getClusterUrl());
    this.shardingStrategy =
        new DefaultShardingStrategy(zkPathManager.getZK(), zkPathManager.getClusterPath(), config.getShardId());

    JSONObject clusterThreadPoolExecutorConf = null;
    JSONObject instanceThreadPoolExecutorConf = null;
    if (config.getSearchThreadPoolConfig() != null) {
      try {
        JSONObject jsonConfig = JSON.parseObject(config.getSearchThreadPoolConfig());
        clusterThreadPoolExecutorConf = jsonConfig.getJSONObject("cluster");
        instanceThreadPoolExecutorConf = jsonConfig.getJSONObject("instance");
      } catch (Exception e) {
        LOGGER.error("Exception in thread pool config parsing: {}", Throwables.getStackTraceAsString(e));
      }
    }

    final ThreadPoolExecutor clusterThreadPoolExecutor = buildThreadPoolExecutor(clusterThreadPoolExecutorConf);
    final ThreadPoolExecutor instanceThreadPoolExecutor = buildThreadPoolExecutor(instanceThreadPoolExecutorConf);

    LOGGER.info(
        "clusterThreadPoolExecutor CorePoolSize {}, MaximumPoolSize {}, QueueCapacity {}, RejectedExecutionHandler {}",
        clusterThreadPoolExecutor.getCorePoolSize(), clusterThreadPoolExecutor.getMaximumPoolSize(),
        clusterThreadPoolExecutor.getQueue().remainingCapacity(),
        clusterThreadPoolExecutor.getRejectedExecutionHandler().getClass().getName());
    LOGGER.info(
        "instanceThreadPoolExecutor CorePoolSize {}, MaximumPoolSize {}, QueueCapacity {}, RejectedExecutionHandler {}",
        instanceThreadPoolExecutor.getCorePoolSize(), instanceThreadPoolExecutor.getMaximumPoolSize(),
        instanceThreadPoolExecutor.getQueue().remainingCapacity(),
        instanceThreadPoolExecutor.getRejectedExecutionHandler().getClass().getName());

    clusterExecutorPool = new ExecutorServiceFuturePool(clusterThreadPoolExecutor);
    instanceExecutorPool = new ExecutorServiceFuturePool(instanceThreadPoolExecutor);
    clusterFuturePoolWaitTimeout = config.getClusterFuturePoolWaitTimeout();
    instanceFuturePoolWaitTimeout = config.getInstanceFuturePoolWaitTimeout();

    if (config.getLindenCoreMode() == LindenConfig.LindenCoreMode.MULTI) {
      lindenCore = new MultiLindenCoreImpl(config);
    } else if (config.getLindenCoreMode() == LindenConfig.LindenCoreMode.HOTSWAP) {
      lindenCore = new HotSwapLindenCoreImpl(config);
    } else {
      lindenCore = new LindenCoreImpl(config);
    }

    indexingManager = IndexingMangerUtil.initIndexingManger(config, shardingStrategy, lindenCore);

    bqlCompiler = new BQLCompiler(config.getSchema());

    try {
      lindenCluster = new CoreLindenCluster(config, shardingStrategy, this);
    } catch (Exception e) {
      lindenCore.close();
      LOGGER.error("Init linden cluster failed : {}", Throwables.getStackTraceAsString(e));
      throw e;
    }

    checkupThread = new Thread() {
      @Override
      public void run() {
        while (true) {
          try {
            Thread.sleep(30000);
            LOGGER.info("Cluster thread pool executor status:" + clusterThreadPoolExecutor);
            LOGGER.info("Instance thread pool executor status:" + instanceThreadPoolExecutor);
            LOGGER.info("JVM memory status: maxMemory {}MB totalMemory {}MB, freeMemory {}MB",
                        Runtime.getRuntime().maxMemory() / 1024 / 1024,
                        Runtime.getRuntime().totalMemory() / 1024 / 1024,
                        Runtime.getRuntime().freeMemory() / 1024 / 1024);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    };
    checkupThread.start();
    lindenWarmer.warmUp(this);
  }

  private static ThreadPoolExecutor buildThreadPoolExecutor(JSONObject threadConfig) {
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    if (threadConfig == null) {
      int min = 2 * availableProcessors;
      int max = 3 * availableProcessors;
      return new ThreadPoolExecutor(min, max, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(500),
                                    new ThreadPoolExecutor.DiscardOldestPolicy());
    }
    int min = threadConfig.containsKey("min") ? threadConfig.getInteger("min") : 2 * availableProcessors + 1;
    int max = threadConfig.containsKey("max") ? threadConfig.getInteger("max") : 3 * availableProcessors + 1;
    min = Math.max(min, 1);
    max = Math.min(max, 6 * availableProcessors);
    min = Math.min(min, max);
    int queueSize = threadConfig.containsKey("queueSize") ? threadConfig.getInteger("queueSize") : 1000;
    LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
    RejectedExecutionHandler handler;
    String policy = threadConfig.containsKey("policy") ? threadConfig.getString("policy") : "discardoldest";
    switch (policy.toLowerCase()) {
      case "abort":
        handler = new ThreadPoolExecutor.AbortPolicy();
        break;
      case "discard":
        handler = new ThreadPoolExecutor.DiscardPolicy();
        break;
      case "discardoldest":
        handler = new ThreadPoolExecutor.DiscardOldestPolicy();
        break;
      default:
        LOGGER.error("Unrecognized policy: " + policy + ", set to DiscardOldestPolicy by default");
        handler = new ThreadPoolExecutor.DiscardOldestPolicy();
        break;
    }
    return new ThreadPoolExecutor(min, max, 1, TimeUnit.MINUTES, queue, handler);
  }

  @Override
  public Future<LindenResult> search(final LindenSearchRequest request) {
    final Stopwatch sw = Stopwatch.createStarted();
    return instanceExecutorPool.apply(new Function0<LindenResult>() {
      @Override
      public LindenResult apply() {
        LindenResult result = null;
        try {
          long eps = sw.elapsed(TimeUnit.MILLISECONDS);
          if (eps > 10) {
            LOGGER.warn("Warning: instanceExecutorPool took " + eps + "ms to start search.");
            if (eps > instanceFuturePoolWaitTimeout) {
              result = buildLindenFailedResult("Waiting time is too long, " + eps + "ms in instance future pool");
              return result;
            }
          }
          result = lindenCore.search(request);
          return result;
        } catch (Exception e) {
          String errorStackInfo = Throwables.getStackTraceAsString(e);
          result = buildLindenFailedResult(errorStackInfo);
        } finally {
          result.setCost((int) sw.elapsed(TimeUnit.MILLISECONDS));
          if (result.isSuccess()) {
            LOGGER.info("Instance search request succeeded, request: {}, hits: {}, cost: {} ms.", request,
                        result.getHitsSize(), result.getCost());
          } else {
            LOGGER.error("Instance search request failed, request: {}, error: {}, cost: {} ms.", request,
                         result.getError(), result.getCost());
          }
          return result;
        }
      }
    });
  }

  @Override
  public Future<Response> delete(final LindenDeleteRequest request) {
    final Stopwatch sw = Stopwatch.createStarted();
    return instanceExecutorPool.apply(new Function0<Response>() {
      @Override
      public Response apply() {
        Response response = null;
        try {
          long eps = sw.elapsed(TimeUnit.MILLISECONDS);
          if (eps > 10) {
            LOGGER.warn("Warning: instanceExecutorPool took " + eps + "ms to start delete.");
            if (eps > instanceFuturePoolWaitTimeout) {
              response =
                  ResponseUtils.buildFailedResponse("Waiting time is too long, " + eps + "ms in instance future pool");
              return response;
            }
          }
          response = lindenCore.delete(request);
        } catch (Exception e) {
          String errorStackInfo = Throwables.getStackTraceAsString(e);
          response = ResponseUtils.buildFailedResponse(errorStackInfo);
        } finally {
          if (response.isSuccess()) {
            LOGGER.info("Instance delete request succeeded, request: {}, cost: {} ms.", request,
                        sw.elapsed(TimeUnit.MILLISECONDS));
          } else {
            LOGGER.error("Instance delete request failed, request: {}, cost: {} ms.", request,
                         sw.elapsed(TimeUnit.MILLISECONDS));
          }
          return response;
        }
      }
    });
  }

  @Override
  public Future<Response> index(final String content) {
    final Stopwatch sw = Stopwatch.createStarted();
    return instanceExecutorPool.apply(new Function0<Response>() {
      @Override
      public Response apply() {
        Response response = null;
        try {
          long eps = sw.elapsed(TimeUnit.MILLISECONDS);
          if (eps > 10) {
            LOGGER.warn("Warning: instanceExecutorPool took " + eps + "ms to start index.");
            if (eps > instanceFuturePoolWaitTimeout) {
              response =
                  ResponseUtils.buildFailedResponse("Waiting time is too long, " + eps + "ms in instance future pool");
              return response;
            }
          }
          LindenIndexRequest indexRequest =
              LindenIndexRequestParser.parse(config.getSchema(), content);
          response = lindenCore.index(indexRequest);
        } catch (Exception e) {
          String errorStackInfo = Throwables.getStackTraceAsString(e);
          response = ResponseUtils.buildFailedResponse(errorStackInfo);
        } finally {
          if (response.isSuccess()) {
            LOGGER.info("Instance index request succeeded, content: {}, cost: {} ms.", content,
                        sw.elapsed(TimeUnit.MILLISECONDS));
          } else {
            LOGGER.error("Instance index request failed, content: {}, cost: {} ms.", content,
                         sw.elapsed(TimeUnit.MILLISECONDS));
          }
          return response;
        }
      }
    });
  }

  // handle single instance request called warmer
  @Override
  public Future<LindenResult> handleBqlRequest(final String bql) {
    final Stopwatch sw = Stopwatch.createStarted();
    return instanceExecutorPool.apply(new Function0<LindenResult>() {
      @Override
      public LindenResult apply() {
        LindenResult result = null;
        String logTag = null;
        try {
          long eps = sw.elapsed(TimeUnit.MILLISECONDS);
          if (eps > 10) {
            LOGGER.warn("Warning: instanceExecutorPool took " + eps + "ms to start handleBqlRequest.");
            if (eps > instanceFuturePoolWaitTimeout) {
              result = buildLindenFailedResult("Waiting time is too long, " + eps + "ms in instance future pool");
              return result;
            }
          }
          LindenRequest lindenRequest = bqlCompiler.compile(bql);
          if (lindenRequest.isSetSearchRequest()) {
            result = lindenCore.search(lindenRequest.getSearchRequest());
            if (result.isSuccess()) {
              logTag = "singleInstanceSearch";
            } else {
              logTag = "failureSingleInstanceSearch";
            }
          } else if (lindenRequest.isSetDeleteRequest()) {
            Response response = lindenCore.delete(lindenRequest.getDeleteRequest());
            result = new LindenResult().setSuccess(response.isSuccess()).setError(response.getError());
            if (result.isSuccess()) {
              logTag = "singleInstanceDelete";
            } else {
              logTag = "failureSingleInstanceDelete";
            }
          } else {
            result = buildLindenFailedResult("unsupported Bql");
            logTag = "unsupportedSingleInstanceBql";
          }
        } catch (Exception e) {
          String errorStackInfo = Throwables.getStackTraceAsString(e);
          result = buildLindenFailedResult(errorStackInfo);
          logTag = "exceptionalSingleInstanceRequest";

        } finally {
          metricsManager.time(sw.elapsed(TimeUnit.NANOSECONDS), logTag);
          result.setCost((int) sw.elapsed(TimeUnit.MILLISECONDS));
          if (result.isSuccess()) {
            LOGGER
                .info("Single instance request succeeded bql: {}, hits: {}, cost: {} ms.", bql, result.getHitsSize(),
                      result.getCost());
          } else {
            LOGGER.error("Single instance request failed bql: {}, error: {}, cost: {} ms.", bql, result.getError(),
                         result.getCost());
          }
          return result;
        }
      }
    });
  }

  @Override
  public Future<LindenServiceInfo> getServiceInfo() {
    return instanceExecutorPool.apply(new Function0<LindenServiceInfo>() {
      @Override
      public LindenServiceInfo apply() {
        LindenServiceInfo serviceInfo;
        try {
          serviceInfo = lindenCore.getServiceInfo();
          CacheInfo cacheInfo = lindenCluster.getCacheInfo();
          serviceInfo.setCacheInfo(cacheInfo);
        } catch (Exception e) {
          serviceInfo = new LindenServiceInfo();
          LOGGER.error("get service info failed : {}", Throwables.getStackTraceAsString(e));
        }
        return serviceInfo;
      }
    });
  }

  @Override
  public Future<Response> handleClusterIndexRequest(String content) {
    final Stopwatch sw = Stopwatch.createStarted();
    Future<Response> responseFuture;
    try {
      responseFuture = Future.value(lindenCluster.index(content));
      metricsManager.time(sw.elapsed(TimeUnit.NANOSECONDS), "index");
      return responseFuture;
    } catch (Exception e) {
      String errorStackInfo = Throwables.getStackTraceAsString(e);
      LOGGER.error("Handle json cluster failed, content : {} - error : {}", content, errorStackInfo);
      responseFuture = ResponseUtils.buildFailedFutureResponse(errorStackInfo);
      metricsManager.time(sw.elapsed(TimeUnit.NANOSECONDS), "failureIndex");
      return responseFuture;
    }
  }

  @Override
  public Future<LindenResult> handleClusterSearchRequest(final String bql) {
    final Stopwatch sw = Stopwatch.createStarted();
    return clusterExecutorPool.apply(new Function0<LindenResult>() {
      @Override
      public LindenResult apply() {
        LindenResult result = null;
        String logTag = null;
        try {
          long eps = sw.elapsed(TimeUnit.MILLISECONDS);
          if (eps > 10) {
            LOGGER.warn("Warning: clusterExecutorPool took " + eps + "ms to start handleClusterSearchRequest.");
            if (eps > clusterFuturePoolWaitTimeout) {
              result = buildLindenFailedResult("Waiting time is too long, " + eps + "ms in cluster future pool");
              return result;
            }
          }
          LindenRequest request = bqlCompiler.compile(bql);
          if (request.isSetSearchRequest()) {
            result = lindenCluster.search(request.getSearchRequest());
            if (result.isSuccess()) {
              logTag = "search";
            } else {
              logTag = "failureSearch";
            }
          } else {
            result = new LindenResult().setSuccess(false).setError("invalid search Bql");
            logTag = "invalidSearchBql";
          }
        } catch (Exception e) {
          String errorStackInfo = Throwables.getStackTraceAsString(e);
          result = buildLindenFailedResult(errorStackInfo);
          logTag = "exceptionalSearchBql";
        } finally {
          metricsManager.time(sw.elapsed(TimeUnit.NANOSECONDS), logTag);
          result.setCost((int) sw.elapsed(TimeUnit.MILLISECONDS));
          if (result.isSuccess()) {
            LOGGER.info("Cluster search request succeeded bql: {}, hits: {}, cost: {} ms.", bql, result.getHitsSize(),
                        result.getCost());
          } else {
            LOGGER.error("Cluster search request failed bql: {}, error: {}, cost: {} ms.", bql, result.getError(),
                         result.getCost());
          }
          return result;
        }
      }
    });
  }

  @Override
  public Future<Response> handleClusterDeleteRequest(final String bql) {
    final Stopwatch sw = Stopwatch.createStarted();
    return clusterExecutorPool.apply(new Function0<Response>() {
      @Override
      public Response apply() {
        Response response = null;
        String logTag = null;
        try {
          long eps = sw.elapsed(TimeUnit.MILLISECONDS);
          if (eps > 10) {
            LOGGER.warn("Warning: clusterExecutorPool took " + eps + "ms to start handleClusterDeleteRequest.");
            if (eps > clusterFuturePoolWaitTimeout) {
              response =
                  ResponseUtils.buildFailedResponse("Waiting time is too long, " + eps + "ms in cluster future pool");
              return response;
            }
          }
          LindenRequest request = bqlCompiler.compile(bql);
          if (request.isSetDeleteRequest()) {
            response = lindenCluster.delete(request.getDeleteRequest());
            if (response.isSuccess()) {
              logTag = "delete";
            } else {
              logTag = "failureDelete";
            }
          } else {
            response = ResponseUtils.buildFailedResponse("invalid delete Bql");
            logTag = "invalidDeleteBql";
          }
        } catch (Exception e) {
          String errorStackInfo = Throwables.getStackTraceAsString(e);
          response = ResponseUtils.buildFailedResponse(errorStackInfo);
          logTag = "exceptionalDeleteBql";
        } finally {
          metricsManager.time(sw.elapsed(TimeUnit.NANOSECONDS), logTag);
          if (response.isSuccess()) {
            LOGGER.info("Cluster delete succeeded bql: {}, cost: {} ms.", bql, sw.elapsed(TimeUnit.MILLISECONDS));
          } else {
            LOGGER.error("Cluster delete failed bql: {}, cost: {} ms.", bql, sw.elapsed(TimeUnit.MILLISECONDS));
          }
          return response;
        }
      }
    });
  }

  @Override
  public Future<LindenResult> handleClusterBqlRequest(final String bql) {
    final Stopwatch sw = Stopwatch.createStarted();
    return clusterExecutorPool.apply(new Function0<LindenResult>() {
      @Override
      public LindenResult apply() {
        LindenResult result = null;
        String logTag = null;
        try {
          long eps = sw.elapsed(TimeUnit.MILLISECONDS);
          if (eps > 10) {
            LOGGER.warn("Warning: clusterExecutorPool took " + eps + "ms to start handleClusterBqlRequest.");
            if (eps > clusterFuturePoolWaitTimeout) {
              result = buildLindenFailedResult("Waiting time is too long, " + eps + "ms in cluster future pool");
              return result;
            }
          }
          LindenRequest request = bqlCompiler.compile(bql);
          if (request.isSetSearchRequest()) {
            result = lindenCluster.search(request.getSearchRequest());
            if (result.isSuccess()) {
              logTag = "search";
            } else {
              logTag = "failureSearch";
            }
          } else if (request.isSetDeleteRequest()) {
            Response response = lindenCluster.delete(request.getDeleteRequest());
            result = new LindenResult().setSuccess(response.isSuccess()).setError(response.getError());
            if (result.isSuccess()) {
              logTag = "delete";
            } else {
              logTag = "failureDelete";
            }
          } else {
            result = buildLindenFailedResult("unsupported Bql");
            logTag = "unsupportedBql";
          }
        } catch (Exception e) {
          String errorStackInfo = Throwables.getStackTraceAsString(e);
          result = buildLindenFailedResult(errorStackInfo);
          logTag = "exceptionalBql";
        } finally {
          metricsManager.time(sw.elapsed(TimeUnit.NANOSECONDS), logTag);
          result.setCost((int) sw.elapsed(TimeUnit.MILLISECONDS));
          if (result.isSuccess()) {
            LOGGER.info("Cluster request succeeded bql: {}, hits: {}, cost: {} ms.", bql, result.getHitsSize(),
                        result.getCost());
          } else {
            LOGGER.error("Cluster request failed bql: {}, error: {}, cost: {} ms.", bql, result.getError(),
                         result.getCost());
          }
          return result;
        }
      }
    });
  }

  @Override
  public Future<Response> handleClusterCommand(final String command) {
    final Stopwatch sw = Stopwatch.createStarted();
    return clusterExecutorPool.apply(new Function0<Response>() {
      @Override
      public Response apply() {
        Response response = null;
        String logTag = null;
        try {
          long eps = sw.elapsed(TimeUnit.MILLISECONDS);
          if (eps > 10) {
            LOGGER.warn("Warning: clusterExecutorPool took " + eps + "ms to start handleClusterCommand.");
            if (eps > clusterFuturePoolWaitTimeout) {
              response =
                  ResponseUtils.buildFailedResponse("Waiting time is too long, " + eps + "ms in cluster future pool");
              return response;
            }
          }
          response = lindenCluster.executeCommand(command);
          if (response.isSuccess()) {
            logTag = "command";
          } else {
            logTag = "failureCommand";
          }
        } catch (Exception e) {
          String errorStackInfo = Throwables.getStackTraceAsString(e);
          response = ResponseUtils.buildFailedResponse(errorStackInfo);
          logTag = "exceptionalCommand";
        } finally {
          metricsManager.time(sw.elapsed(TimeUnit.NANOSECONDS), logTag);
          if (response.isSuccess()) {
            LOGGER.info("handleClusterCommand succeeded command: {}, cost: {} ms.", command,
                        sw.elapsed(TimeUnit.MILLISECONDS));
          } else {
            LOGGER.error("handleClusterCommand failed command: {}, cost: {} ms.", command,
                         sw.elapsed(TimeUnit.MILLISECONDS));
          }
          return response;
        }
      }
    });
  }

  @Override
  public Future<Response> executeCommand(final String command) {
    final Stopwatch sw = Stopwatch.createStarted();
    return instanceExecutorPool.apply(new Function0<Response>() {
      @Override
      public Response apply() {
        LOGGER.info("Receive command {}", command);
        Response response = null;
        try {
          long eps = sw.elapsed(TimeUnit.MILLISECONDS);
          if (eps > 10) {
            LOGGER.warn("Warning: instanceExecutorPool took " + eps + "ms to start executeCommand.");
            if (eps > instanceFuturePoolWaitTimeout) {
              response =
                  ResponseUtils.buildFailedResponse("Waiting time is too long, " + eps + "ms in instance future pool");
              return response;
            }
          }
          response = lindenCore.executeCommand(command);
        } catch (Exception e) {
          String errorStackInfo = Throwables.getStackTraceAsString(e);
          response = ResponseUtils.buildFailedResponse(errorStackInfo);
        } finally {
          if (response.isSuccess()) {
            LOGGER.info("executeCommand succeeded, command: {}, cost: {} ms.", command,
                        sw.elapsed(TimeUnit.MILLISECONDS));
          } else {
            LOGGER.error("executeCommand failed, content: {}, cost: {} ms.", command,
                         sw.elapsed(TimeUnit.MILLISECONDS));
          }
          return response;
        }
      }
    });
  }

  @Deprecated
  @Override
  public Future<LindenResult> searchByBqlCluster(String bql) {
    return handleClusterBqlRequest(bql);
  }

  private LindenResult buildLindenFailedResult(String error) {
    return new LindenResult(false).setError(error);
  }

  public void close() throws IOException {
    indexingManager.stop();
    lindenCluster.close();
    lindenCore.close();
    metricsManager.close();
    lindenWarmer.close();
    shardingStrategy.close();

    config.deconstruct();

    checkupThread.interrupt();
    clusterExecutorPool.executor().shutdown();
    instanceExecutorPool.executor().shutdown();
  }

  public void refresh() throws IOException {
    lindenCore.refresh();
  }

  public LindenConfig getLindenConfig() {
    return config;
  }
}
