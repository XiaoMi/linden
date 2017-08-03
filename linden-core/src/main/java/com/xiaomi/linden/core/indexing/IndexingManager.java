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

package com.xiaomi.linden.core.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.plugin.gateway.DataProvider;
import com.xiaomi.linden.thrift.common.IndexRequestType;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;
import com.xiaomi.linden.thrift.common.Response;

abstract public class IndexingManager<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingManager.class);

  protected final DataProvider<T> provider;
  protected final List<LinkedBlockingQueue<LindenIndexRequest>> queues = new ArrayList<>();
  protected final ExecutorService indexingService;
  protected final int threadNum;
  protected final Producer producer;
  protected final LindenConfig lindenConfig;
  private final LindenCore lindenCore;
  protected final List<Future<?>> consumerResponses = new ArrayList<>();
  protected final Function<T, LindenIndexRequest> indexRequestParser;
  private final ShardingStrategy shardingStrategy;

  public IndexingManager(DataProvider<T> provider, LindenConfig lindenConfig, LindenCore lindenCore,
                         Function<T, LindenIndexRequest> parser, ShardingStrategy shardingStrategy) {
    this.provider = provider;
    if (lindenConfig.getLindenCoreMode() != LindenConfig.LindenCoreMode.HOTSWAP) {
      this.threadNum = lindenConfig.getIndexManagerThreadNum();
    } else {
      // hot swap mode need order-preserving index data, since swap command data is in the same channel with index data
      this.threadNum = 1;
    }
    this.indexingService = Executors.newFixedThreadPool(threadNum);
    this.producer = new Producer();
    this.lindenConfig = lindenConfig;
    this.lindenCore = lindenCore;
    this.indexRequestParser = parser;
    this.shardingStrategy = shardingStrategy;
  }

  protected void index(LindenIndexRequest indexRequest) {
    try {
      Response response = lindenCore.index(indexRequest);
      if (!response.isSuccess()) {
        LOGGER.error("Handle index request failed : type={}, {}", indexRequest.getType(), indexRequest,
                     response.getError());
      }
    } catch (Exception e) {
      LOGGER.error("Handle index request failed : {} - {}", indexRequest, Throwables.getStackTraceAsString(e));
    }
  }

  public void start() {
    producer.start();
    for (int i = 0; i < threadNum; ++i) {
      consumerResponses.add(indexingService.submit(new Consumer(i)));
      queues.add(new LinkedBlockingQueue<LindenIndexRequest>(500));
    }
  }

  public void stop() {
    LOGGER.info("Shutting down indexing manager...");
    producer.interrupt();
    try {
      LOGGER.info("Shutting down indexing manager...first close producer");
      producer.join();
    } catch (InterruptedException e) {
      LOGGER.error("Fail to first close producer");
    }
    try {
      LOGGER.info("Shutting down indexing manager...second close dataProvider");
      provider.close();
    } catch (IOException e) {
      LOGGER.error("Fail to close dataProvider");
    }
    for (Future<?> response : consumerResponses) {
      response.cancel(true);
    }
    indexingService.shutdown();
    try {
      LOGGER.info("Shutting down indexing manager...third close indexingConsumers");
      indexingService.awaitTermination(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Fail to close indexingConsumers");
    }
    LOGGER.info("Shutting down indexing manager...done");
  }

  private class Producer extends Thread {

    @Override
    public void run() {
      while (true) {
        try {
          T data = provider.next();
          if (data == null) {
            Thread.sleep(500);
            continue;
          }
          LindenIndexRequest indexRequest = indexRequestParser.apply(data);
          assert indexRequest != null;
          if (indexRequest.getType() == IndexRequestType.DELETE || shardingStrategy
              .accept(indexRequest.getId(), indexRequest.getRouteParam())) {
            int queueIdx = 0;
            // indexRequest.getId() is null when this is an operation request
            if (indexRequest.getId() != null) {
              int reHash = Integer.toString(indexRequest.getId().hashCode()).hashCode();
              queueIdx = (reHash & Integer.MAX_VALUE) % threadNum;
            }
            queues.get(queueIdx).put(indexRequest);
          }
        } catch (InterruptedException e) {
          break;
        } catch (Exception e) {
          LOGGER.error("{}", Throwables.getStackTraceAsString(e));
        }
      }
      LOGGER.info("Indexing producer exit.");
    }
  }

  private class Consumer extends Thread {

    private int cid;

    public Consumer(int cid) {
      this.cid = cid;
    }

    @Override
    public void run() {
      while (true) {
        try {
          LindenIndexRequest request = queues.get(cid).poll(500, TimeUnit.MILLISECONDS);
          if (request == null) {
            Thread.sleep(500);
            continue;
          }
          index(request);
        } catch (InterruptedException e) {
          break;
        }
      }
      LOGGER.info("Indexing consumer exit.");
    }
  }
}
