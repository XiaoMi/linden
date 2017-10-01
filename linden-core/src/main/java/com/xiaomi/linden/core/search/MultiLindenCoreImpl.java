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

package com.xiaomi.linden.core.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Throwables;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.FutureTransformer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import com.xiaomi.linden.cluster.ResultMerger;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.RuntimeInfoUtils;
import com.xiaomi.linden.core.indexing.DocNumLimitMultiIndexStrategy;
import com.xiaomi.linden.core.indexing.IndexNameCustomizedMultiIndexStrategy;
import com.xiaomi.linden.core.indexing.MultiIndexStrategy;
import com.xiaomi.linden.core.indexing.TimeLimitMultiIndexStrategy;
import com.xiaomi.linden.thrift.common.FileDiskUsageInfo;
import com.xiaomi.linden.thrift.common.IndexRequestType;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenServiceInfo;
import com.xiaomi.linden.thrift.common.Response;
import com.xiaomi.linden.util.FileNameUtils;
import com.xiaomi.linden.util.PrefixNameFileFilter;
import com.xiaomi.linden.util.ResponseUtils;

public class MultiLindenCoreImpl extends LindenCore {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiLindenCoreImpl.class);
  private static final int DEFAULT_SEARCH_TIMEOUT = 2000;
  private static final String LINDEN = "linden";
  private static final String EXPIRED_INDEX_NAME_PREFIX = "expired_";
  private final Map<String, LindenCore> lindenCoreMap = new ConcurrentHashMap<>();
  private final LindenConfig config;
  private String baseIndexDir;
  private MultiIndexStrategy multiIndexStrategy;

  public MultiLindenCoreImpl(final LindenConfig config) throws Exception {
    this.config = config;
    this.baseIndexDir = config.getIndexDirectory();

    LindenConfig.MultiIndexDivisionType divisionType = config.getMultiIndexDivisionType();
    switch (divisionType) {
      case TIME_HOUR:
      case TIME_DAY:
      case TIME_MONTH:
      case TIME_YEAR:
        multiIndexStrategy = new TimeLimitMultiIndexStrategy(this, config);
        break;
      case DOC_NUM:
        multiIndexStrategy = new DocNumLimitMultiIndexStrategy(this, config);
        break;
      case INDEX_NAME:
        multiIndexStrategy = new IndexNameCustomizedMultiIndexStrategy(this);
        break;
    }

    if (config.getIndexType() != LindenConfig.IndexType.RAM) {
      File[] files = getIndexDirectories();
      if (files != null) {
        int indexNum = files.length;
        if (config.getMultiIndexMaxLiveIndexNum() != -1
            && config.getMultiIndexMaxLiveIndexNum() < indexNum) {
          indexNum = config.getMultiIndexMaxLiveIndexNum();
        }
        for (int i = 0; i < indexNum; ++i) {
          getLindenCore(files[i].getName());
        }
      }
    }
  }

  private File[] getIndexDirectories() {
    File[] files = new File(baseIndexDir).listFiles(new PrefixNameFileFilter(MultiIndexStrategy.MULTI_INDEX_PREFIX_NAME));
    if (files != null && files.length > 0) {
      FileNameUtils.sort(files, -1);
    }
    return files;
  }

  public Map<String, LindenCore> getLindenCoreMap() {
    return this.lindenCoreMap;
  }

  @Override
  public LindenResult search(final LindenSearchRequest request) throws IOException {
    final List<LindenResult> resultList = new ArrayList<>();
    final List<Future<BoxedUnit>> futures = new ArrayList<>();

    List<String> indexNames;
    // Only INDEX_NAME division type supports specified index name request
    if (multiIndexStrategy instanceof TimeLimitMultiIndexStrategy
        || multiIndexStrategy instanceof DocNumLimitMultiIndexStrategy) {
      indexNames = new ArrayList<>(lindenCoreMap.keySet());
    } else {
      if (request.getIndexNames() == null || (request.getIndexNamesSize() == 1 && request.getIndexNames().get(0)
          .equals(LINDEN))) {
        indexNames = new ArrayList<>(lindenCoreMap.keySet());
      } else {
        indexNames = request.getIndexNames();
        for (int i = 0; i < indexNames.size(); ++i) {
          indexNames.set(i, MultiIndexStrategy.MULTI_INDEX_PREFIX_NAME + indexNames.get(i));
        }
      }
    }

    for (final String indexName : indexNames) {
      final LindenCore core = lindenCoreMap.get(indexName);
      if (core != null) {
        futures
            .add(Future.value(core.search(request)).transformedBy(new FutureTransformer<LindenResult, BoxedUnit>() {
              @Override
              public BoxedUnit map(LindenResult lindenResult) {
                synchronized (resultList) {
                  resultList.add(lindenResult);
                }
                return BoxedUnit.UNIT;
              }

              @Override
              public BoxedUnit handle(Throwable t) {
                LOGGER.info("Index {} search error: {}", indexName,
                            Throwables.getStackTraceAsString(t));
                return BoxedUnit.UNIT;
              }
            }));
      } else {
        LOGGER.error("Index {} doesn't exist.", indexName);
      }
    }
    Future<List<BoxedUnit>> collected = Future.collect(futures);
    try {
      collected.apply(Duration.apply(DEFAULT_SEARCH_TIMEOUT, TimeUnit.MILLISECONDS));
    } catch (Exception e) {
      LOGGER.error("Multi-index search error: {}", Throwables.getStackTraceAsString(e));
    }
    return ResultMerger.merge(request, resultList);
  }

  @Override
  public Response delete(LindenDeleteRequest request) throws IOException {
    List<String> indexNames;
    // Only INDEX_NAME division type supports specified index name request
    if (multiIndexStrategy instanceof TimeLimitMultiIndexStrategy
        || multiIndexStrategy instanceof DocNumLimitMultiIndexStrategy) {
      indexNames = new ArrayList<>(lindenCoreMap.keySet());
    } else {
      if (request.getIndexNames() == null || (request.getIndexNamesSize() == 1 && request.getIndexNames().get(0)
          .equals(LINDEN))) {
        indexNames = new ArrayList<>(lindenCoreMap.keySet());
      } else {
        indexNames = request.getIndexNames();
        for (int i = 0; i < indexNames.size(); ++i) {
          indexNames.set(i, MultiIndexStrategy.MULTI_INDEX_PREFIX_NAME + indexNames.get(i));
        }
      }
    }

    StringBuilder errorInfo = new StringBuilder();
    for (final String indexName : indexNames) {
      final LindenCore core = lindenCoreMap.get(indexName);
      if (core != null) {
        try {
          Response response = core.delete(request);
          if (!response.isSuccess()) {
            errorInfo.append(indexName + ":" + response.getError() + ";");
            LOGGER.error("Multi-index {} delete error: {}", indexName, response.error);
          }
        } catch (Exception e) {
          errorInfo.append(indexName + ":" + Throwables.getStackTraceAsString(e) + ";");
          LOGGER.error("Multi-index {} delete error: {}", indexName, Throwables.getStackTraceAsString(e));
        }
      } else {
        errorInfo.append(indexName + " doesn't exist");
        LOGGER.error("Multi-index {} delete error: " + indexName + " doesn't exist");
      }
    }
    if (errorInfo.length() > 0) {
      return ResponseUtils.buildFailedResponse("Multi-index delete error: " + errorInfo.toString());
    } else {
      return ResponseUtils.SUCCESS;
    }
  }

  @Override
  public void refresh() throws IOException {
    for (Map.Entry<String, LindenCore> entry : lindenCoreMap.entrySet()) {
      entry.getValue().refresh();
    }
  }

  public synchronized LindenCore getLindenCore(String indexName) throws IOException {
    LindenCore lindenCore = lindenCoreMap.get(indexName);
    if (lindenCore == null) {
      lindenCore = new LindenCoreImpl(config, indexName);
      lindenCoreMap.put(indexName, lindenCore);
      if (config.getMultiIndexMaxLiveIndexNum() != -1 &&
          config.getMultiIndexMaxLiveIndexNum() < lindenCoreMap.size()) {
        List<String> keys = new ArrayList<>(lindenCoreMap.keySet());
        Collections.sort(keys, new Comparator<String>() {
          @Override
          public int compare(String o1, String o2) {
            return o1.compareTo(o2);
          }
        });
        String oldestIndexName = keys.get(0);
        LindenCore core = lindenCoreMap.remove(oldestIndexName);
        core.close();

        if (config.getIndexType() != LindenConfig.IndexType.RAM) {
          String dir = FilenameUtils.concat(baseIndexDir, oldestIndexName);
          String destDir = FilenameUtils.concat(baseIndexDir, EXPIRED_INDEX_NAME_PREFIX + oldestIndexName);
          if (new File(dir).exists()) {
            FileUtils.moveDirectory(new File(dir), new File(destDir));
          }
        }
      }
    }
    return lindenCore;
  }

  @Override
  public Response index(LindenIndexRequest request) throws IOException {
    if (request.getType().equals(IndexRequestType.DELETE_INDEX)) {
      return deleteIndex(request);
    }

    LindenCore core = multiIndexStrategy.getCurrentLindenCore(request.getIndexName());
    if (core != null) {
      switch (request.getType()) {
        case INDEX:
          return core.index(request);
        case DELETE:
          return delete(request);
        case REPLACE:
          request.setType(IndexRequestType.DELETE);
          Response response = delete(request);
          if (response.isSuccess()) {
            request.setType(IndexRequestType.INDEX);
            response = core.index(request);
          }
          return response;
        default:
          return ResponseUtils.buildFailedResponse("IndexRequestType " + request.getType() + " is not supported.");
      }
    } else {
      return ResponseUtils.buildFailedResponse("No corresponding linden core is found.");
    }
  }

  private Response deleteIndex(LindenIndexRequest request) throws IOException {

    // Only INDEX_NAME division type supports index delete
    if (multiIndexStrategy instanceof TimeLimitMultiIndexStrategy
        || multiIndexStrategy instanceof DocNumLimitMultiIndexStrategy) {
      return ResponseUtils.buildFailedResponse("Index delete is not supported in current multi-index core");
    }

    if (request.getIndexName() == null) {
      return ResponseUtils.buildFailedResponse("Index name is not set in index delete request.");
    }

    String fullIndexName = MultiIndexStrategy.MULTI_INDEX_PREFIX_NAME + request.getIndexName();
    if (lindenCoreMap.containsKey(fullIndexName)) {
      LindenCore core = lindenCoreMap.remove(fullIndexName);
      if (core != null) {
        core.close();
      }

      if (config.getIndexType() != LindenConfig.IndexType.RAM) {
        String dir = FilenameUtils.concat(baseIndexDir, fullIndexName);
        String destDir = FilenameUtils.concat(baseIndexDir, "delete_" + fullIndexName);
        if (new File(dir).exists()) {
          FileUtils.moveDirectory(new File(dir), new File(destDir));
        }
      }
      return ResponseUtils.SUCCESS;
    }
    return ResponseUtils.buildFailedResponse("Index " + request.getIndexName() + " is not found.");
  }

  private Response delete(LindenIndexRequest request) throws IOException {
    StringBuilder errorInfo = new StringBuilder();
    for (Map.Entry<String, LindenCore> entry : lindenCoreMap.entrySet()) {
      try {
        Response response = entry.getValue().index(request);
        if (!response.isSuccess()) {
          errorInfo.append(entry.getKey() + ":" + response.getError() + ";");
          LOGGER.error("Multi Linden core index {} delete error: {}", entry.getKey(), response.error);
        }
      } catch (Exception e) {
        errorInfo.append(entry.getKey() + ":" + Throwables.getStackTraceAsString(e) + ";");
        LOGGER.error("Multi index delete index {} error: {}.", entry.getKey(), Throwables.getStackTraceAsString(e));
      }
    }
    if (errorInfo.length() > 0) {
      return ResponseUtils.buildFailedResponse("Multi index delete index error: " + errorInfo.toString());
    }
    return ResponseUtils.SUCCESS;
  }

  @Override
  public void commit() throws IOException {
    for (Map.Entry<String, LindenCore> entry : lindenCoreMap.entrySet()) {
      entry.getValue().commit();
    }
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, LindenCore> entry : lindenCoreMap.entrySet()) {
      entry.getValue().close();
    }
  }

  @Override
  public LindenServiceInfo getServiceInfo() throws IOException {
    int docsNum = 0;
    LindenServiceInfo serviceInfo = new LindenServiceInfo();
    List<String> indexNames = new ArrayList<>();
    for (Map.Entry<String, LindenCore> entry : lindenCoreMap.entrySet()) {
      serviceInfo = entry.getValue().getServiceInfo();
      docsNum += serviceInfo.getDocsNum();
      indexNames.add(entry.getKey());
    }
    List<String> paths = new ArrayList<>();
    if (baseIndexDir != null) {
      paths.add(baseIndexDir);
    }
    if (config.getLogPath() != null) {
      paths.add(config.getLogPath());
    }
    List<FileDiskUsageInfo> fileUsedInfos = RuntimeInfoUtils.getRuntimeFileInfo(paths);
    serviceInfo.setDocsNum(docsNum).setIndexNames(indexNames).setFileUsedInfos(fileUsedInfos);
    return serviceInfo;
  }

  @Override
  public Response mergeIndex(int maxNumSegments) throws IOException {
    for (Map.Entry<String, LindenCore> entry : lindenCoreMap.entrySet()) {
      entry.getValue().mergeIndex(maxNumSegments);
    }
    return ResponseUtils.SUCCESS;
  }
}
