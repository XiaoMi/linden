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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.RuntimeInfoUtils;
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

// When using HotSwapLindenCoreImpl, ideally you should stop sending docs to current index
// during preparing next index stage, else you should make sure all docs sent to current index
// are also included in your next index.
public class HotSwapLindenCoreImpl extends LindenCore {

  private static final Logger LOGGER = LoggerFactory.getLogger(HotSwapLindenCoreImpl.class);
  private static final String LINDEN = "linden";
  private static final String CURRENT_INDEX_NAME_PREFIX = "current_";
  private static final String NEXT_INDEX_NAME_PREFIX = "next_";
  private static final int MAX_NEXT_INDEX_NUM = 3;
  private LindenCore currentLindenCore;
  private String currentIndexName;
  private String currentIndexTimeStamp;
  private final Map<String, LindenCore> lindenCoreMap = new ConcurrentHashMap<>();
  private final LindenConfig lindenConfig;
  private String baseIndexDir;

  public HotSwapLindenCoreImpl(final LindenConfig config) throws Exception {
    lindenConfig = config;
    baseIndexDir = lindenConfig.getIndexDirectory();
    if (lindenConfig.getIndexType() != LindenConfig.IndexType.RAM) {
      File[] files = getIndexDirectories(CURRENT_INDEX_NAME_PREFIX);
      if (files != null && files.length > 0) {
        currentIndexName = files[0].getName();
        LOGGER.info("Current index name is " + currentIndexName);
      } else {
        currentIndexName = CURRENT_INDEX_NAME_PREFIX + System.currentTimeMillis();
        LOGGER.info("Create empty current index " + currentIndexName);
      }
      currentLindenCore = getLindenCore(currentIndexName);
      currentIndexTimeStamp = currentIndexName.substring(CURRENT_INDEX_NAME_PREFIX.length());

      files = getIndexDirectories(NEXT_INDEX_NAME_PREFIX);
      if (files != null && files.length > 0) {
        int indexNum = Math.min(files.length, MAX_NEXT_INDEX_NUM);
        for (int i = 0; i < indexNum; ++i) {
          File file = files[i];
          if (file.isDirectory()) {
            getLindenCore(file.getName());
          }
        }
      }
    } else {
      currentIndexName = CURRENT_INDEX_NAME_PREFIX + System.currentTimeMillis();
      currentLindenCore = getLindenCore(currentIndexName);
      currentIndexTimeStamp = currentIndexName.substring(CURRENT_INDEX_NAME_PREFIX.length());
    }
  }

  private File[] getIndexDirectories(final String prefix) {
    File[] files = new File(baseIndexDir).listFiles(new PrefixNameFileFilter(prefix));
    if (files != null && files.length > 0) {
      FileNameUtils.sort(files, -1);
    }
    return files;
  }

  @Override
  public LindenResult search(final LindenSearchRequest request) throws IOException {
    return currentLindenCore.search(request);
  }

  @Override
  public Response delete(LindenDeleteRequest request) throws IOException {
    if (!request.isSetIndexNames()) {
      return currentLindenCore.delete(request);
    }
    if (request.getIndexNames().size() == 1) {
      if (request.getIndexNames().get(0).equals(LINDEN)) {
        return currentLindenCore.delete(request);
      } else {
        final LindenCore core = lindenCoreMap.get(request.getIndexNames().get(0));
        if (core == null) {
          throw new IOException("No linden core named " + request.getIndexNames().get(0) + " is found");
        }
        return core.delete(request);
      }
    }
    throw new IOException("Bad index names in delete request");
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
      lindenCore = new LindenCoreImpl(lindenConfig, indexName);
      lindenCoreMap.put(indexName, lindenCore);
      LOGGER.info("Create new Linden core: " + indexName);
      // current linden core is also in lindenCoreMap
      if (lindenCoreMap.size() > MAX_NEXT_INDEX_NUM + 1) {
        List<String> keys = new ArrayList<>(lindenCoreMap.keySet());
        String oldestCoreName = null;
        for (int i = 0; i < keys.size(); ++i) {
          if (keys.get(i).startsWith(NEXT_INDEX_NAME_PREFIX)) {
            if (oldestCoreName == null || oldestCoreName.compareTo(keys.get(i)) > 0) {
              oldestCoreName = keys.get(i);
            }
          }
        }
        if (oldestCoreName == null) {
          throw new IOException("There is no next linden core in the map.");
        }
        LindenCore core = lindenCoreMap.remove(oldestCoreName);
        core.close();
        if (lindenConfig.getIndexType() != LindenConfig.IndexType.RAM) {
          String dir = FilenameUtils.concat(baseIndexDir, oldestCoreName);
          FileUtils.deleteQuietly(new File(dir));
          LOGGER.info("Abandon and delete index: " + oldestCoreName);
        }
      }
    }
    return lindenCore;
  }

  @Override
  public synchronized Response swapIndex(String indexName) throws IOException {
    if (Strings.isNullOrEmpty(indexName)) {
      throw new IOException("Index name is empty in swap index request.");
    }
    if (!indexName.startsWith(NEXT_INDEX_NAME_PREFIX)) {
      throw new IOException("Invalid index name: " + indexName);
    }
    // May receive swap request more than one times
    String nextIndexTimeStamp = indexName.substring(NEXT_INDEX_NAME_PREFIX.length());
    if (!currentIndexTimeStamp.equals(nextIndexTimeStamp)) {
      LOGGER.info("Begin swapping index " + indexName);
      if (!lindenCoreMap.containsKey(indexName)) {
        LOGGER.error("No index found for: " + indexName);
        return ResponseUtils.buildFailedResponse("No index found for: " + indexName);
      }
      if (lindenConfig.getIndexType() != LindenConfig.IndexType.RAM) {
        LindenCore nextCore = lindenCoreMap.remove(indexName);
        nextCore.close();
        String dir = FilenameUtils.concat(baseIndexDir, indexName);
        String newIndexName = indexName.replaceFirst(NEXT_INDEX_NAME_PREFIX, CURRENT_INDEX_NAME_PREFIX);
        String destDir = FilenameUtils.concat(baseIndexDir, newIndexName);
        FileUtils.moveDirectory(new File(dir), new File(destDir));
        LOGGER.info("Move " + dir + " directory to " + destDir);
        nextCore = getLindenCore(newIndexName);
        // swap
        String lastIndexName = currentIndexName;
        currentLindenCore = nextCore;
        currentIndexName = newIndexName;
        currentIndexTimeStamp = nextIndexTimeStamp;

        // remove last core from map
        LindenCore lastCore = lindenCoreMap.remove(lastIndexName);
        // close last core
        lastCore.close();
        // mark last core expired
        dir = FilenameUtils.concat(baseIndexDir, lastIndexName);
        FileUtils.deleteQuietly(new File(dir));
        LOGGER.info("Expire and delete index: " + lastIndexName);
      } else {
        // RAM type
        // swap
        String lastIndexName = currentIndexName;
        currentLindenCore = lindenCoreMap.get(indexName);
        currentIndexName = indexName;
        // remove last core from map
        LindenCore lastCore = lindenCoreMap.remove(lastIndexName);
        // close last core
        lastCore.close();
      }
      LOGGER.info("Swapping index " + indexName + " done");
    }
    return ResponseUtils.SUCCESS;
  }

  @Override
  public Response mergeIndex(int maxNumSegments) throws IOException {
    return currentLindenCore.mergeIndex(maxNumSegments);
  }

  @Override
  public Response flushIndex() throws IOException {
    return currentLindenCore.flushIndex();
  }

  @Override
  public Response index(LindenIndexRequest request) throws IOException {
    if (request.getType().equals(IndexRequestType.SWAP_INDEX)) {
      Response response;
      try {
        response = swapIndex(request.getIndexName());
      } catch (Exception e) {
        LOGGER.error("Swapping index " + request.getIndexName() + " failed, " + e);
        throw new IOException("Swapping index " + request.getIndexName() + " failed!", e);
      }
      return response;
    }
    String indexName = request.getIndexName();
    LindenCore core = currentLindenCore;
    if (indexName != null && !indexName.equals(currentIndexName)) {
      if (indexName.startsWith(NEXT_INDEX_NAME_PREFIX)) {
        // Accept bootstrap index request after swap is done in case something unexpected happened
        // that swap command is executed before bootstrap is done
        if (!indexName.substring(NEXT_INDEX_NAME_PREFIX.length()).equals(currentIndexTimeStamp)) {
          core = getLindenCore(indexName);
        }
      } else {
        throw new IOException("Bad index name " + indexName + " in HotSwapLindenCoreImpl.");
      }
    }
    return core.index(request);
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
    LindenServiceInfo serviceInfo = currentLindenCore.getServiceInfo();
    List<String> indexNames = new ArrayList<>();
    for (Map.Entry<String, LindenCore> entry : lindenCoreMap.entrySet()) {
      indexNames.add(entry.getKey());
    }
    List<String> paths = new ArrayList<>();
    if (baseIndexDir != null) {
      paths.add(baseIndexDir);
    }
    if (lindenConfig.getLogPath() != null) {
      paths.add(lindenConfig.getLogPath());
    }
    List<FileDiskUsageInfo> fileUsedInfos = RuntimeInfoUtils.getRuntimeFileInfo(paths);
    serviceInfo.setIndexNames(indexNames).setFileUsedInfos(fileUsedInfos);
    return serviceInfo;
  }
}
