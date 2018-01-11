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

import java.io.IOException;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenServiceInfo;
import com.xiaomi.linden.thrift.common.Response;
import com.xiaomi.linden.util.ResponseUtils;

abstract public class LindenCore {

  private static final Logger LOGGER = LoggerFactory.getLogger(LindenCore.class);

  private static final String COMMAND_TYPE = "type";
  private static final String COMMAND_OPTIONS = "options";

  public boolean isIndexingManagerPaused() {
    return indexingManagerPaused;
  }

  private volatile boolean indexingManagerPaused = false;

  abstract public LindenResult search(LindenSearchRequest request) throws IOException;

  abstract public Response delete(LindenDeleteRequest request) throws IOException;

  abstract public void refresh() throws IOException;

  abstract public Response index(LindenIndexRequest request) throws IOException;

  abstract public void commit() throws IOException;

  abstract public void close() throws IOException;

  abstract public LindenServiceInfo getServiceInfo() throws IOException;

  public Response swapIndex(String indexName) throws IOException {
    throw new IOException("Current linden core mode doesn't support swap index");
  }

  abstract public Response mergeIndex(int maxNumSegments) throws IOException;

  abstract public Response flushIndex() throws IOException;

  public Response executeCommand(String command) throws IOException {
    JSONObject jsonCommand = JSONObject.parseObject(command);
    String type = jsonCommand.getString(COMMAND_TYPE);
    JSONObject options = jsonCommand.getJSONObject(COMMAND_OPTIONS);
    if (type == null) {
      throw new IOException("No command type");
    }
    LindenConfig.CommandType commandType = LindenConfig.CommandType.valueOf(type.toUpperCase());
    switch (commandType) {
      case SWAP_INDEX:
        String indexName = options.getString("index");
        if (StringUtils.isEmpty(indexName)) {
          throw new IOException("Index name is empty in options " + options);
        }
        return swapIndex(indexName);
      case MERGE_INDEX:
        Integer maxNumSegments = options.getInteger("count");
        if (maxNumSegments == null || maxNumSegments < 1) {
          throw new IOException("Invalid count in options " + options);
        }
        return mergeIndex(maxNumSegments);
      case FLUSH_INDEX:
        return flushIndex();
      case PAUSE_INDEXING:
        return PauseIndexingManager();
      case RESUME_INDEXING:
        return ResumeIndexingManager();
      default:
        throw new IOException("Unsupported command type: " + command);
    }
  }

  public Response PauseIndexingManager() {
    indexingManagerPaused = true;
    LOGGER.info("Indexing manager is paused");
    return ResponseUtils.SUCCESS;
  }

  public Response ResumeIndexingManager() {
    indexingManagerPaused = false;
    LOGGER.info("Indexing manager is resumed");
    return ResponseUtils.SUCCESS;
  }
}
