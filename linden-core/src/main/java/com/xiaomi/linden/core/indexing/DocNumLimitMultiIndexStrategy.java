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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.core.search.MultiLindenCoreImpl;
import com.xiaomi.linden.util.DateUtils;

public class DocNumLimitMultiIndexStrategy implements MultiIndexStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(DocNumLimitMultiIndexStrategy.class);
  private static int INDEX_CHECK_INTERVAL_MILLISECONDS = 5000;
  private MultiLindenCoreImpl multiLindenCoreImpl;
  private LindenConfig lindenConfig;
  private volatile LindenCore currentLindenCore;
  private Long lastCompareTime;

  public DocNumLimitMultiIndexStrategy(MultiLindenCoreImpl multiLindenCoreImpl, LindenConfig lindenConfig) {
    this.multiLindenCoreImpl = multiLindenCoreImpl;
    this.lindenConfig = lindenConfig;
    this.lastCompareTime = -1L;
  }

  @Override
  public LindenCore getCurrentLindenCore(String unusedIndexName) throws IOException {
    if (currentLindenCore == null) {
      synchronized (this) {
        if (currentLindenCore == null) {
          String fullIndexName = MULTI_INDEX_PREFIX_NAME + DateUtils.getCurrentTime();
          currentLindenCore = multiLindenCoreImpl.getLindenCore(fullIndexName);
          LOGGER.info("Create linden core, index name: {}", fullIndexName);
        }
      }
    } else {
      Long now = System.currentTimeMillis();
      if (now > (lastCompareTime + INDEX_CHECK_INTERVAL_MILLISECONDS)) {
        if (currentLindenCore.getServiceInfo() != null) {
          int docNum = currentLindenCore.getServiceInfo().getDocsNum();
          LOGGER.info("Current doc number: {}, multi index doc num limit: {}",
                      docNum, lindenConfig.getMultiIndexDocNumLimit());
          if (docNum >= lindenConfig.getMultiIndexDocNumLimit()) {
            synchronized (this) {
              docNum = currentLindenCore.getServiceInfo().getDocsNum();
              if (docNum >= lindenConfig.getMultiIndexDocNumLimit()) {
                String fullIndexName = MULTI_INDEX_PREFIX_NAME + DateUtils.getCurrentTime();
                currentLindenCore = multiLindenCoreImpl.getLindenCore(fullIndexName);
              }
            }
          }
          lastCompareTime = now;
        }
      }
    }
    return currentLindenCore;
  }
}
