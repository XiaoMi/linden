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

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.core.search.MultiLindenCoreImpl;
import com.xiaomi.linden.util.DateUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TimeLimitMultiIndexStrategy implements MultiIndexStrategy {
  private MultiLindenCoreImpl multiLindenCoreImpl;
  private LindenConfig lindenConfig;
  private String currentIndexName;
  private LindenCore currentLindenCore;

  public TimeLimitMultiIndexStrategy(MultiLindenCoreImpl multiLindenCoreImpl, LindenConfig lindenConfig) {
    this.multiLindenCoreImpl = multiLindenCoreImpl;
    this.lindenConfig = lindenConfig;

    if (multiLindenCoreImpl.getLindenCoreMap() != null && multiLindenCoreImpl.getLindenCoreMap().size() > 0) {
      List<String> keys = new ArrayList<>(multiLindenCoreImpl.getLindenCoreMap().keySet());
      Collections.sort(keys, new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          return o2.compareTo(o1);
        }
      });
      currentIndexName = keys.get(0);
    } else {
      currentIndexName = null;
    }
  }

  @Override
  public LindenCore getCurrentLindenCore(String unusedIndexName) throws IOException {
    String suffixName;
    switch (lindenConfig.getMultiIndexDivisionType()) {
      case TIME_HOUR:
        suffixName = DateUtils.getCurrentHour();
        break;
      case TIME_DAY:
        suffixName = DateUtils.getCurrentDay();
        break;
      case TIME_MONTH:
        suffixName = DateUtils.getCurrentMonth();
        break;
      case TIME_YEAR:
        suffixName = DateUtils.getCurrentYear();
        break;
      default:
        throw new IOException("Unsupported division type in TimeLimitMultiIndexStrategy");
    }
    String fullIndexName = MULTI_INDEX_PREFIX_NAME + suffixName;
    if (currentIndexName == null || !fullIndexName.equals(currentIndexName)) {
      synchronized (this) {
        if (currentIndexName == null || !fullIndexName.equals(currentIndexName)) {
          currentLindenCore = multiLindenCoreImpl.getLindenCore(fullIndexName);
          currentIndexName = fullIndexName;
        }
      }
    }
    return currentLindenCore;
  }
}
