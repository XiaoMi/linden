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

import com.google.common.base.Strings;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.core.search.MultiLindenCoreImpl;

import java.io.IOException;

public class IndexNameCustomizedMultiIndexStrategy implements MultiIndexStrategy {
  private MultiLindenCoreImpl multiLindenCoreImpl;

  public IndexNameCustomizedMultiIndexStrategy(MultiLindenCoreImpl multiLindenCoreImpl) {
    this.multiLindenCoreImpl = multiLindenCoreImpl;
  }

  @Override
  public LindenCore getCurrentLindenCore(String indexName) throws IOException {
    if (Strings.isNullOrEmpty(indexName)) {
      return null;
    }
    String fullIndexName = MULTI_INDEX_PREFIX_NAME + indexName;
    LindenCore lindenCore;
    if (multiLindenCoreImpl.getLindenCoreMap().containsKey(fullIndexName)) {
      lindenCore = multiLindenCoreImpl.getLindenCoreMap().get(fullIndexName);
    } else {
      synchronized (this) {
        if (multiLindenCoreImpl.getLindenCoreMap().containsKey(fullIndexName)) {
          lindenCore = multiLindenCoreImpl.getLindenCoreMap().get(fullIndexName);
        } else {
          lindenCore = multiLindenCoreImpl.getLindenCore(fullIndexName);
        }
      }
    }
    return lindenCore;
  }
}
