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

package com.xiaomi.linden.core.plugin;

import com.xiaomi.linden.core.search.query.model.CustomCacheWrapper;
import com.xiaomi.linden.core.search.query.model.LindenScoreModelStrategy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestCustomCacheWrapper extends CustomCacheWrapper {
  private LindenScoreModelStrategy.FieldValues<Float> ranks;
  private LindenScoreModelStrategy.FieldValues<Integer> catIds;

  @Override
  public void init() throws IOException {
    ranks = getFieldValues("rank");
    catIds = getFieldValues("cat1");
  }

  @Override
  public TestCacheObject createValue(int doc) {
    TestCacheObject cacheObject = new TestCacheObject();
    Map<Integer, Float> hashMap = new HashMap<>();
    hashMap.put(doc, ranks.get(doc) + catIds.get(doc));
    cacheObject.setValues(hashMap);
    return cacheObject;
  }
}
