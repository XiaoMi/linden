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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import com.xiaomi.linden.core.search.query.model.LindenScoreModelStrategy;
import com.xiaomi.linden.thrift.common.LindenInputParam;
import com.xiaomi.linden.thrift.common.LindenValue;

public class TestScoreModelStrategy extends LindenScoreModelStrategy {
  private LindenScoreModelStrategy.FieldValues<TestCacheObject> customObjs;

  @Override
  public void init() throws IOException {
    customObjs = registerCustomCacheWrapper(new TestCustomCacheWrapper());
    List<LindenInputParam> params = getParams();
    LindenInputParam firstParam = params.get(0);
    Assert.assertEquals(new LindenInputParam("param1"), firstParam);
    LindenInputParam secondParam = params.get(1);
    Map<LindenValue,LindenValue> dicts = secondParam.getValue().getMapValue();
    Assert.assertEquals(2.0, dicts.get(new LindenValue().setStringValue("b")).getDoubleValue(), 0.001);
  }

  @Override
  public double computeScore() throws IOException {
    TestCacheObject cacheObject = customObjs.get();
    return cacheObject.getValues().get(doc());
  }
}
