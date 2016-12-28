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

import mockit.Mock;

import com.xiaomi.linden.lucene.query.flexiblequery.FlexibleScoreModelStrategy;
import com.xiaomi.linden.thrift.common.LindenInputParam;
import com.xiaomi.linden.thrift.common.LindenValue;

public class MockedScoreModelV1 extends FlexibleScoreModelStrategy {

  protected List<LindenInputParam> scoreModelParams;
  protected float base;
  protected boolean explain;

  @Mock
  public void init() throws IOException {
    scoreModelParams = getParams();
    base = hasValidParam(0) ? (float) getParamValue(0).getDoubleValue() : 2f;
    explain = hasValidParam(1) && getParamValue(1).getStringValue().equalsIgnoreCase("true");
  }

  public boolean hasValidParam(int i) {
    return scoreModelParams.size() > i && scoreModelParams.get(i) != null && scoreModelParams.get(i).isSetValue();
  }

  public LindenValue getParamValue(int i) {
    return scoreModelParams.get(i).getValue();
  }

  @Mock
  public double computeScore() throws IOException {
    return 1;
  }
}
