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

package com.xiaomi.linden.thrift.builder.query;

import com.xiaomi.linden.thrift.common.LindenDisMaxQuery;
import com.xiaomi.linden.thrift.common.LindenQuery;

import java.util.ArrayList;
import java.util.List;

public class LindenDisMaxQueryBuilder extends LindenQueryBuilder {

  private List<LindenQuery> queries = new ArrayList<>();
  private float tie = 0.7f;

  public LindenDisMaxQueryBuilder setTie(float tie) {
    this.tie = tie;
    return this;
  }

  public LindenDisMaxQueryBuilder addQuery(LindenQuery query) {
    queries.add(query);
    return this;
  }

  @Override
  public LindenQuery build() {
    LindenDisMaxQuery disMaxQuery = new LindenDisMaxQuery().setTie(tie).setQueries(queries);
    return new LindenQuery().setDisMaxQuery(disMaxQuery);
  }
}
