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

import com.google.common.collect.Lists;
import com.xiaomi.linden.thrift.common.LindenBooleanClause;
import com.xiaomi.linden.thrift.common.LindenBooleanQuery;
import com.xiaomi.linden.thrift.common.LindenBooleanSubQuery;
import com.xiaomi.linden.thrift.common.LindenQuery;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

public class LindenBooleanQueryBuilder extends LindenQueryBuilder {
  private boolean disableCoord = false;
  private List<Map.Entry<LindenQuery, LindenBooleanClause>> queries = Lists.newArrayList();
  private double boost = 1.0;

  public LindenBooleanQueryBuilder addQuery(LindenQuery query, LindenBooleanClause clause) {
    queries.add(new AbstractMap.SimpleEntry<>(query, clause));
    return this;
  }

  public LindenBooleanQueryBuilder setBoost(double boost) {
    this.boost = boost;
    return this;
  }

  public LindenBooleanQueryBuilder setDisableCoord(boolean disableCoord) {
    this.disableCoord = disableCoord;
    return this;
  }

  @Override
  public LindenQuery build() {
    LindenBooleanQuery booleanQuery = new LindenBooleanQuery();
    for (Map.Entry<LindenQuery, LindenBooleanClause> query : queries) {
      booleanQuery.addToQueries(new LindenBooleanSubQuery().setQuery(query.getKey()).setClause(query.getValue()));
    }
    booleanQuery.setDisableCoord(disableCoord);
    return new LindenQuery().setBooleanQuery(booleanQuery).setBoost(boost);
  }

}
