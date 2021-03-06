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

import com.xiaomi.linden.thrift.common.LindenQuery;
import com.xiaomi.linden.thrift.common.LindenQueryStringQuery;
import com.xiaomi.linden.thrift.common.Operator;

public class LindenQueryStringQueryBuilder extends LindenQueryBuilder {
  private String query;
  private Operator operator = Operator.OR;
  private boolean disableCoord = true;

  public LindenQueryStringQueryBuilder setQuery(String query) {
    this.query = query;
    return this;
  }

  public LindenQueryStringQueryBuilder setOperator(Operator operator) {
    this.operator = operator;
    return this;
  }

  public LindenQueryStringQueryBuilder setDisableCoord(boolean disableCoord) {
    this.disableCoord = disableCoord;
    return this;
  }

  @Override
  public LindenQuery build() {
    LindenQueryStringQuery stringQuery = new LindenQueryStringQuery()
        .setQuery(query).setOperator(operator).setDisableCoord(disableCoord);
    return new LindenQuery().setQueryString(stringQuery);
  }
}
