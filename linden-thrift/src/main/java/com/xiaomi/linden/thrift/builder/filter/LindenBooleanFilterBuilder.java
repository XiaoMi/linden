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

package com.xiaomi.linden.thrift.builder.filter;

import com.xiaomi.linden.thrift.common.LindenBooleanClause;
import com.xiaomi.linden.thrift.common.LindenBooleanFilter;
import com.xiaomi.linden.thrift.common.LindenBooleanSubFilter;
import com.xiaomi.linden.thrift.common.LindenFilter;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LindenBooleanFilterBuilder {
  private List<Map.Entry<LindenFilter, LindenBooleanClause>> filters = new ArrayList<>();

  public LindenBooleanFilterBuilder addFilter(LindenFilter filter, LindenBooleanClause clause) {
    filters.add(new AbstractMap.SimpleEntry<>(filter, clause));
    return this;
  }

  public LindenFilter build() {
    LindenBooleanFilter booleanFilter = new LindenBooleanFilter();
    for (Map.Entry<LindenFilter, LindenBooleanClause> filter : filters) {
      booleanFilter.addToFilters(new LindenBooleanSubFilter().setFilter(filter.getKey()).setClause(filter.getValue()));
    }
    return new LindenFilter().setBooleanFilter(booleanFilter);
  }
}
