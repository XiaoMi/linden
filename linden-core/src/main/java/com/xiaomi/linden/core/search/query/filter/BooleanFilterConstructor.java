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

package com.xiaomi.linden.core.search.query.filter;

import java.util.List;

import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Filter;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenBooleanSubFilter;
import com.xiaomi.linden.thrift.common.LindenFilter;

public class BooleanFilterConstructor extends FilterConstructor {

  @Override
  protected Filter construct(LindenFilter lindenFilter, LindenConfig config) throws Exception {
    List<LindenBooleanSubFilter> booleanSubFilterList = lindenFilter.getBooleanFilter().getFilters();
    BooleanFilter booleanFilter = new BooleanFilter();
    for (LindenBooleanSubFilter booleanSubFilter : booleanSubFilterList) {
      LindenFilter subFilter = booleanSubFilter.getFilter();
      switch (booleanSubFilter.clause) {
        case MUST:
          booleanFilter.add(FilterConstructor.constructFilter(subFilter, config), BooleanClause.Occur.MUST);
          continue;
        case SHOULD:
          booleanFilter.add(FilterConstructor.constructFilter(subFilter, config), BooleanClause.Occur.SHOULD);
          continue;
        case MUST_NOT:
          booleanFilter.add(FilterConstructor.constructFilter(subFilter, config), BooleanClause.Occur.MUST_NOT);
      }
    }
    return booleanFilter;
  }
}
