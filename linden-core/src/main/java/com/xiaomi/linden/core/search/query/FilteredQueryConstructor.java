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

package com.xiaomi.linden.core.search.query;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.search.query.filter.FilterConstructor;
import com.xiaomi.linden.thrift.common.LindenQuery;


public class FilteredQueryConstructor extends QueryConstructor {

  @Override
  protected Query construct(LindenQuery lindenQuery, LindenConfig config) throws Exception {
    Query query = QueryConstructor.constructQuery(lindenQuery.getFilteredQuery().getLindenQuery(), config);
    Filter filter = FilterConstructor.constructFilter(lindenQuery.getFilteredQuery().getLindenFilter(), config);
    return new FilteredQuery(query, filter);
  }
}
