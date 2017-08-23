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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryWrapperFilter;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.search.query.QueryConstructor;
import com.xiaomi.linden.thrift.common.LindenFilter;

public class QueryFilterConstructor extends FilterConstructor {

  @Override
  protected Filter construct(LindenFilter lindenFilter, LindenConfig config) throws Exception {
    QueryWrapperFilter filter = new QueryWrapperFilter(
        QueryConstructor.constructQuery(lindenFilter.getQueryFilter().getQuery(), config));
    return filter;
  }
}
