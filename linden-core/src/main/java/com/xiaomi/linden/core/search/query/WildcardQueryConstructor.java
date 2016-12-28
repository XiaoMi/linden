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

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenQuery;
import com.xiaomi.linden.thrift.common.LindenWildcardQuery;

public class WildcardQueryConstructor extends QueryConstructor {
  @Override
  protected Query construct(LindenQuery lindenQuery, LindenConfig config) throws IOException {
    if (lindenQuery.isSetWildcardQuery()) {
      LindenWildcardQuery lindenWildcardQuery = lindenQuery.getWildcardQuery();
      return new WildcardQuery(new Term(lindenWildcardQuery.getField(), lindenWildcardQuery.getQuery()));
    }
    return null;
  }
}
