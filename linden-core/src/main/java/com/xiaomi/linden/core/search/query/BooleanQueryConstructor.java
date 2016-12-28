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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenBooleanQuery;
import com.xiaomi.linden.thrift.common.LindenBooleanSubQuery;
import com.xiaomi.linden.thrift.common.LindenQuery;

public class BooleanQueryConstructor extends QueryConstructor {

  @Override
  protected Query construct(LindenQuery lindenQuery, LindenConfig config) throws Exception {
    if (lindenQuery.isSetBooleanQuery()) {
      LindenBooleanQuery ldBoolQuery = lindenQuery.getBooleanQuery();
      BooleanQuery booleanQuery = new BooleanQuery(ldBoolQuery.isDisableCoord());
      for (LindenBooleanSubQuery subQuery : ldBoolQuery.getQueries()) {
        Query query = constructQuery(subQuery.getQuery(), config);
        switch (subQuery.getClause()) {
          case SHOULD:
            booleanQuery.add(query, BooleanClause.Occur.SHOULD);
            break;
          case MUST:
            booleanQuery.add(query, BooleanClause.Occur.MUST);
            break;
          case MUST_NOT:
            booleanQuery.add(query, BooleanClause.Occur.MUST_NOT);
            break;
          default:
            throw new IOException("This should never happen, boolean clause is " + subQuery.getClause());
        }
      }
      return booleanQuery;
    }
    return null;
  }
}
