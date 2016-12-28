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

import com.google.common.base.Throwables;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenQuery;
import com.xiaomi.linden.thrift.common.LindenQueryStringQuery;
import com.xiaomi.linden.thrift.common.Operator;

public class QueryStringQueryConstructor extends QueryConstructor {

  @Override
  protected Query construct(LindenQuery lindenQuery, LindenConfig config) throws IOException {
    LindenQueryStringQuery stringQuery = lindenQuery.getQueryString();
    QueryParser.Operator op = QueryParser.Operator.OR;
    if (stringQuery.isSetOperator() && stringQuery.getOperator() == Operator.AND) {
      op = QueryParser.Operator.AND;
    }
    QueryParser queryParser = new LindenQueryParser(config);
    String content = stringQuery.getQuery();
    try {
      queryParser.setDefaultOperator(op);
      Query query = queryParser.parse(content);
      // disable coord
      if (query instanceof BooleanQuery) {
        BooleanQuery bQuery = (BooleanQuery) query;
        BooleanQuery booleanQuery = new BooleanQuery(stringQuery.isDisableCoord());
        BooleanClause[] clauses = bQuery.getClauses();
        for (BooleanClause clause : clauses) {
          booleanQuery.add(clause);
        }
        booleanQuery.setBoost(query.getBoost());
        query = booleanQuery;
      }
      return query;
    } catch (ParseException e) {
      throw new IOException(Throwables.getStackTraceAsString(e));
    }
  }
}
