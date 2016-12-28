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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.search.query.model.LindenScoreQuery;
import com.xiaomi.linden.thrift.common.LindenQuery;

abstract public class QueryConstructor {

  private static final QueryConstructor TERM_QUERY_CONSTRUCTOR = new TermQueryConstructor();
  private static final QueryConstructor BOOLEAN_QUERY_CONSTRUCTOR = new BooleanQueryConstructor();
  private static final QueryConstructor FLEXIBLE_QUERY_CONSTRUCTOR = new FlexibleQueryConstructor();
  private static final QueryConstructor MATCH_ALL_QUERY_CONSTRUCTOR = new MatchAllQueryConstructor();
  private static final QueryConstructor QUERY_STRING_QUERY_CONSTRUCTOR = new QueryStringQueryConstructor();
  private static final QueryConstructor RANGE_QUERY_CONSTRUCTOR = new RangeQueryConstructor();
  private static final QueryConstructor DIS_MAX_QUERY_CONSTRUCTOR = new DisMaxQueryConstructor();
  private static final QueryConstructor FILTERED_QUERY_CONSTRUCTOR = new FilteredQueryConstructor();
  private static final QueryConstructor WILDCARD_QUERY_CONSTRUCTOR = new WildcardQueryConstructor();

  public static Query constructQuery(LindenQuery lindenQuery, LindenConfig config) throws Exception {
    if (lindenQuery == null) {
      return new MatchAllDocsQuery();
    }

    Query query;
    if (lindenQuery.isSetTermQuery()) {
      query = TERM_QUERY_CONSTRUCTOR.construct(lindenQuery, config);
    } else if (lindenQuery.isSetBooleanQuery()) {
      query = BOOLEAN_QUERY_CONSTRUCTOR.construct(lindenQuery, config);
    } else if (lindenQuery.isSetFlexQuery()) {
      query = FLEXIBLE_QUERY_CONSTRUCTOR.construct(lindenQuery, config);
    } else if (lindenQuery.isSetMatchAllQuery()) {
      query = MATCH_ALL_QUERY_CONSTRUCTOR.construct(lindenQuery, config);
    } else if (lindenQuery.isSetRangeQuery()) {
      query = RANGE_QUERY_CONSTRUCTOR.construct(lindenQuery, config);
    } else if (lindenQuery.isSetQueryString()) {
      query = QUERY_STRING_QUERY_CONSTRUCTOR.construct(lindenQuery, config);
    } else if (lindenQuery.isSetDisMaxQuery()) {
      query = DIS_MAX_QUERY_CONSTRUCTOR.construct(lindenQuery, config);
    } else if (lindenQuery.isSetFilteredQuery()) {
      query = FILTERED_QUERY_CONSTRUCTOR.construct(lindenQuery, config);
    } else if (lindenQuery.isSetWildcardQuery()) {
      query = WILDCARD_QUERY_CONSTRUCTOR.construct(lindenQuery, config);
    } else {
      throw new IOException("Need query detail.");
    }

    if (lindenQuery.isSetBoost()) {
      query.setBoost(query.getBoost() * (float) lindenQuery.getBoost());
    }

    if (lindenQuery.isSetScoreModel()) {
      return new LindenScoreQuery(query, lindenQuery.getScoreModel(), config);
    }
    return query;
  }

  protected abstract Query construct(LindenQuery lindenQuery, LindenConfig config) throws Exception;


  public static Integer intVal(String val) {
    return val == null ? null : Integer.valueOf(val);
  }

  public static Float floatVal(String val) {
    return val == null ? null : Float.valueOf(val);
  }

  public static Long longVal(String val) {
    return val == null ? null : Long.valueOf(val);
  }

  public static Double doubleVal(String val) {
    return val == null ? null : Double.valueOf(val);
  }

  public static BytesRef bytesRefVal(String val) {
    return val == null ? null : new BytesRef(val);
  }
}
