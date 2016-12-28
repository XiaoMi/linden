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

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;

import static com.xiaomi.linden.core.search.query.QueryConstructor.bytesRefVal;
import static com.xiaomi.linden.core.search.query.QueryConstructor.doubleVal;
import static com.xiaomi.linden.core.search.query.QueryConstructor.floatVal;
import static com.xiaomi.linden.core.search.query.QueryConstructor.intVal;
import static com.xiaomi.linden.core.search.query.QueryConstructor.longVal;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenType;

// not thread safe
public class LindenQueryParser extends QueryParser {
  private final LindenConfig config;

  public LindenQueryParser(LindenConfig config) throws IOException {
    super("", config.getSearchAnalyzerInstance());
    this.config = config;
  }

  @Override
  protected Query getRangeQuery(String field, String min, String max,
                                boolean startInclusive, boolean endInclusive) {
    LindenFieldSchema fieldSchema = config.getFieldSchema(field);
    String name = fieldSchema.getName();
    LindenType indexFieldType = fieldSchema.getType();
    Query query = null;
    switch (indexFieldType) {
      case STRING:
        query = new TermRangeQuery(name, bytesRefVal(min), bytesRefVal(max), startInclusive, endInclusive);
        break;
      case INTEGER:
        query = NumericRangeQuery.newIntRange(name, intVal(min), intVal(max), startInclusive, endInclusive);
        break;
      case LONG:
        query = NumericRangeQuery.newLongRange(name, longVal(min), longVal(max), startInclusive, endInclusive);
        break;
      case DOUBLE:
        query = NumericRangeQuery.newDoubleRange(name, doubleVal(min), doubleVal(max), startInclusive, endInclusive);
        break;
      case FLOAT:
        query = NumericRangeQuery.newFloatRange(name, floatVal(min), floatVal(max), startInclusive, endInclusive);
        break;
      default:
        break;
    }
    return query;
  }
}
