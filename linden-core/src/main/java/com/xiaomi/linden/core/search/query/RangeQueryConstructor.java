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

import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenQuery;
import com.xiaomi.linden.thrift.common.LindenRange;
import com.xiaomi.linden.thrift.common.LindenRangeQuery;
import com.xiaomi.linden.thrift.common.LindenType;

public class RangeQueryConstructor extends QueryConstructor {

  @Override
  protected Query construct(LindenQuery lindenQuery, LindenConfig config) throws IOException {
    if (lindenQuery.isSetRangeQuery()) {
      LindenRangeQuery lindenRangeQuery = lindenQuery.getRangeQuery();
      LindenRange range = lindenRangeQuery.getRange();
      String fieldName = range.getField();
      LindenType type = range.getType();
      String start = range.getStartValue();
      String end = range.getEndValue();
      boolean startClose = range.isStartClosed();
      boolean endClose = range.isEndClosed();
      Query query = null;
      switch (type) {
        case STRING:
        case FACET:
          query = new TermRangeQuery(fieldName, bytesRefVal(start), bytesRefVal(end), startClose, endClose);
          break;
        case INTEGER:
          query = NumericRangeQuery.newIntRange(fieldName, intVal(start), intVal(end), startClose, endClose);
          break;
        case LONG:
          query = NumericRangeQuery.newLongRange(fieldName, longVal(start), longVal(end), startClose, endClose);
          break;
        case DOUBLE:
          query = NumericRangeQuery.newDoubleRange(fieldName, doubleVal(start), doubleVal(end), startClose, endClose);
          break;
        case FLOAT:
          query = NumericRangeQuery.newFloatRange(fieldName, floatVal(start), floatVal(end), startClose, endClose);
          break;
      }
      return query;
    }
    // todo throw exception.
    return null;
  }
}
