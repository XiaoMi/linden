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

import java.io.IOException;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.TermRangeFilter;

import static com.xiaomi.linden.core.search.query.QueryConstructor.bytesRefVal;
import static com.xiaomi.linden.core.search.query.QueryConstructor.doubleVal;
import static com.xiaomi.linden.core.search.query.QueryConstructor.floatVal;
import static com.xiaomi.linden.core.search.query.QueryConstructor.intVal;
import static com.xiaomi.linden.core.search.query.QueryConstructor.longVal;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenFilter;
import com.xiaomi.linden.thrift.common.LindenRange;
import com.xiaomi.linden.thrift.common.LindenRangeFilter;
import com.xiaomi.linden.thrift.common.LindenType;

public class RangeFilterConstructor extends FilterConstructor {

  @Override
  protected Filter construct(LindenFilter lindenFilter, LindenConfig config) throws IOException {
    LindenRangeFilter lindenRangeFilter = lindenFilter.getRangeFilter();
    LindenRange range = lindenRangeFilter.getRange();
    LindenType type = range.getType();
    String start = range.getStartValue();
    String end = range.getEndValue();
    String fieldName = range.getField();
    boolean startClose = range.isStartClosed();
    boolean endClose = range.isEndClosed();

    Filter filter = null;
    switch (type) {
      case STRING:
      case FACET:
        filter = new TermRangeFilter(fieldName, bytesRefVal(start), bytesRefVal(end), startClose, endClose);
        break;
      case INTEGER:
        filter = NumericRangeFilter.newIntRange(fieldName, intVal(start), intVal(end), startClose, endClose);
        break;
      case LONG:
        filter = NumericRangeFilter.newLongRange(fieldName, longVal(start), longVal(end), startClose, endClose);
        break;
      case DOUBLE:
        filter = NumericRangeFilter.newDoubleRange(fieldName, doubleVal(start), doubleVal(end), startClose, endClose);
        break;
      case FLOAT:
        filter = NumericRangeFilter.newFloatRange(fieldName, floatVal(start), floatVal(end), startClose, endClose);
        break;
    }
    return filter;
  }
}
