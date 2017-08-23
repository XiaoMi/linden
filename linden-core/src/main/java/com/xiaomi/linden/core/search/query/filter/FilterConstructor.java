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

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenFilter;

public abstract class FilterConstructor {

  private static final FilterConstructor TERM_FILTER_CONSTRUCTOR = new TermFilterConstructor();
  private static final FilterConstructor RANGE_FILTER_CONSTRUCTOR = new RangeFilterConstructor();
  private static final FilterConstructor QUERY_FILTER_CONSTRUCTOR = new QueryFilterConstructor();
  private static final FilterConstructor BOOLEAN_FILTER_CONSTRUCTOR = new BooleanFilterConstructor();
  private static final FilterConstructor SPATIAL_FILTER_CONSTRUCTOR = new SpatialFilterConstructor();
  private static final FilterConstructor NOT_NULL_FIELD_FILTER_CONSTRUCTOR = new NotNullFieldFilterConstructor();

  public static Filter constructFilter(LindenFilter lindenFilter, LindenConfig config) throws Exception {
    if (lindenFilter == null) {
      return null;
    }

    if (lindenFilter.isSetTermFilter()) {
      return TERM_FILTER_CONSTRUCTOR.construct(lindenFilter, config);
    } else if (lindenFilter.isSetRangeFilter()) {
      return RANGE_FILTER_CONSTRUCTOR.construct(lindenFilter, config);
    } else if (lindenFilter.isSetQueryFilter()) {
      return QUERY_FILTER_CONSTRUCTOR.construct(lindenFilter, config);
    } else if (lindenFilter.isSetBooleanFilter()) {
      return BOOLEAN_FILTER_CONSTRUCTOR.construct(lindenFilter, config);
    } else if (lindenFilter.isSetSpatialFilter()) {
      return SPATIAL_FILTER_CONSTRUCTOR.construct(lindenFilter, config);
    } else if (lindenFilter.isSetNotNullFieldFilter()) {
      return NOT_NULL_FIELD_FILTER_CONSTRUCTOR.construct(lindenFilter, config);
    }
    return null;
  }

  protected abstract Filter construct(LindenFilter lindenFilter, LindenConfig config) throws Exception;
}
