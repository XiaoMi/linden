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

package com.xiaomi.linden.thrift.builder.filter;

import com.xiaomi.linden.thrift.common.LindenFilter;
import com.xiaomi.linden.thrift.common.LindenRange;
import com.xiaomi.linden.thrift.common.LindenRangeFilter;
import com.xiaomi.linden.thrift.common.LindenType;

public class LindenRangeFilterBuilder {

  public static LindenFilter buildRangeFilter(String field, LindenType type, String startValue, String endValue,
                                              boolean isStartClosed, boolean isEndClosed) {
    LindenRangeFilter rangeFilter = new LindenRangeFilter(new LindenRange(field, type, isStartClosed, isEndClosed));
    if (startValue != null) {
      rangeFilter.getRange().setStartValue(startValue);
    }
    if (endValue != null) {
      rangeFilter.getRange().setEndValue(endValue);
    }
    return new LindenFilter().setRangeFilter(rangeFilter);
  }
}
