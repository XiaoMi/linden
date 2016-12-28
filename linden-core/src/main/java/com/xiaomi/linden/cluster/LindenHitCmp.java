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

package com.xiaomi.linden.cluster;

import java.util.Comparator;
import java.util.List;

import com.xiaomi.linden.thrift.common.LindenHit;
import com.xiaomi.linden.thrift.common.LindenSortField;

/**
 * compare for LindenHit
 */
public class LindenHitCmp implements Comparator<LindenHit> {
  private List<LindenSortField> sortFields;
  LindenHitCmp(List<LindenSortField> sortFields) {
    this.sortFields = sortFields;
  }

  /**
   * natural order is ascending:a negative integer, zero, or a positive integer
   * as the first argument is less than, equal to, or greater than the second.
   * if reverse natural order, the returned value * -1
   * @param o1
   * @param o2
   * @return
   */
  @Override
  public int compare(LindenHit o1, LindenHit o2) {
    int cmp = 0;
    if (null == sortFields || sortFields.isEmpty()) {
      cmp = Double.compare(o1.getScore(), o2.getScore());
      return cmp * -1;
    }
    for (LindenSortField field : sortFields) {
      int isReverse = field.isReverse() ? -1 : 1;
      String v1 = o1.getFields().get(field.getName());
      String v2 = o2.getFields().get(field.getName());
      switch (field.getType()) {
        case INTEGER:
          cmp = Integer.compare(Integer.valueOf(v1), Integer.valueOf(v2));
          break;
        case LONG:
          cmp = Long.compare(Long.valueOf(v1), Long.valueOf(v2));
          break;
        case FLOAT:
          cmp = Float.compare(Float.valueOf(v1), Float.valueOf(v2));
          break;
        case DOUBLE:
          cmp = Double.compare(Double.valueOf(v1), Double.valueOf(v2));
          break;
        case STRING:
          cmp = v1.compareTo(v2);
          break;
        //score and distance: default is reverse,
        // but score is from high to low, distance is from near to far
        case SCORE:
          cmp = Double.compare(o1.getScore(), o2.getScore());
          break;
        case DISTANCE:
          cmp = Double.compare(o1.getDistance(), o2.getDistance());
          //distance is from near to far
          cmp = cmp * -1;
          break;
      }
      if (cmp != 0) {
        return isReverse * cmp;
      }
    }
    return 0;
  }
}
