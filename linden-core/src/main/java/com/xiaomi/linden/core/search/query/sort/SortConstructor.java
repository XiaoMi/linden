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

package com.xiaomi.linden.core.search.query.sort;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenSort;
import com.xiaomi.linden.thrift.common.LindenSortField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import java.io.IOException;

public class SortConstructor {

  public static Sort constructSort(LindenSearchRequest request, IndexSearcher indexSearcher, LindenConfig config) throws IOException {
    if (!request.isSetSort())
      return null;

    LindenSort lindenSort = request.getSort();
    SortField[] sortFields = new SortField[lindenSort.getFieldsSize()];
    for (int i = 0; i < lindenSort.getFieldsSize(); ++i) {
      LindenSortField field = lindenSort.getFields().get(i);
      SortField.Type type = SortField.Type.STRING;
      boolean isReverse = field.isReverse();
      switch (field.getType()) {
        case STRING:
          type = SortField.Type.STRING;
          break;
        case DOUBLE:
          type = SortField.Type.DOUBLE;
          break;
        case FLOAT:
          type = SortField.Type.FLOAT;
          break;
        case INTEGER:
          type = SortField.Type.INT;
          break;
        case LONG:
          type = SortField.Type.LONG;
          break;
        case SCORE:
          type = SortField.Type.SCORE;
          isReverse = !isReverse;
          break;
        case DISTANCE:
          if (request.isSetSpatialParam()) {
            Point point = SpatialContext.GEO.makePoint(
                request.getSpatialParam().getCoordinate().getLongitude(),
                request.getSpatialParam().getCoordinate().getLatitude());
            ValueSource valueSource = config.getSpatialStrategy().makeDistanceValueSource(point, DistanceUtils.DEG_TO_KM);
            sortFields[i] = valueSource.getSortField(false).rewrite(indexSearcher);
          }
          continue;
      }
      sortFields[i] = new SortField(field.getName(), type, isReverse);
    }
    return new Sort(sortFields);
  }
}
