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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;

import com.xiaomi.linden.common.schema.LindenSchemaConf;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenFilter;
import com.xiaomi.linden.thrift.common.LindenSpatialFilter;

public class SpatialFilterConstructor extends FilterConstructor {

  private static SpatialContext spatialContext = SpatialContext.GEO;
  private static int maxLevels = 11;
  private static SpatialPrefixTree grid = new GeohashPrefixTree(spatialContext, maxLevels);
  private static SpatialStrategy spatialStrategy = new RecursivePrefixTreeStrategy(grid, LindenSchemaConf.GEO_FIELD);

  @Override
  protected Filter construct(LindenFilter lindenFilter, LindenConfig config) throws IOException {
    LindenSpatialFilter spatialFilter = lindenFilter.getSpatialFilter();
    SpatialArgs spatialArgs = new SpatialArgs(
        SpatialOperation.Intersects,
        spatialContext.makeCircle(
            spatialFilter.getSpatialParam().coordinate.getLongitude(),
            spatialFilter.getSpatialParam().coordinate.getLatitude(),
            DistanceUtils
                .dist2Degrees(spatialFilter.getSpatialParam().getDistanceRange(), DistanceUtils.EARTH_MEAN_RADIUS_KM)));
    return spatialStrategy.makeFilter(spatialArgs);
  }
}
