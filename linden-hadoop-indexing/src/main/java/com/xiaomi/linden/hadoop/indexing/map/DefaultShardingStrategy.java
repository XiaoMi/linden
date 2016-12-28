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

package com.xiaomi.linden.hadoop.indexing.map;

import java.util.ArrayList;
import java.util.List;

import com.xiaomi.linden.thrift.common.IndexRouteParam;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;

public class DefaultShardingStrategy {
  public static int calculateShard(int shardCount, LindenIndexRequest indexRequest) {
    String id = indexRequest.getId();
    if (id == null) {
      return -1;
    }

    IndexRouteParam routeParam = indexRequest.getRouteParam();
    if (routeParam == null || !routeParam.isSetShardIds() || routeParam.getShardIds().isEmpty()) {
      return Math.abs(id.hashCode()) % shardCount;
    }

    List<Integer> ids = new ArrayList<>(routeParam.getShardIds());
    int pos = Math.abs(id.hashCode()) % routeParam.getShardIdsSize();
    return ids.get(pos);
  }
}
