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

package com.xiaomi.linden.core.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.zkclient.IZkChildListener;
import com.github.zkclient.ZkClient;

import com.xiaomi.linden.common.ZKClientFactory;
import com.xiaomi.linden.thrift.common.IndexRouteParam;

public class DefaultShardingStrategy extends ShardingStrategy {
  private AtomicInteger shardCount = new AtomicInteger();
  private ZkClient zkClient;

  public DefaultShardingStrategy(String zk, String path, int shardId) {
    super(shardId);
    zkClient = ZKClientFactory.getClient(zk);

    shardCount.set(1);
    List<String> shards = zkClient.getChildren(path);
    if (shards != null && shards.size() != 0) {
      shardCount.set(shards.size());
    }
    zkClient.subscribeChildChanges(path, new IZkChildListener() {
      @Override
      public void handleChildChange(String s, List<String> shards) throws Exception {
        if (shards != null) {
          shardCount.set(shards.size());
        }
      }
    });
  }

  @Override
  public boolean accept(String id) {
    return accept(id, null);
  }

  @Override
  public boolean accept(String id, IndexRouteParam routeParam) {
    return accept(id, routeParam, getShardId());
  }

  @Override
  public boolean accept(String id, IndexRouteParam routeParam, int shardId) {
    // id is null means this is an operation command, all shard should accept it
    if (id == null) {
      return true;
    }

    if (routeParam == null || !routeParam.isSetShardIds() || routeParam.getShardIds().isEmpty()) {
      return Math.abs(id.hashCode() % shardCount.get()) == shardId;
    }

    if (routeParam.getShardIds().contains(shardId)) {
      List<Integer> ids = new ArrayList<>(routeParam.getShardIds());
      int pos = Math.abs(id.hashCode() % routeParam.getShardIdsSize());
      return ids.get(pos) == shardId;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    zkClient.close();
  }
}
