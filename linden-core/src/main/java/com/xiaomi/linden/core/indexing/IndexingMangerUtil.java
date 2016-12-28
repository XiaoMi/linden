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

import kafka.message.MessageAndMetadata;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.LindenConfigBuilder;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.plugin.LindenPluginManager;
import com.xiaomi.linden.plugin.gateway.DataProvider;
import com.xiaomi.linden.plugin.gateway.LindenGateway;

public class IndexingMangerUtil {

  public static IndexingManager initIndexingManger(LindenConfig config, ShardingStrategy shardingStrategy,
                                                   LindenCore lindenCore)
      throws IOException {
    IndexingManager indexingManager = null;
    LindenPluginManager pluginManager = config.getPluginManager();
    LindenGateway gateway = pluginManager.getInstance(LindenConfigBuilder.GATEWAY, LindenGateway.class);
    if (gateway != null) {
      DataProvider dataProvider = gateway.buildDataProvider();
      if (dataProvider != null) {
        if (dataProvider.getType() == String.class) {
          indexingManager = new StringIndexingManager(config, shardingStrategy, lindenCore, dataProvider);
        } else if (dataProvider.getType() == MessageAndMetadata.class) {
          indexingManager = new KafkaIndexingManager(config, shardingStrategy, lindenCore, dataProvider);
        } else {
          throw new IOException("Unsupported data provider type");
        }
        indexingManager.start();
      }
    }
    return indexingManager;
  }

}
