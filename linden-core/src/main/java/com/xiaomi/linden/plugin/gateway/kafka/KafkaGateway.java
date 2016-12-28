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

package com.xiaomi.linden.plugin.gateway.kafka;

import com.google.common.base.Preconditions;
import com.xiaomi.linden.plugin.gateway.DataProvider;
import com.xiaomi.linden.plugin.gateway.LindenGateway;

import java.io.IOException;

public class KafkaGateway extends LindenGateway {
  private static final String ZOOKEEPER = "zookeeper";
  private static final String GROUP = "group";
  private static final String TOPIC = "topic";
  @Override
  public DataProvider buildDataProvider() throws IOException {
    Preconditions.checkNotNull(config.get(ZOOKEEPER));
    Preconditions.checkNotNull(config.get(GROUP));
    Preconditions.checkNotNull(config.get(TOPIC));
    return new KafkaDataProvider(config.get(ZOOKEEPER), config.get(TOPIC), config.get(GROUP));
  }
}
