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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.plugin.gateway.DataProvider;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;

public class KafkaIndexingManager extends IndexingManager<MessageAndMetadata<byte[], byte[]>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIndexingManager.class);

  public KafkaIndexingManager(final LindenConfig lindenConfig, ShardingStrategy shardingStrategy,
                              LindenCore lindenCore, DataProvider<MessageAndMetadata<byte[], byte[]>> provider) {
    super(provider, lindenConfig, lindenCore, new Function<MessageAndMetadata<byte[], byte[]>, LindenIndexRequest>() {
      @Override
      public LindenIndexRequest apply(MessageAndMetadata<byte[], byte[]> messageAndMetadata) {
        LindenIndexRequest indexRequest = null;
        long offset = messageAndMetadata.offset();
        long partition = messageAndMetadata.partition();
        String message = new String(messageAndMetadata.message());
        try {
          indexRequest = LindenIndexRequestParser.parse(lindenConfig.getSchema(), message);
          LOGGER.info("Parse index request : id={}, rout={}, type={}, content({}/{})={}", indexRequest.getId(),
                      indexRequest.getRouteParam(), indexRequest.getType(), partition, offset, message);
        } catch (IOException e) {
          LOGGER.error("Parse index request failed : {} - {}", message, Throwables.getStackTraceAsString(e));
        }
        return indexRequest;
      }
    }, shardingStrategy);
  }
}
