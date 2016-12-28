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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import com.xiaomi.linden.plugin.gateway.DataProvider;

public class KafkaDataProvider extends DataProvider<MessageAndMetadata<byte[], byte[]>> {

  private final ConsumerConnector consumer;
  private final ConsumerIterator<byte[], byte[]> iter;

  public KafkaDataProvider(String zookeeper, String topic, String groupId) {
    super(MessageAndMetadata.class);
    Properties props = new Properties();
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);
    props.put("zookeeper.session.timeout.ms", "30000");
    props.put("auto.commit.interval.ms", "1000");
    props.put("fetch.message.max.bytes", "4194304");
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);

    iter = stream.iterator();
  }

  @Override
  public MessageAndMetadata<byte[], byte[]> next() {
    if (iter.hasNext()) {
      return iter.next();
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    consumer.shutdown();
  }
}
