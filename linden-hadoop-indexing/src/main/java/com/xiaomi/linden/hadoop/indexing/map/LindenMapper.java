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

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.Term;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.indexing.LindenIndexRequestParser;
import com.xiaomi.linden.core.search.LindenDocParser;
import com.xiaomi.linden.hadoop.indexing.keyvalueformat.IntermediateForm;
import com.xiaomi.linden.hadoop.indexing.keyvalueformat.Shard;
import com.xiaomi.linden.hadoop.indexing.util.LindenConfigBuilder;
import com.xiaomi.linden.thrift.common.IndexRequestType;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;

public class LindenMapper extends Mapper<Object, Object, Shard, IntermediateForm> {

  private final static Logger logger = Logger.getLogger(LindenMapper.class);
  private Configuration conf;
  private Shard[] shards;

  private LindenConfig lindenConfig;
  private FacetsConfig facetsConfig;

  @Override
  public void map(Object key, Object value, Context context)
      throws IOException, InterruptedException {
    LindenIndexRequest indexRequest;
    try {
      indexRequest = LindenIndexRequestParser.parse(lindenConfig.getSchema(), value.toString());
    } catch (Exception e) {
      ExceptionUtils.printRootCauseStackTrace(e);
      throw new IllegalStateException("LindenIndexRequestParser parsing error", e);
    }

    if (indexRequest.getType() != IndexRequestType.INDEX) {
      throw new IllegalStateException("Index request type error");
    }

    Document doc = LindenDocParser.parse(indexRequest.getDoc(), lindenConfig);
    // now we have uid and lucene Doc;
    IntermediateForm form = new IntermediateForm();
    form.process(new Term(lindenConfig.getSchema().getId(), indexRequest.getDoc().getId()), doc,
                 lindenConfig.getIndexAnalyzerInstance(), facetsConfig);
    form.closeWriter();

    int chosenShard = DefaultShardingStrategy.calculateShard(shards.length, indexRequest);
    if (chosenShard >= 0) {
      // insert into one shard
      context.write(shards[chosenShard], form);
    } else {
      logger.error("calculateShard failed for " + value.toString());
      return;
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    conf = context.getConfiguration();
    shards = Shard.getIndexShards(conf);
    lindenConfig = LindenConfigBuilder.build();
    facetsConfig = lindenConfig.createFacetsConfig();
  }
}
