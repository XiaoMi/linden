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

package com.xiaomi.linden.core;

import java.io.IOException;

import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.core.indexing.LindenIndexRequestParser;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.core.search.LindenCoreImpl;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenType;

public class TestLindenSortingMergePolicy {

  public LindenCore lindenCore;
  public LindenConfig lindenConfig;
  public BQLCompiler bqlCompiler;

  public TestLindenSortingMergePolicy() throws Exception {
    lindenConfig = new LindenConfig().setIndexType(LindenConfig.IndexType.RAM).setClusterUrl("127.0.0.1:2181/test");
    lindenConfig.putToProperties("merge.policy.class",
                                 "com.xiaomi.linden.lucene.merge.SortingMergePolicyFactory");
    lindenConfig.putToProperties("merge.policy.sort.field", "rank");
    lindenConfig.putToProperties("merge.policy.sort.field.type", "float");
    lindenConfig.putToProperties("merge.policy.sort.desc", "true");
    lindenConfig.setPluginPath("./");
    ZooKeeperService.start();

    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(new LindenFieldSchema().setName("title").setIndexed(true).setStored(true).setTokenized(true));
    schema.addToFields(new LindenFieldSchema().setName("rank").setType(LindenType.FLOAT).setIndexed(true).setStored(true));
    lindenConfig.setSchema(schema);
    bqlCompiler = new BQLCompiler(schema);
    lindenCore = new LindenCoreImpl(lindenConfig);

    init();
  }

  public void init() throws Exception {
    try {
      handleRequest("{\"id\":1, \"title\": \"lucene 1\",\"rank\": 1.2}");
      handleRequest("{\"id\":2, \"title\": \"lucene 2\",\"rank\": 1.1}");
      handleRequest("{\"id\":3, \"title\": \"lucene 3\",\"rank\": 1.3}");
      handleRequest("{\"id\":4, \"title\": \"lucene 4\",\"rank\": 1.5}");
      handleRequest("{\"id\":5, \"title\": \"lucene 5\",\"rank\": 1.0}");
      lindenCore.commit();

      handleRequest("{\"id\":6, \"title\": \"lucene 6\",\"rank\": 2.6}");
      handleRequest("{\"id\":7, \"title\": \"lucene 7\",\"rank\": 4.7}");
      handleRequest("{\"id\":8, \"title\": \"lucene 8\",\"rank\": 1.9}");
      handleRequest("{\"id\":9, \"title\": \"lucene 9\",\"rank\": 2.1}");
      handleRequest("{\"id\":10, \"title\": \"lucene 10\",\"rank\": 2.7}");
      lindenCore.commit();

      handleRequest("{\"id\":11, \"title\": \"lucene 11\",\"rank\": 1.6}");
      handleRequest("{\"id\":12, \"title\": \"lucene 12\",\"rank\": 2.7}");
      handleRequest("{\"id\":13, \"title\": \"lucene 13\",\"rank\": 0.9}");
      handleRequest("{\"id\":14, \"title\": \"lucene 14\",\"rank\": 4.1}");
      handleRequest("{\"id\":15, \"title\": \"lucene 15\",\"rank\": 3.7}");
      lindenCore.commit();
      // the 1st and the 2nd segments are merged to a new sorted segment, the 3rd is not sorted
      lindenCore.mergeIndex(2);
      lindenCore.refresh();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void handleRequest(String content) throws IOException {
    LindenIndexRequest indexRequest = LindenIndexRequestParser.parse(lindenConfig.getSchema(), content);
    lindenCore.index(indexRequest);
  }

  @Test
  public void testEarlyTermination() throws IOException {
    String bql = "select * from linden by query is \"title:(lucene 2)\" in top 3 limit 10 source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);

    Assert.assertEquals(8, result.getHits().size());
    Assert.assertEquals("7", result.getHits().get(0).getId());

    bql = "select * from linden by query is \"title:(lucene 13)\" in top 3 limit 10 source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(8, result.getHits().size());
    Assert.assertEquals("13", result.getHits().get(0).getId());

    bql = "select * from linden by query is \"title:(lucene 13)\" in top 2 order by rank limit 10 source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(7, result.getHits().size());
    Assert.assertEquals("7", result.getHits().get(0).getId());
  }
}
