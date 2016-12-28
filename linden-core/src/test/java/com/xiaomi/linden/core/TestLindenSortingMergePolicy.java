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
    lindenConfig.setMergePolicy("com.xiaomi.linden.lucene.merge.SortingMergePolicyFactory");
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
      lindenCore.merge(1);
      lindenCore.commit();
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
  public void testBasic() throws IOException {
    String bql = "select * from linden source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(5, result.getTotalHits());
    Assert.assertEquals("4", result.getHits().get(0).getId());
  }
}
