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

package com.xiaomi.linden.lucene.analyzer;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.TestLindenCoreBase;
import com.xiaomi.linden.thrift.common.*;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestLindenMMSeg4jAnalyzer extends TestLindenCoreBase {

  public TestLindenMMSeg4jAnalyzer() throws Exception {
    try {
      handleRequest("{\"id\":1, \"title\": \"海军上将刘华清\"}");
      handleRequest("{\"id\":2, \"title\": \"刘华\"}");
      lindenCore.commit();
      lindenCore.refresh();
      bqlCompiler = new BQLCompiler(lindenConfig.getSchema());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void init() {
    lindenConfig = new LindenConfig()
        .setIndexType(LindenConfig.IndexType.RAM)
        .setClusterUrl("127.0.0.1:2181/mock")
        .setSearchAnalyzer("com.xiaomi.linden.lucene.analyzer.LindenMMSeg4jAnalyzerFactory")
        .setIndexAnalyzer("com.xiaomi.linden.lucene.analyzer.LindenMMSeg4jAnalyzerFactory");
    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(new LindenFieldSchema().setName("title").setIndexed(true).setTokenized(true));
    lindenConfig.setSchema(schema);
    lindenConfig.putToProperties("search.analyzer.class", "com.xiaomi.linden.lucene.analyzer.LindenMMSeg4jAnalyzerFactory");
    lindenConfig.putToProperties("index.analyzer.class", "com.xiaomi.linden.lucene.analyzer.LindenMMSeg4jAnalyzerFactory");
  }

  @Test
  public void testQueryString() throws IOException {
    String bql = "select * from linden by query is \"title:刘华清\"";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    // phrase test
    bql = "select * from linden by query is 'title:\"刘华清\"'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());
  }
}
