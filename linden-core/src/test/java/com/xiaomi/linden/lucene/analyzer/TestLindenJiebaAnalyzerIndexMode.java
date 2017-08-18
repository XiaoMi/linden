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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.TestLindenCoreBase;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;

public class TestLindenJiebaAnalyzerIndexMode extends TestLindenCoreBase {

  public TestLindenJiebaAnalyzerIndexMode() throws Exception {
    try {
      handleRequest("{\"id\":1, \"title\": [\"海军上将刘华清\"]}");
      handleRequest("{\"id\":2, \"title\": [\"刘华\"]}");
      handleRequest("{\"id\":3, \"title\": [\"海军上将刘华清!!!\", \"中国\", \"人民\", \"李玉洁\", \"尚铁龙\", \"胡晓光\"]}");
      lindenCore.commit();
      lindenCore.refresh();
      bqlCompiler = new BQLCompiler(lindenConfig.getSchema());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void init() {
    lindenConfig.setIndexType(LindenConfig.IndexType.RAM);
    lindenConfig.setClusterUrl("127.0.0.1:2181/mock");
    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(
        new LindenFieldSchema().setName("title").setIndexed(true).setTokenized(true).setSnippet(true).setMulti(true));
    lindenConfig.setSchema(schema);
    lindenConfig
        .putToProperties("search.analyzer.class", "com.xiaomi.linden.lucene.analyzer.LindenJiebaAnalyzerFactory");
    lindenConfig
        .putToProperties("index.analyzer.class", "com.xiaomi.linden.lucene.analyzer.LindenJiebaAnalyzerFactory");
    lindenConfig
        .putToProperties("index.analyzer.mode", "index");
  }

  @Test
  public void testIndexMode() throws IOException {
    String bql = "select * from linden by query is \"title:刘华清\"";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    // phrase test
    bql = "select * from linden by query is 'title:\"刘华清\"'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());
    bql = "select * from linden by query is 'title:\"海军上将刘华清\"'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(0, result.getTotalHits());

    // snippet test
    bql = "select * from linden by query is 'title:(海军上将刘华清中国龙)' snippet title";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());
    Assert.assertEquals("<b>海军上将</b><b>刘华清</b>",
                        result.getHits().get(0).getSnippets().get("title").getSnippet());
    Assert.assertEquals("<b>海军上将</b><b>刘华清</b>!!! <b>中国</b> 人民 李玉洁 尚铁龙 胡晓光",
                        result.getHits().get(1).getSnippets().get("title").getSnippet());

    bql = "select * from linden by query is 'title:(海军中国铁龙)' snippet title";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());
    Assert.assertEquals("<b>海军</b>上将刘华清!!! <b>中国</b> 人民 李玉洁 尚铁龙 胡晓光",
                        result.getHits().get(0).getSnippets().get("title").getSnippet());
  }
}
