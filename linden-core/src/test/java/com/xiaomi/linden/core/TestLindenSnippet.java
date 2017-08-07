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
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;

public class TestLindenSnippet extends TestLindenCoreBase {

  public TestLindenSnippet() throws Exception {
    try {
      handleRequest(generateRequest(
          "1",
          "This is a test. Just a test highlighting from postings. Feel free to ignore.",
          "I am hoping for best"));
      handleRequest(generateRequest(
          "2",
          "Highlighting the first term. Hope it works.",
          "But best may not be good enough."));
      handleRequest(generateRequest(
          "3",
          "8科技早点:猎豹机版游戏MX2忽视小米1代6",
          "But best may not be good enough."));
      handleRequest(generateRequest(
          "4",
          "新发布的小米6如何? 想买小米6吗? 4月28日开抢小米6. 哪里抢小米6? 告诉你也没用, 反正你都买不到小米6",
          "小米6号称7年巅峰之作，新一代性能怪兽。中国首发骁龙835处理器+标配6GB内存，安兔兔综合跑分184292，小米6超越再竖立安卓标杆。"));
      lindenCore.commit();
      lindenCore.refresh();
      bqlCompiler = new BQLCompiler(lindenConfig.getSchema());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String generateRequest(String id, String title, String body) {
    return String.format("{\"id\":\"%s\", \"title\":\"%s\", \"body\": \"%s\"}", id, title, body);
  }

  @Override
  public void init() throws Exception {
    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(new LindenFieldSchema().setName("title").setTokenized(true).setIndexed(true).setSnippet(true));
    schema.addToFields(new LindenFieldSchema().setName("body").setTokenized(true).setIndexed(true).setSnippet(true));
    lindenConfig.setSchema(schema);
    lindenConfig.setSearchAnalyzer("com.xiaomi.linden.lucene.analyzer.LindenMMSeg4jAnalyzerFactory");
    lindenConfig.setIndexAnalyzer("com.xiaomi.linden.lucene.analyzer.LindenMMSeg4jAnalyzerFactory");
    lindenConfig
        .putToProperties("search.analyzer.class", "com.xiaomi.linden.lucene.analyzer.LindenMMSeg4jAnalyzerFactory");
    lindenConfig.putToProperties("search.analyzer.unique", "false");
    lindenConfig
        .putToProperties("index.analyzer.class", "com.xiaomi.linden.lucene.analyzer.LindenMMSeg4jAnalyzerFactory");
    lindenConfig.putToProperties("index.analyzer.unique", "false");
  }

  @Test
  public void basicTest() throws IOException {
    String bql = "SELECT * FROM linden by query is 'title:test' snippet title";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals("This is a <b>test</b>. Just a <b>test</b> highlighting from postings. ",
                        result.getHits().get(0).getSnippets().get("title").getSnippet());
    bql = "SELECT * FROM linden by query is 'title:highlighting' snippet title";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals("<b>Highlighting</b> the first term. ",
                        result.getHits().get(0).getSnippets().get("title").getSnippet());

    bql = "SELECT * FROM linden by query is 'title:游戏' snippet title";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert
        .assertEquals("8科技早点:猎豹机版<b>游戏</b>MX2忽视小米1代6", result.getHits().get(0).getSnippets().get("title").getSnippet());
  }

  @Test
  public void multiTest() throws IOException {
    String bql = "SELECT * FROM linden by query is 'title:test AND body:best' snippet title, body";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals("This is a <b>test</b>. Just a <b>test</b> highlighting from postings. ",
                        result.getHits().get(0).getSnippets().get("title").getSnippet());
    Assert.assertEquals("I am hoping for <b>best</b>", result.getHits().get(0).getSnippets().get("body").getSnippet());
  }

  @Test
  public void flexibleQueryTest() throws IOException {
    String bql = "SELECT * FROM LINDEN BY flexible_query is \"test best\" in (title, body) USING MODEL simplest BEGIN\n"
                 + "   float sum = 0f;\n"
                 + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
                 + "        for (int j = 0; j < getTermLength(); ++j) {\n"
                 + "            if (isMatched(i, j)) {\n"
                 + "                sum += getScore(i, j);\n"
                 + "            }\n"
                 + "        } \n"
                 + "    } \n"
                 + "    return sum;\n"
                 + "END\n"
                 + "snippet title, body Limit 0, 10\n";

    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals("This is a <b>test</b>. Just a <b>test</b> highlighting from postings. ",
                        result.getHits().get(0).getSnippets().get("title").getSnippet());
    Assert.assertEquals("I am hoping for <b>best</b>", result.getHits().get(0).getSnippets().get("body").getSnippet());
  }

  @Test
  public void passageLimitTest() throws IOException {
    String bql = "SELECT * FROM linden by query is 'title:小米6' or query is 'body:小米6' snippet title, body";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(
        "新发布的<b>小米</b><b>6</b>如何? 想买<b>小米</b><b>6</b>吗? 4月28日开抢<b>小米</b><b>6</b>. 哪里抢<b>小米</b><b>6</b>? 告诉你也没用, 反正你都买不到<b>小米</b><b>6</b>",
        result.getHits().get(0).getSnippets().get("title").getSnippet());
    Assert.assertEquals(
        "<b>小米</b><b>6</b>号称7年巅峰之作，新一代性能怪兽。中国首发骁龙835处理器+标配6GB内存，安兔兔综合跑分184292，<b>小米</b><b>6</b>超越再竖立安卓标杆。",
        result.getHits().get(0).getSnippets().get("body").getSnippet());
  }
}
