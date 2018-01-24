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
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.TestLindenCoreBase;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;


public class TestLindenWordDelimiterAnalyzer extends TestLindenCoreBase {

  public TestLindenWordDelimiterAnalyzer() throws Exception {
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
        .putToProperties("search.analyzer.class",
                         "com.xiaomi.linden.lucene.analyzer.LindenWordDelimiterAnalyzerFactory");
    lindenConfig
        .putToProperties("index.analyzer.class",
                         "com.xiaomi.linden.lucene.analyzer.LindenWordDelimiterAnalyzerFactory");
    lindenConfig
        .putToProperties("index.analyzer.luceneMatchVersion", "LUCENE_4_10_0");
    lindenConfig
        .putToProperties("search.analyzer.luceneMatchVersion", "LUCENE_4_10_0");
  }

  @Test
  public void testIndexMode() throws IOException {
    String bql = "select * from linden by query is \"title:刘华清\"";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(3, result.getTotalHits());

    // phrase test
    bql = "select * from linden by query is 'title:\"刘华清\"'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());
    bql = "select * from linden by query is 'title:\"海军上将刘华清\"'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    // snippet test
    bql = "select * from linden by query is 'title:(海军上将刘华清中国龙)' snippet title";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(3, result.getTotalHits());
    Assert.assertEquals(
        "<b>海</b><b>军</b><b>上</b><b>将</b><b>刘</b><b>华</b><b>清</b>!!! <b>中</b><b>国</b> 人民 李玉洁 尚铁<b>龙</b> 胡晓光",
        result.getHits().get(0).getSnippets().get("title").getSnippet());
    Assert.assertEquals("<b>海</b><b>军</b><b>上</b><b>将</b><b>刘</b><b>华</b><b>清</b>",
                        result.getHits().get(1).getSnippets().get("title").getSnippet());

  }

  @Test
  public void testLindenWordDelimiterAnalyzer() throws Exception {
    LindenWordDelimiterAnalyzerFactory wordDelimiterAnalyzerFactory = new LindenWordDelimiterAnalyzerFactory();
    Map<String, String> args = new HashMap<>();
    Map<String, String> lastargs = new HashMap<>();
    args.put("luceneMatchVersion", "LUCENE_4_10_0");
    lastargs.putAll(args);
    Analyzer analyzer = wordDelimiterAnalyzerFactory.getInstance(args);
    TokenStream stream = analyzer.tokenStream("", new StringReader("Hello, this is a test case. " +
                                                                   "你好，这是一个测试的实例。"
                                                                   + "created2018by sls sun-li-shun SunLiShun"));
    String
        expected =
        "[hello][test][case][你][好][这][是][一][个][测][试][的][实][例][created][2018][sls][sun][li][shun][sun][li][shun]";
    String out = "";
    stream.reset();
    while (stream.incrementToken()) {
      out += "[" + stream.getAttribute(CharTermAttribute.class).toString() + "]";
    }
    Assert.assertEquals(expected, out);

    args.put("lower.case", "false");
    args.putAll(lastargs);
    lastargs.putAll(args);
    analyzer = wordDelimiterAnalyzerFactory.getInstance(args);
    stream = analyzer.tokenStream("", new StringReader("Hello, this is a test case. " +
                                                       "你好，这是一个测试的实例。" + "created2018by sls on 20140707"));
    expected =
        "[Hello][test][case][你][好][这][是][一][个][测][试][的][实][例][created][2018][sls][20140707]";
    out = "";
    stream.reset();
    while (stream.incrementToken()) {
      out += "[" + stream.getAttribute(CharTermAttribute.class).toString() + "]";
    }
    Assert.assertEquals(expected, out);

    args.put("set.stopwords", "false");
    args.putAll(lastargs);
    lastargs.putAll(args);
    analyzer = wordDelimiterAnalyzerFactory.getInstance(args);
    stream = analyzer.tokenStream("", new StringReader("Hello, this is a test case. " +
                                                       "你好，这是一个测试的实例。" + "created2018by sls on 20140707"));
    expected =
        "[Hello][this][is][a][test][case][你][好][这][是][一][个][测][试][的][实][例][created][2018][by][sls][on][20140707]";
    out = "";
    stream.reset();
    while (stream.incrementToken()) {
      out += "[" + stream.getAttribute(CharTermAttribute.class).toString() + "]";
    }
    Assert.assertEquals(expected, out);

    args.putAll(lastargs);
    args.put("splitOnCaseChange", "0");
    args.put("set.stopwords", "false");
    args.put("lower.case", "true");
    lastargs.putAll(args);
    analyzer = wordDelimiterAnalyzerFactory.getInstance(args);
    stream = analyzer.tokenStream("", new StringReader("Hello, this is a test case. " +
                                                       "你好，这是一个测试的实例。" + "created2018by sls sun-li-shun SunLiShun"));
    expected =
        "[hello][this][is][a][test][case][你][好][这][是][一][个][测][试][的][实][例][created][2018][by][sls][sun][li][shun][sunlishun]";
    out = "";
    stream.reset();
    while (stream.incrementToken()) {
      out += "[" + stream.getAttribute(CharTermAttribute.class).toString() + "]";
    }
    Assert.assertEquals(expected, out);
  }
}
