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

import com.alibaba.fastjson.JSON;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.core.search.query.QueryConstructor;
import com.xiaomi.linden.core.search.query.filter.FilterConstructor;
import com.xiaomi.linden.lucene.query.flexiblequery.FlexibleQuery;
import com.xiaomi.linden.thrift.builder.query.LindenFlexibleQueryBuilder;
import com.xiaomi.linden.thrift.builder.query.LindenQueryBuilder;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenQuery;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenScoreModel;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenType;

public class TestLindenCore extends TestLindenCoreBase {

  public float DELTA5 = 0.00001f;
  public static final String
      data1 =
      JSON.parse(
          "{\"id\":\"1\",\"title\":\"lucene 1\",\"field1\":\"aaa\",\"field2\":\"aaa aaa\",\"rank\": 1.2,\"cat1\":1,\"cat2\":1.5,\"hotwords\":\"hello\"}")
          .toString();
  public static final String
      data2 =
      JSON.parse(
          "{\"id\":\"2\",\"title\":\"lucene 2\",\"field1\":\"bbb\",\"rank\":4.5,\"cat1\":2,\"cat2\":2.5,\"tagnum\":100}")
          .toString();
  public static final String
      data3 =
      JSON.parse(
          "{\"id\":\"3\",\"title\":\"lucene 3\",\"field1\":\"ccc\",\"rank\":4.5,\"cat1\":3,\"cat2\":3.5,\"tagstr\":[\"ok\"]}")
          .toString();
  public static final String
      data4 =
      JSON.parse(
          "{\"type\":\"index\",\"content\":{\"id\":\"4\",\"title\":\"lucene 4\",\"field1\":\"ddd\",\"rank\":10.3,\"cat1\":4,\"cat2\":4.5}}")
          .toString();
  public static final String
      data5 =
      JSON.parse("{\"id\":\"5\",\"title\":\"lucene 5\",\"field1\":\"ddd\",\"rank\":10.3,\"cat1\":5,\"cat2\":5.5}")
          .toString();
  public static final String data6 = JSON.parse("{\"id\":\"6\",\"tagstr\":[\"MI4\",\"MI Note\",\"Note3\"]}").toString();
  public static final String data7 = JSON.parse("{\"id\":\"7\",\"tagstr\":[\"MI4C\",\"MI Note Pro\"]}").toString();

  public static final String deleteData5 = "{\"type\": \"delete\", \"id\" : \"5\"}";

  public TestLindenCore() throws Exception {
    try {
      handleRequest(data1);
      handleRequest(data2);
      handleRequest(data3);
      handleRequest(data4);
      handleRequest(data5);
      handleRequest(data6);
      handleRequest(data7);
      handleRequest(deleteData5);
      lindenCore.commit();
      lindenCore.refresh();
      bqlCompiler = new BQLCompiler(lindenConfig.getSchema());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void init() throws Exception {
    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(new LindenFieldSchema().setName("title").setIndexed(true).setStored(true).setTokenized(true));
    schema.addToFields(new LindenFieldSchema().setName("field1").setIndexed(true).setStored(false));
    schema.addToFields(new LindenFieldSchema().setName("field2").setIndexed(true).setTokenized(true).setStored(false)
                           .setOmitFreqs(true).setOmitNorms(true));
    schema.addToFields(
        new LindenFieldSchema().setName("rank").setType(LindenType.FLOAT).setIndexed(true).setStored(true));
    schema.addToFields(
        new LindenFieldSchema().setName("cat1").setType(LindenType.INTEGER).setIndexed(true).setStored(true));
    schema.addToFields(
        new LindenFieldSchema().setName("cat2").setType(LindenType.DOUBLE).setIndexed(true).setStored(true));
    schema.addToFields(
        new LindenFieldSchema().setName("tagstr").setIndexed(true).setMulti(true).setStored(true).setMulti(true));
    schema.addToFields(new LindenFieldSchema().setName("hotwords").setStored(true));
    schema.addToFields(new LindenFieldSchema().setName("tagnum").setType(LindenType.INTEGER).setIndexed(true));
    lindenConfig.setSchema(schema);
  }

  @Test
  public void basicTest() throws Exception {
    LindenSearchRequest request = new LindenSearchRequest().setQuery(
        LindenQueryBuilder.buildTermQuery("title", "lucene"));

    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
  }

  @Test
  public void flexibleQueryTest() throws IOException {
    String function = "    float sum = 0;\n" +
                      "    for (int i = 0; i < getFieldLength(); ++i) {\n" +
                      "      for (int j = 0; j < getTermLength(); ++j) {\n" +
                      "        sum += 100 * getScore(i, j) * rank();\n" +
                      "      }\n" +
                      "    }\n" +
                      "    return sum;";
    LindenQuery flexQuery = new LindenFlexibleQueryBuilder()
        .setQuery("lucene").addField("title").addModel("test", function).build();
    LindenResult result = lindenCore.search(new LindenSearchRequest().setQuery(flexQuery));
    Assert.assertEquals(4, result.getTotalHits());

    flexQuery = new LindenFlexibleQueryBuilder()
        .setQuery("lucene 1").addField("title").addModel("test", function).setFullMatch(true).build();
    result = lindenCore.search(new LindenSearchRequest().setQuery(flexQuery));
    Assert.assertEquals(1, result.getTotalHits());
  }

  @Test
  public void flexibleQueryEmptyFieldTest() throws IOException {
    String function = "   if (tagnum() != 0) {\n" +
                      "      return 10f;\n" +
                      "    } else if (tagstr().length > 0) {\n" +
                      "      return 100f;\n" +
                      "    } else {\n" +
                      "      return 1f;\n" +
                      "    }";

    LindenQuery flexQuery = new LindenFlexibleQueryBuilder()
        .setQuery("lucene").addField("title").addModel("test", function).build();
    LindenResult result = lindenCore.search(new LindenSearchRequest().setQuery(flexQuery));
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals("3", result.getHits().get(0).getId());
    Assert.assertEquals(100f, result.getHits().get(0).getScore(), 0.01);
    Assert.assertEquals("2", result.getHits().get(1).getId());
    Assert.assertEquals(10f, result.getHits().get(1).getScore(), 0.01);
  }

  @Test
  public void bqlFlexibleQueryTest() throws IOException {
    String bql = "select * from linden by flexible_query is \"lucene 2\" in (title)\n" +
                 "using model test(Float m = 10)\n" +
                 "begin\n" +
                 "    float sum = 0f;\n" +
                 "    for (int i = 0; i < getFieldLength(); ++i) {\n" +
                 "        for (int j = 0; j < getTermLength(); ++j) {\n" +
                 "            sum += 10 * getScore(i, j);\n" +
                 "            addTermExpl(i, j, 10, getExpl(\"[%.2f]\", sum));\n" +
                 "        }\n" +
                 "        addFieldExpl(i, 20, getExpl(\"[%.2f]\", sum * m));\n" +
                 "    }\n" +
                 "    return sum * m;\n" +
                 "end explain";
    LindenSearchRequest lindenRequest = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(lindenRequest);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals("[158.20]        [FIELD:title MATCHED:2]",
                        result.getHits().get(0).getExplanation().getDetails().get(0).getDescription());
  }

  @Test
  public void lindenScoreModelTest() throws IOException {
    String func = " return rank() * 10;";
    LindenScoreModel model = new LindenScoreModel().setName("test").setFunc(func);

    LindenSearchRequest request = new LindenSearchRequest().setQuery(
        LindenQueryBuilder.buildTermQuery("title", "lucene").setScoreModel(model));
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals(103f, result.getHits().get(0).getScore(), 0.1f);

    // bad score model function
    func = " return rank() * 10 + a;";
    model = new LindenScoreModel().setName("test").setFunc(func);

    request = new LindenSearchRequest().setQuery(
        LindenQueryBuilder.buildTermQuery("title", "lucene").setScoreModel(model));

    try {
      lindenCore.search(request);
    } catch (Exception e) {
      // do nothing
    }
    try {
      lindenCore.search(request);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("score model test compile failed, please check score model code"));
    }
  }

  @Test
  public void termQueryTest() throws Exception {
    String bql = "select * from linden where title='lucene'";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
  }

  @Test
  public void lindenBQLScoreModelParamTest() throws IOException {
    String bql = "select * from linden by query is \"title:lucene\" " +
                 "using score model test(Long param1 = $long1, Map<String, Double> dict = {'中文': 1.0, 'b': 2.0})"
                 + "begin\n"
                 + "double sum = 0;\n"
                 + "sum += dict.get(\"中文\");\n"
                 + "for (String entry : dict.keySet()) {\n"
                 + "sum += dict.get(entry);\n"
                 + "}\n"
                 + "return sum;\n"
                 + "end explain";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals(4.0, result.getHits().get(0).getScore(), 0.1f);
  }

  @Test
  public void lindenBQLScoreModelPluginParamTest() throws IOException {
    String bql = "select * from linden by flexible_query is \"lucene\" in (title) "
                 + "using model Plugin com.xiaomi.linden.core.plugin.MockedScoreModelV1(Long param1 = $long1,"
                 + "String exp = \"$exp\""
                 + ")"
                 + " source explain $exp";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
  }

  @Test
  public void lindenBQLScoreModelTest() throws IOException {
    String bql = "select * from linden by query is \"title:lucene\" " +
                 "using score model test(long aa = 100000000000000, String title = ['a', 'b'], long a = 10, Double b = [1, 2, 3, 4]) "
                 +
                 "begin " +
                 "    float sum = 0;\n" +
                 "    for (Double m : b) {\n" +
                 "      sum += m;\n" +
                 "    }" +
                 "writeExplanation(\"rank:%.2f, a:%d\", rank(), a); " +
                 "return sum + rank() * 10 * a; " +
                 "end explain";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals(1040f, result.getHits().get(0).getScore(), 0.1f);
    Assert.assertTrue(result.getHits().get(0).getExplanation().toString()
                          .contains("value:1040.0, description:rank:10.30, a:10"));
  }

  @Test
  public void bqlSortTest() throws IOException {
    String bql = "select * from linden by query is \"title:lucene\" order by rank, field1 desc source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals("4", result.getHits().get(0).getId());
    Assert.assertEquals("10.3", result.getHits().get(0).getFields().get("rank"));
    Assert.assertEquals("ddd", result.getHits().get(0).getFields().get("field1"));
    Assert.assertEquals("3", result.getHits().get(1).getId());
    Assert.assertEquals("4.5", result.getHits().get(1).getFields().get("rank"));
    Assert.assertEquals("ccc", result.getHits().get(1).getFields().get("field1"));

    bql = "select * from linden by query is \"title:lucene\" order by rank, field1 asc source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals("4", result.getHits().get(0).getId());
    Assert.assertEquals("10.3", result.getHits().get(0).getFields().get("rank"));
    Assert.assertEquals("ddd", result.getHits().get(0).getFields().get("field1"));
    Assert.assertEquals("2", result.getHits().get(1).getId());
    Assert.assertEquals("4.5", result.getHits().get(1).getFields().get("rank"));
    Assert.assertEquals("bbb", result.getHits().get(1).getFields().get("field1"));

    bql = "select * from linden by query is \"title:lucene\" order by rank asc source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals("1", result.getHits().get(0).getId());
    Assert.assertEquals("1.2", result.getHits().get(0).getFields().get("rank"));
  }

  @Test
  public void bqlInTest() throws IOException {
    String bql = "select * from linden where cat1 in (1, 3)";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "select * from linden where cat1 in (1)";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden where cat1 in (1, 3) except(3)";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden where cat2 in (1.5, 3.5)";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "select * from linden where cat2 > 1.5 and cat2 in () except(3.5)";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());
  }

  @Test
  public void bqlNullTest() throws Exception {
    String bql = "select * from linden where cat1 is null";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "select * from linden by cat1 is not null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());

    bql = "select * from linden where cat1 is null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "select * from linden where title is not null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());

    bql = "select * from linden by cat1 is null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "select * from linden where cat3 is null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(6, result.getTotalHits());

    bql = "select * from linden where cat3 is not null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(0, result.getTotalHits());

    bql = "select * from linden where tagstr is null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(3, result.getTotalHits());

    bql = "select * from linden where tagstr is not null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(3, result.getTotalHits());

    bql = "select * from linden where id is not null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(6, result.getTotalHits());

    bql = "select * from linden where hotwords is not null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(0, result.getTotalHits());

    bql = "select * from linden where hotwords is null";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(6, result.getTotalHits());
  }

  @Test
  public void testQueryString() throws IOException {
    String bql = "select * from linden by query is \"title:lucene\"";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());

    // phrase test
    bql = "select * from linden by query is 'title:\"lucene 1\"'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden by query is 'title:(lucene 1)' OP(AND)";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden by query is 'cat1:{* TO 3}'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "select * from linden by query is 'cat1:[3 TO 3]'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden by query is 'cat1:[3 TO 3]'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());
  }

  @Test
  public void testSearchById() throws IOException {
    String bql = "select * from linden where id in ('1', '2', '3')";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(3, result.getTotalHits());

    bql = "select * from linden where id = '1'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden by id = '1'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());
  }

  @Test
  public void testRangeQuery() throws IOException {
    String bql = "select * from linden where cat1 = 3";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden where cat1 > 3";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden where cat1 < 3";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "select * from linden where cat1 >= 3";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "select * from linden where cat1 < 3 and cat1 > 1";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden where cat1 between 1 AND 3";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(3, result.getTotalHits());

    bql = "select * from linden where cat2 > 3.5";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden where cat2 < 3.5";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "select * from linden where cat2 between 1.5 AND 3.5";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(3, result.getTotalHits());

    bql = "select * from linden by cat2 < 3.5";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "select * from linden by cat2 between 1.5 AND 3.5";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(3, result.getTotalHits());
  }

  @Test
  public void notEqualTest() throws Exception {
    String bql = "select * from linden where id <> '3'";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(5, result.getTotalHits());

    bql = "select * from linden where field1 <> 'aaa'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(5, result.getTotalHits());

    bql = "select * from linden by field1 <> 'aaa'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(5, result.getTotalHits());
  }

  @Test
  public void likeTest() throws Exception {
    String bql = "select * from linden by field1 like \"aa*\" source boost by 2";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    Query query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof WildcardQuery);
    Assert.assertEquals("field1:aa*^2.0", query.toString());
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(1, result.getTotalHits());

    bql = "select * from linden by field1 not like \"aaa*\" source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof BooleanQuery);
    Assert.assertEquals("+*:* -field1:aaa*", query.toString());
    result = lindenCore.search(request);
    Assert.assertEquals(5, result.getTotalHits());
  }

  @Test
  public void omitFreqsTest() throws IOException {
    String bql = "select * from linden by query is 'field2:aaa' explain";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals("tf(freq=1.0), with freq of:",
                        result.getHits().get(0).getExplanation().getDetails().get(0).getDetails().get(0)
                            .getDescription());
  }

  @Test
  public void testScoreModelPlugin() throws IOException {
    String bql = "select * from linden where title='lucene'" +
                 " using score model plugin com.xiaomi.linden.core.plugin.TestScoreModelStrategy"
                 + "(Long param1 = $long1, Map<String, Double> dict = {'中文': 1.0, 'b': 2.0});";
    LindenSearchRequest searchRequest = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(searchRequest);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals(14.3, result.getHits().get(0).getScore(), 0.01);
  }

  @Test
  public void deleteTest() throws IOException {
    String bql = "delete from linden where id = '1'";
    LindenDeleteRequest request = bqlCompiler.compile(bql).getDeleteRequest();
    lindenCore.delete(request);
    lindenCore.refresh();
    bql = "select * from linden where id = '1'";
    LindenSearchRequest searchRequest = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(searchRequest);
    Assert.assertEquals(0, result.getTotalHits());

    bql = "select * from linden";
    searchRequest = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(searchRequest);
    Assert.assertEquals(5, result.getTotalHits());

    bql = "select * from linden where id >= '1' and id <='3'";
    searchRequest = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(searchRequest);
    Assert.assertEquals(2, result.getTotalHits());

    bql = "delete from linden where id >= '1' and id <='3'";
    LindenDeleteRequest deleteRequest = bqlCompiler.compile(bql).getDeleteRequest();
    lindenCore.delete(deleteRequest);
    lindenCore.refresh();
    bql = "select * from linden";
    searchRequest = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(searchRequest);
    Assert.assertEquals(3, result.getTotalHits());

    bql = "delete from linden";
    request = bqlCompiler.compile(bql).getDeleteRequest();
    lindenCore.delete(request);
    lindenCore.refresh();
    bql = "select * from linden";
    searchRequest = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(searchRequest);
    Assert.assertEquals(0, result.getTotalHits());
  }

  @Test
  public void testDisMax() throws InterruptedException, IOException {
    String bql = "select * from linden source | select * from linden source";
    LindenSearchRequest lindenRequest = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(lindenRequest);
    Assert.assertEquals(6, result.getTotalHits());
  }

  @Test
  public void testBoosts() throws Exception {
    String bql = "select * from linden by title = \"lucene\" and field1 = \"aaa\" boost by 2";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    Query query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof BooleanQuery);
    Assert.assertEquals(2f, query.getBoost(), DELTA5);

    bql = "select * from linden by title = \"lucene\" boost by 3";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof TermQuery);
    Assert.assertEquals(3f, query.getBoost(), DELTA5);

    bql = "select * from linden by Query is 'title:lucene' subBoost by 4 or tagstr = 'MI4'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof BooleanQuery);
    BooleanQuery booleanQuery = (BooleanQuery) query;
    Assert.assertEquals(4f, booleanQuery.clauses().get(0).getQuery().getBoost(), DELTA5);
    Assert.assertEquals(1f, booleanQuery.clauses().get(1).getQuery().getBoost(), DELTA5);
    request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(5, result.getHitsSize());
    Assert.assertEquals("1", result.getHits().get(0).getId());

    bql = "select * from linden by Query is 'title:lucene' subBoost by 4 or tagstr = 'MI4' subBoost by 2";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof BooleanQuery);
    booleanQuery = (BooleanQuery) query;
    Assert.assertEquals(4f, booleanQuery.clauses().get(0).getQuery().getBoost(), DELTA5);
    Assert.assertEquals(2f, booleanQuery.clauses().get(1).getQuery().getBoost(), DELTA5);
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(5, result.getHitsSize());
    Assert.assertEquals("6", result.getHits().get(0).getId());

    bql = "select * from linden by rank > 4 subBoost by 4 or tagstr = 'MI4'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof BooleanQuery);
    booleanQuery = (BooleanQuery) query;
    Assert.assertEquals(4f, booleanQuery.clauses().get(0).getQuery().getBoost(), DELTA5);
    Assert.assertEquals(1f, booleanQuery.clauses().get(1).getQuery().getBoost(), DELTA5);
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getHitsSize());
    Assert.assertEquals("2", result.getHits().get(0).getId());

    bql = "select * from linden by rank > 4 subBoost by 4 or tagstr = 'MI4' subBoost by 2.0";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof BooleanQuery);
    booleanQuery = (BooleanQuery) query;
    Assert.assertEquals(4f, booleanQuery.clauses().get(0).getQuery().getBoost(), DELTA5);
    Assert.assertEquals(2f, booleanQuery.clauses().get(1).getQuery().getBoost(), DELTA5);
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getHitsSize());
    Assert.assertEquals("6", result.getHits().get(0).getId());

    // this boolean query case, does not support term query boost
    bql = "select * from linden by title = \"lucene\" and field1 = \"aaa\" boost by 2" +
          "|select * from linden by title = \"lucene\" boost by 3";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof DisjunctionMaxQuery);
    DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) query;
    Assert.assertEquals(2f, disjunctionMaxQuery.getDisjuncts().get(0).getBoost(), DELTA5);
    Assert.assertEquals(3f, disjunctionMaxQuery.getDisjuncts().get(1).getBoost(), DELTA5);
    Assert.assertEquals("(((+title:lucene +field1:aaa)^2.0) | title:lucene^3.0)~0.1", query.toString());

    bql =
        "select * from linden by query is \"title:(lucene test)^2\" where rank > 1 boost by 2";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof BooleanQuery);
    Assert.assertEquals("(title:lucene title:test)^4.0", query.toString());

    bql = "select * from linden by flexible_query is \"lucene test\" in (title)\n" +
          "using model test(Float m = 10)\n" +
          "begin\n" +
          "    return score();\n" +
          "end where rank > 1 source explain boost by 2";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertEquals("FlexibleQuery([title]:[lucene,test])^2.0", query.toString());

    bql =
        "select * from linden by query is \"title:lucene^2\" where rank > 1 order by rank, field1 desc source boost by 2"
        +
        "|select * from linden by query is \"title:test^3 field1:aaa\" where rank > 1 order by rank, field1 desc source boost by 0.03";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof DisjunctionMaxQuery);
    disjunctionMaxQuery = (DisjunctionMaxQuery) query;
    // subQuery 1 is filterQuery which contains a termQuery
    Assert.assertTrue(disjunctionMaxQuery.getDisjuncts().get(0) instanceof FilteredQuery);
    FilteredQuery filteredQuery = (FilteredQuery) disjunctionMaxQuery.getDisjuncts().get(0);
    Assert.assertTrue(filteredQuery.getQuery() instanceof TermQuery);
    Assert.assertEquals(4f, filteredQuery.getQuery().getBoost(), DELTA5);
    // subQuery 2 is filterQuery which contains a booleanQuery
    Assert.assertTrue(disjunctionMaxQuery.getDisjuncts().get(1) instanceof FilteredQuery);
    filteredQuery = (FilteredQuery) disjunctionMaxQuery.getDisjuncts().get(1);
    Assert.assertTrue(filteredQuery.getQuery() instanceof BooleanQuery);
    Assert.assertEquals(0.03f, filteredQuery.getQuery().getBoost(), DELTA5);
    Assert.assertEquals(
        "(filtered(title:lucene^4.0)->rank:{1.0 TO *} | filtered((title:test^3.0 field1:aaa)^0.03)->rank:{1.0 TO *})~0.1",
        query.toString());

    // test FlexibleQuery
    bql = "select * from linden by flexible_query is \"lucene\" in (title)\n" +
          "using model test(Float m = 10)\n" +
          "begin\n" +
          "    float sum = 0f;\n" +
          "    for (int i = 0; i < getFieldLength(); ++i) {\n" +
          "        for (int j = 0; j < getTermLength(); ++j) {\n" +
          "            sum += 10 * getScore(i, j);\n" +
          "            addTermExpl(i, j, 10, getExpl(\"[%.2f]\", sum));\n" +
          "        }\n" +
          "        addFieldExpl(i, 20, getExpl(\"[%.2f]\", sum * m));\n" +
          "    }\n" +
          "    return sum * m;\n" +
          "end where rank > 1 source explain";
    bql = bql + "|select * from linden by flexible_query is \"test^3\" in (field1)\n" +
          "using model test(Float m = 10)\n" +
          "begin\n" +
          "    float sum = 0f;\n" +
          "    for (int i = 0; i < getFieldLength(); ++i) {\n" +
          "        for (int j = 0; j < getTermLength(); ++j) {\n" +
          "            sum += 10 * getScore(i, j);\n" +
          "            addTermExpl(i, j, 10, getExpl(\"[%.2f]\", sum));\n" +
          "        }\n" +
          "        addFieldExpl(i, 20, getExpl(\"[%.2f]\", sum * m));\n" +
          "    }\n" +
          "    return sum * m;\n" +
          "end where rank > 1 source explain boost by 0.5";
    request = bqlCompiler.compile(bql).getSearchRequest();
    query = QueryConstructor.constructQuery(request.getQuery(), lindenConfig);
    Assert.assertTrue(query instanceof DisjunctionMaxQuery);
    disjunctionMaxQuery = (DisjunctionMaxQuery) query;
    // subQuery 1 is a FilteredQuery which contains a FlexibleQuery
    Assert.assertTrue(disjunctionMaxQuery.getDisjuncts().get(0) instanceof FilteredQuery);
    filteredQuery = (FilteredQuery) disjunctionMaxQuery.getDisjuncts().get(0);
    Assert.assertTrue(filteredQuery.getQuery() instanceof FlexibleQuery);
    Assert.assertEquals(1f, filteredQuery.getQuery().getBoost(), DELTA5);
    // subQuery 2 is a FilteredQuery which contains a FlexibleQuery
    Assert.assertTrue(disjunctionMaxQuery.getDisjuncts().get(1) instanceof FilteredQuery);
    filteredQuery = (FilteredQuery) disjunctionMaxQuery.getDisjuncts().get(1);
    Assert.assertTrue(filteredQuery.getQuery() instanceof FlexibleQuery);
    Assert.assertEquals(0.5f, filteredQuery.getQuery().getBoost(), DELTA5);
    Assert.assertEquals(
        "(filtered(FlexibleQuery([title]:[lucene]))->rank:{1.0 TO *} | filtered(FlexibleQuery([field1]:[test^3.0])^0.5)->rank:{1.0 TO *})~0.1",
        query.toString());
  }


  @Test
  public void testFlexibleFilter() throws Exception {
    String bql = "select name from linden  where flexible_query is 'qq音乐' full_match in (name^1.5) \n"
                 + "USING MODEL test \n"
                 + "begin\n"
                 + " return score();\n"
                 + "end\n"
                 + "order by score,id  limit $offset, $length source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    Filter filter = FilterConstructor.constructFilter(request.getFilter(), lindenConfig);
    Assert.assertEquals("QueryWrapperFilter(FlexibleQuery([name^1.5]:[qq,音,乐]fullMatch))", filter.toString());

    bql = "select name from linden \n"
          + "by flexible_query is \"lucene\" in (title^1.2) \n"
          + "USING MODEL test1 \n"
          + "begin \n"
          + "  return score() + 1;\n"
          + "end\n"
          + "where flexible_query is 'ddd' full_match in (field1)\n"
          + "USING MODEL filter_func begin return 1f; end \n"
          + "order by score,id limit $offset, $length source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(1, result.getHitsSize());
    Assert.assertEquals(1f, result.getHits().get(0).getScore(), DELTA5);
    Assert.assertEquals("4", result.getHits().get(0).getId());
  }

  @Test
  public void testMultiIndexField() throws InterruptedException, IOException {
    String bql = "select * from linden where tagstr='MI4' source";
    LindenSearchRequest lindenRequest = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(lindenRequest);
    Assert.assertEquals(1, result.getTotalHits());
    Assert.assertEquals("6", result.getHits().get(0).getId());
    Assert.assertEquals(data6, result.getHits().get(0).getSource());
    bql = "select * from linden by tagstr='MI Note' source";
    lindenRequest = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(lindenRequest);
    Assert.assertEquals(1, result.getTotalHits());
    Assert.assertEquals("6", result.getHits().get(0).getId());

    bql = "select * from linden by tagstr='MI4C' source";
    lindenRequest = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(lindenRequest);
    Assert.assertEquals(1, result.getTotalHits());
    Assert.assertEquals("7", result.getHits().get(0).getId());
    Assert.assertEquals(data7, result.getHits().get(0).getSource());
    bql = "select * from linden where tagstr='MI Note Pro'";
    lindenRequest = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(lindenRequest);
    Assert.assertEquals(1, result.getTotalHits());
    Assert.assertEquals("7", result.getHits().get(0).getId());

    bql = "select * from linden source " +
          "using score model test " +
          "begin " +
          "  float sum = 0;\n" +
          "  for (String tag : tagstr()) {\n" +
          "    if (tag.equals(\"MI Note Pro\")) {\n" +
          "      sum += 10.0; \n" +
          "      }\n" +
          "  }\n" +
          "  return sum;\n" +
          "end";
    lindenRequest = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(lindenRequest);
    Assert.assertEquals(6, result.getTotalHits());
    Assert.assertEquals("7", result.getHits().get(0).getId());
    Assert.assertEquals(10.0, result.getHits().get(0).getScore(), 0.001);
    Assert.assertEquals("{\"id\":\"7\",\"tagstr\":[\"MI4C\",\"MI Note Pro\"]}", result.getHits().get(0).getSource());
  }
}
