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

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.thrift.builder.query.LindenFlexibleQueryBuilder;
import com.xiaomi.linden.thrift.common.GroupParam;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenQuery;
import com.xiaomi.linden.thrift.common.LindenRequest;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestLindenGroupSearch extends TestLindenCoreBase {

  public TestLindenGroupSearch() throws Exception {
    try {
      handleRequest("{\"id\":1, \"title\": \"lucene 1\", \"field1\": \"aaa\", \"field2\": \"aaa aaa\", \"rank\": 1.2, \"cat1\":1}");
      handleRequest("{\"id\":2, \"title\": \"lucene 2\", \"field1\": \"bbb\", \"rank\": 4.5, \"cat1\":2}");
      handleRequest("{\"id\":3, \"title\": \"lucene 3\", \"field1\": \"ccc\", \"rank\": 4.0, \"cat1\":1}");
      handleRequest("{\"type\": \"index\", \"content\": {\"id\":4, \"title\": \"lucene 4\", \"field1\": \"ddd\", \"rank\": 10.3, \"cat1\":2}}");
      handleRequest("{\"id\":5, \"title\": \"lucene 5\", \"field1\": \"ddd\", \"rank\": 10.0, \"cat1\":3}");
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
    schema.addToFields(new LindenFieldSchema().setName("rank").setType(LindenType.FLOAT).setIndexed(true).setStored(true));
    schema.addToFields(new LindenFieldSchema().setName("cat1").setIndexed(true).setStored(true));
    lindenConfig.setSchema(schema);
  }

  @Test
  public void groupSearchTest1() throws IOException {
    String function = "    float sum = 0;\n" +
        "    for (int i = 0; i < getFieldLength(); ++i) {\n" +
        "      for (int j = 0; j < getTermLength(); ++j) {\n" +
        "        sum += getScore(i, j) * rank();\n" +
        "      }\n" +
        "    }\n" +
        "    return sum;";
    LindenQuery query = new LindenFlexibleQueryBuilder()
        .setQuery("lucene").addField("title").addModel("test", function).build();
    LindenSearchRequest request = new LindenSearchRequest().setQuery(query).setSource(false).setExplain(false);
    request.setGroupParam(new GroupParam("cat1").setGroupInnerLimit(2));
    LindenResult result = lindenCore.search(request);

    Assert.assertEquals(true, result.isSuccess());
    Assert.assertEquals(5, result.getTotalHits());
    Assert.assertEquals(3, result.getTotalGroups());
    Assert.assertEquals(3, result.getHitsSize());
    Assert.assertEquals(2, result.getHits().get(0).getGroupHitsSize());
    Assert.assertEquals(1, result.getHits().get(1).getGroupHitsSize());
    Assert.assertEquals(2, result.getHits().get(2).getGroupHitsSize());
  }

  @Test
  public void groupSearchTest2() throws IOException {
    String bql = "select title,field1,cat1,rank from linden by flexible_query is 'lucene' in (title, field1)" +
                 " USING MODEL test" +
                 " begin " +
                 "    float sum = 0;\n" +
                 "    for (int i = 0; i < getFieldLength(); ++i) {\n" +
                 "      for (int j = 0; j < getTermLength(); ++j) {\n" +
                 "        sum += getScore(i, j) * rank();\n" +
                 "      }\n" +
                 "    }\n" +
                 "    return sum;" +
                 " end " +
                 " group by cat1 TOP 3" +
                 " limit 0,10 " +
                 " source explain";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    Assert.assertEquals(true, request.isSetGroupParam());
    Assert.assertEquals("cat1", request.getGroupParam().getGroupField());
    Assert.assertEquals(3, request.getGroupParam().getGroupInnerLimit());

    LindenResult result = lindenCore.search(request);

    Assert.assertEquals(true, result.isSuccess());
    Assert.assertEquals(5, result.getTotalHits());
    Assert.assertEquals(3, result.getTotalGroups());
    Assert.assertEquals(3, result.getHitsSize());
    Assert.assertEquals(2, result.getHits().get(0).getGroupHitsSize());
    Assert.assertEquals(1, result.getHits().get(1).getGroupHitsSize());
    Assert.assertEquals(2, result.getHits().get(2).getGroupHitsSize());

    bql = "select * from linden order by rank group by cat1 TOP 3 limit 0,10 source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    Assert.assertEquals(true, request.isSetGroupParam());
    Assert.assertEquals("cat1", request.getGroupParam().getGroupField());
    Assert.assertEquals(3, request.getGroupParam().getGroupInnerLimit());

    result = lindenCore.search(request);

    Assert.assertEquals(true, result.isSuccess());
    Assert.assertEquals(5, result.getTotalHits());
    Assert.assertEquals(3, result.getTotalGroups());
    Assert.assertEquals(3, result.getHitsSize());
    Assert.assertEquals(2, result.getHits().get(0).getGroupHitsSize());
    Assert.assertEquals("10.3", result.getHits().get(0).getGroupHits().get(0).getFields().get("rank"));
    Assert.assertEquals("4.5", result.getHits().get(0).getGroupHits().get(1).getFields().get("rank"));
    Assert.assertEquals(1, result.getHits().get(1).getGroupHitsSize());
    Assert.assertEquals("10.0", result.getHits().get(1).getGroupHits().get(0).getFields().get("rank"));
    Assert.assertEquals(2, result.getHits().get(2).getGroupHitsSize());
    Assert.assertEquals("4.0", result.getHits().get(2).getGroupHits().get(0).getFields().get("rank"));
    Assert.assertEquals("1.2", result.getHits().get(2).getGroupHits().get(1).getFields().get("rank"));
  }

  // dynamic field
  @Test
  public void groupSearchTest3() throws IOException {
    String bql = "select * from linden order by rank group by cat1.STRING TOP 3 limit 0,10 source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    Assert.assertEquals(true, request.isSetGroupParam());
    Assert.assertEquals("cat1", request.getGroupParam().getGroupField());
    Assert.assertEquals(3, request.getGroupParam().getGroupInnerLimit());

    LindenResult result = lindenCore.search(request);

    Assert.assertEquals(true, result.isSuccess());
    Assert.assertEquals(5, result.getTotalHits());
    Assert.assertEquals(3, result.getTotalGroups());
    Assert.assertEquals(3, result.getHitsSize());
    Assert.assertEquals(2, result.getHits().get(0).getGroupHitsSize());
    Assert.assertEquals("10.3", result.getHits().get(0).getGroupHits().get(0).getFields().get("rank"));
    Assert.assertEquals("4.5", result.getHits().get(0).getGroupHits().get(1).getFields().get("rank"));
    Assert.assertEquals(1, result.getHits().get(1).getGroupHitsSize());
    Assert.assertEquals("10.0", result.getHits().get(1).getGroupHits().get(0).getFields().get("rank"));
    Assert.assertEquals(2, result.getHits().get(2).getGroupHitsSize());
    Assert.assertEquals("4.0", result.getHits().get(2).getGroupHits().get(0).getFields().get("rank"));
    Assert.assertEquals("1.2", result.getHits().get(2).getGroupHits().get(1).getFields().get("rank"));
  }

}
