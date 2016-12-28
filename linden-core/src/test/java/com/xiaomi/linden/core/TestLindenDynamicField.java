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
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.thrift.common.LindenDocument;
import com.xiaomi.linden.thrift.common.LindenField;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenType;


public class TestLindenDynamicField extends TestLindenCoreBase {

  public LindenSchema schema;
  public String jsonStr = "{\n"
                 + "  \"id\": \"1\",\n"
                 + "  \"name\": \"appstore-search\",\n"
                 + "  \"level\": \"info\",\n"
                 + "  \"log\": \"search result is empty\",\n"
                 + "  \"host\": \"xiaomi-search01.bj\",\n"
                 + "  \"shard\": \"1\",\n"
                 + "  \"_dynamic\": [\n"
                 + "    {\n"
                 + "      \"mgroup\": \"misearch\",\n"
                 + "      \"_type\": \"string\"\n"
                 + "    },\n"
                 + "    {\n"
                 + "      \"cost\": 30,\n"
                 + "      \"_type\": \"long\",\n"
                 + "    },\n"
                 + "    {\n"
                 + "      \"num\": 7.7,\n"
                 + "      \"_type\": \"float\",\n"
                 + "    },\n"
                 + "    {\n"
                 + "      \"count\": 3,\n"
                 + "      \"_type\": \"int\",\n"
                 + "    },\n"
                 + "    {\n"
                 + "      \"val\": \"10.0\",\n"
                 + "      \"_type\": \"double\",\n"
                 + "    }\n"
                 + "    {\n"
                 + "      \"text\": \"this is a tokenized string field\",\n"
                 + "      \"_tokenize\": \"true\",\n"
                 + "    }\n"
                 + "  ]\n"
                 + "}";

  public TestLindenDynamicField() throws Exception {
    try {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("type", "index");
      jsonObject.put("content", JSONObject.parseObject(jsonStr));
      handleRequest(jsonObject.toString());
      lindenCore.commit();
      lindenCore.refresh();
      bqlCompiler = new BQLCompiler(lindenConfig.getSchema());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void init() {
    schema = new LindenSchema().setId("id");
    schema.addToFields(
        new LindenFieldSchema("name", LindenType.STRING).setIndexed(true).setTokenized(true)
            .setStored(true));
    schema.addToFields(
        new LindenFieldSchema("level", LindenType.STRING).setIndexed(true).setStored(true));
    schema.addToFields(
        new LindenFieldSchema("log", LindenType.STRING).setIndexed(true).setTokenized(true)
            .setStored(true));
    schema.addToFields(
        new LindenFieldSchema("host", LindenType.STRING).setIndexed(true).setStored(true));
    schema.addToFields(
        new LindenFieldSchema("shard", LindenType.INTEGER).setIndexed(true).setStored(true));
    lindenConfig.setSchema(schema);
  }

  @Test
  public void testBuildField() throws IOException {
    LindenDocument lindenDocument = LindenDocumentBuilder.build(schema, JSON.parseObject(jsonStr));
    List<LindenField> fields = lindenDocument.getFields();
    Assert.assertEquals(true, fields.contains(new LindenField(new LindenFieldSchema()
                                                                  .setName("mgroup")
                                                                  .setType(LindenType.STRING)
                                                                  .setIndexed(true)
                                                                  .setStored(true),
                                                              "misearch")));
    Assert.assertEquals(true, fields.contains(new LindenField(new LindenFieldSchema()
                                                                  .setName("cost")
                                                                  .setType(LindenType.LONG)
                                                                  .setIndexed(true)
                                                                  .setStored(true),
                                                              "30")));
  }

  @Test
  public void testDynamicField() throws IOException {
    String
        bql =
        "select id,name,level,log,host,shard,cost.long,mgroup,num.float,count.int,val.double from linden by " +
        " query is 'name:appstore' where query is 'cost.long:{30 TO 340]' " +
        " source ";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(0, result.getHitsSize());

    bql = "select id,name,level,log,host,shard,cost.long,mgroup,num.float,count.int,val.double from linden by " +
          " query is 'name:appstore cost.long:{30 TO 340]' " +
          " source ";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    JSONObject hit0Source = JSON.parseObject(result.hits.get(0).getSource());
    Assert.assertEquals("1", hit0Source.getString("id"));
    Assert.assertEquals(30L, hit0Source.getLongValue("cost"));

    bql = "select id,name,level,log,host,shard,cost.long,mgroup,num.float,count.int,val.double from linden by " +
          " query is 'name:appstore' where query is 'cost.long:[30 TO 340]' " +
          " source ";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    hit0Source = JSON.parseObject(result.hits.get(0).getSource());
    Assert.assertEquals("1", hit0Source.getString("id"));
    Assert.assertEquals(30L, hit0Source.getLongValue("cost"));

    bql = "select id,name,level,log,host,shard,cost.long,mgroup,num.float,count.int,val.double from linden by " +
          " query is '+name:appstore +cost.long:{30 TO 340]' " +
          " source ";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(0, result.getHitsSize());

    bql = "select id,name,level,log,host,shard,cost.long,mgroup,num.float,count.int,val.double from linden by " +
          " query is 'name:appstore' where query is 'num.float:[3 TO 340]' " +
          " source ";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    hit0Source = JSON.parseObject(result.hits.get(0).getSource());
    Assert.assertEquals("1", hit0Source.getString("id"));
    Assert.assertEquals(7.7f, hit0Source.getFloatValue("num"), 0.01);

    bql = "select id,name,level,log,host,shard,cost.long,mgroup,num.float,count.int,val.double from linden by " +
          " query is 'name:appstore' where query is 'count.int:[3 TO 340]' " +
          " source ";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    hit0Source = JSON.parseObject(result.hits.get(0).getSource());
    Assert.assertEquals("1", hit0Source.getString("id"));
    Assert.assertEquals(3, hit0Source.getIntValue("count"));

    bql = "select id,name,level,log,host,shard,cost.long,mgroup,num.float,count.int,val.double from linden by " +
          " query is 'name:appstore' where query is 'val.double:[3 TO 340]' " +
          " source ";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    hit0Source = JSON.parseObject(result.hits.get(0).getSource());
    Assert.assertEquals("1", hit0Source.getString("id"));
    Assert.assertEquals(10.0, hit0Source.getDoubleValue("val"), 0.01);

    bql = "select id,name,level,log,host,shard,cost.long,mgroup,num.float,count.int,val.double from linden by " +
          " query is 'name:appstore' order by val.double source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    hit0Source = JSON.parseObject(result.hits.get(0).getSource());
    Assert.assertEquals("1", hit0Source.getString("id"));
    Assert.assertEquals(10.0, hit0Source.getDoubleValue("val"), 0.01);
    Assert.assertEquals("10.0", result.getHits().get(0).getFields().get("val"));

    bql = "select text.string from linden by query is 'name:appstore' source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    hit0Source = JSON.parseObject(result.hits.get(0).getSource());
    Assert.assertEquals("1", hit0Source.getString("id"));
    Assert.assertEquals("this is a tokenized string field", hit0Source.getString("text"));

    bql = "select text from linden by query is 'name:appstore' source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    hit0Source = JSON.parseObject(result.hits.get(0).getSource());
    Assert.assertEquals("1", hit0Source.getString("id"));
    Assert.assertEquals("this is a tokenized string field", hit0Source.getString("text"));

    bql = "select text from linden by query is 'text:field' source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    hit0Source = JSON.parseObject(result.hits.get(0).getSource());
    Assert.assertEquals("1", hit0Source.getString("id"));
    Assert.assertEquals("this is a tokenized string field", hit0Source.getString("text"));
  }
}
