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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenType;

public class TestLindenFieldCache extends TestLindenCoreBase {

  public TestLindenFieldCache() throws Exception {
    try {
      for (int i = 0; i < 10; ++i) {
        JSONObject json = new JSONObject();
        json.put("id", "doc_" + i);
        json.put("title", "lucene " + i);
        JSONArray jsonArray = new JSONArray();
        for (int j = 0; j < 3; ++j) {
          jsonArray.add(i + j);
        }
        json.put("ids_int", jsonArray);
        json.put("ids_long", jsonArray);
        json.put("ids_float", jsonArray);
        json.put("ids_double", jsonArray);
        jsonArray = new JSONArray();
        for (int j = 0; j < 3; ++j) {
          jsonArray.add(String.valueOf(i + j));
        }
        json.put("ids_str", jsonArray);
        json.put("rank", i * 10);

        handleRequest(json.toJSONString());
      }
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
    schema.addToFields(new LindenFieldSchema().setName("title").setIndexed(true).setTokenized(true));
    schema.addToFields(new LindenFieldSchema().setName("ids_int").setType(LindenType.INTEGER).setIndexed(true)
                           .setTokenized(true).setMulti(true));
    schema.addToFields(new LindenFieldSchema().setName("ids_long").setType(LindenType.INTEGER).setIndexed(true)
                           .setTokenized(true).setMulti(true));
    schema.addToFields(new LindenFieldSchema().setName("ids_str").setType(LindenType.STRING).setIndexed(true)
                           .setTokenized(true).setMulti(true));
    schema.addToFields(new LindenFieldSchema().setName("ids_float").setType(LindenType.FLOAT).setIndexed(true)
                           .setTokenized(false).setMulti(true));
    schema.addToFields(new LindenFieldSchema().setName("ids_double").setType(LindenType.DOUBLE).setIndexed(true)
                           .setTokenized(false).setMulti(true));
    schema.addToFields(new LindenFieldSchema().setName("rank").setType(LindenType.FLOAT).setIndexed(true));
    lindenConfig.setSchema(schema);
  }

  @Test
  public void intListTest() throws IOException {
    String bql = "select * from linden by query is \"title:lucene\" " +
                 "using score model test " +
                 "begin " +
                 "    float sum = 0;\n" +
                 "    for (int a : ids_int()) {\n" +
                 "      sum += a;\n" +
                 "    }\n" +
                 "    return sum;\n" +
                 "end";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(10, result.getTotalHits());
    Assert.assertEquals(30, result.getHits().get(0).getScore(), 0.1f);

    bql = "select * from linden where ids_int = 2  " +
          "using score model test " +
          "begin " +
          "    float sum = 0;\n" +
          "    for (int a : ids_int()) {\n" +
          "      sum += a;\n" +
          "    }\n" +
          "    return sum;\n" +
          "end";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(3, result.getTotalHits());
    Assert.assertEquals(9, result.getHits().get(0).getScore(), 0.1f);
  }

  @Test
  public void stringListTest() throws IOException {
    String bql = "select * from linden by query is \"title:lucene\" " +
                 "using score model test " +
                 "begin " +
                 "  int base = doc();\n" +
                 "  for (String str : ids_str()) {\n" +
                 "    if (base++ != Integer.valueOf(str)) {\n" +
                 "      return -1.0;\n" +
                 "    }\n" +
                 "  }\n" +
                 "  return 1.0;\n" +
                 "end";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(10, result.getTotalHits());
    for (int i = 0; i < 10; ++i) {
      Assert.assertTrue(result.getHits().get(i).score > 0.0);
    }
  }

  @Test
  public void floatListTest() throws IOException {
    String bql = "select * from linden by query is \"title:lucene\" " +
                 "using score model test " +
                 "begin " +
                 "  int base = doc();\n" +
                 "  for (float f : ids_float()) {\n" +
                 "    if (f != (float)(base++)) {\n" +
                 "      return -1.0;\n" +
                 "    }\n" +
                 "  }\n" +
                 "  return 1.0;\n" +
                 "end";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(10, result.getTotalHits());
    for (int i = 0; i < 10; ++i) {
      Assert.assertTrue(result.getHits().get(i).score > 0.0);
    }
  }

  @Test
  public void doubleListTest() throws IOException {
    String bql = "select * from linden by query is \"title:lucene\" " +
                 "using score model test " +
                 "begin " +
                 "  int base = doc();\n" +
                 "  for (double d : ids_double()) {\n" +
                 "    if (d != (double)(base++)) {\n" +
                 "      return -1.0;\n" +
                 "    }\n" +
                 "  }\n" +
                 "  return 1.0;\n" +
                 "end";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(10, result.getTotalHits());
    for (int i = 0; i < 10; ++i) {
      Assert.assertTrue(result.getHits().get(i).score > 0.0);
    }
  }

  @Test
  public void idTest() throws IOException {
    String bql = "select * from linden by query is \"title:lucene\" " +
                 "using score model test " +
                 "begin " +
                 "  String expectedId = \"doc_\" + doc();\n" +
                 "  if (!id().equals(expectedId)) {\n" +
                 "      return -1.0;\n" +
                 "  }\n" +
                 "return 1.0;\n" +
                 "end";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(10, result.getTotalHits());
    for (int i = 0; i < 10; ++i) {
      Assert.assertTrue(result.getHits().get(i).score > 0.0);
    }
  }
}
