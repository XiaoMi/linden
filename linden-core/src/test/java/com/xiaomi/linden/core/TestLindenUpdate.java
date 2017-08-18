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
import com.xiaomi.linden.thrift.common.LindenType;
import com.xiaomi.linden.thrift.common.Response;

/**
 * Created by vaccine on 14-10-22.
 */
public class TestLindenUpdate extends TestLindenCoreBase {

  public TestLindenUpdate() throws Exception {
    try {
      handleRequest(
          "{\"id\":1, \"title\": \"lucene 1\", \"field1\": \"aaa\", \"rank\": 1.2, \"cat1\":1, \"cat2\":1.5}");
      handleRequest(
          "{\"id\":2, \"title\": \"lucene 2\", \"field1\": \"bbb\", \"rank\": 4.5, \"cat1\":2, \"cat2\":2.5, \"tagnum\": [100]}");
      handleRequest(
          "{\"id\":3, \"title\": \"lucene 3\", \"field1\": \"ccc\", \"rank\": 4.5, \"cat1\":3, \"cat2\":3.5, \"tagstr\":\"ok\"}");
      handleRequest(
          "{\"type\": \"index\", \"content\": {\"id\":4, \"title\": \"lucene 4\", \"field1\": \"ddd\", \"rank\": 10.3, \"cat1\":4, \"cat2\":4.5}}");
      handleRequest(
          "{\"id\":5, \"title\": \"lucene 5\", \"field1\": \"ddd\", \"rank\": 10.3, \"cat1\":5, \"cat2\":5.5}");
      handleRequest("{\"type\": \"delete\", \"id\" : \"5\"}");
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
    schema.addToFields(new LindenFieldSchema().setName("field1").setDocValues(true));
    schema.addToFields(new LindenFieldSchema().setName("rank").setType(LindenType.FLOAT).setIndexed(true));
    schema.addToFields(new LindenFieldSchema().setName("cat1").setType(LindenType.INTEGER).setStored(true));
    schema.addToFields(new LindenFieldSchema().setName("cat2").setType(LindenType.DOUBLE).setDocValues(true));
    schema.addToFields(
        new LindenFieldSchema().setName("tagstr").setType(LindenType.STRING).setIndexed(true).setDocValues(true));
    schema.addToFields(
        new LindenFieldSchema().setName("tagnum").setType(LindenType.INTEGER).setIndexed(true).setStored(true)
            .setMulti(true));
    lindenConfig.setSchema(schema);
    lindenConfig.setEnableSourceFieldCache(true);
  }

  @Test
  public void updateDocValueTest() throws Exception {
    // update document id3
    // using default update type: doc_value
    handleRequest("{\"type\": \"update\", \"content\": {\"id\":3, \"field1\":\"ccc_c\", \"cat2\":33.5}}");

    lindenCore.refresh();

    String bql = "select * from linden by query is \'title:\"lucene 3\"\' source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(1, result.getHitsSize());
    Assert.assertEquals(
        "{\"cat1\":3,\"cat2\":33.5,\"field1\":\"ccc_c\",\"id\":\"3\",\"rank\":4.5,\"tagstr\":\"ok\",\"title\":\"lucene 3\"}",
        result.getHits().get(0).getSource());

    // this case won't go docvalue path, since tagstr is not only docvalue but also indexed
    handleRequest("{\"type\": \"update\", \"content\": {\"id\":3, \"tagstr\":\"not ok\"}}");
    lindenCore.refresh();
    bql = "select * from linden where tagstr = 'not ok' source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getHitsSize());
    Assert.assertEquals(
        "{\"cat1\":3,\"cat2\":33.5,\"field1\":\"ccc_c\",\"id\":\"3\",\"rank\":4.5,\"tagstr\":\"not ok\",\"title\":\"lucene 3\"}",
        result.getHits().get(0).getSource());
  }

  @Test
  public void updateIndex() throws Exception {
    // update document 3
    handleRequest("{\"type\": \"update\", \"content\": {\"id\":3, \"tagnum\":[10]}}");

    lindenCore.refresh();

    String bql = "select * from linden by query is 'id:3' source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(1, result.getHitsSize());
    Assert.assertEquals(
        "{\"cat1\":3,\"cat2\":3.5,\"field1\":\"ccc\",\"id\":\"3\",\"rank\":4.5,\"tagnum\":[10],\"tagstr\":\"ok\",\"title\":\"lucene 3\"}",
        result.getHits().get(0).getSource());

    handleRequest("{\"type\": \"update\", \"content\": {\"id\":3, \"tagnum\":[6,7]}}");

    lindenCore.refresh();
    result = lindenCore.search(request);
    Assert.assertEquals(1, result.getHitsSize());
    Assert.assertEquals(
        "{\"cat1\":3,\"cat2\":3.5,\"field1\":\"ccc\",\"id\":\"3\",\"rank\":4.5,\"tagnum\":[6,7],\"tagstr\":\"ok\",\"title\":\"lucene 3\"}",
        result.getHits().get(0).getSource());
  }

  @Test
  public void updateNonexistentDoc() throws Exception {
    Response response = handleRequest("{\"type\": \"update\", \"content\": {\"id\":6, \"title\":\"lucene 6\"}}");
    Assert.assertFalse(response.isSuccess());
  }
}
