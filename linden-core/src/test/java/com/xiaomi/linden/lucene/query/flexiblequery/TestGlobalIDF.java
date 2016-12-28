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

package com.xiaomi.linden.lucene.query.flexiblequery;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.core.TestLindenCoreBase;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestGlobalIDF extends TestLindenCoreBase {
  public TestGlobalIDF() throws Exception {
    try {
      handleRequest("{\"id\":1, \"title\": \"lucene 1\", \"field1\": \"lucene aaa\", \"field2\": \"lucene id1\", \"rank\": 1.2, \"cat1\":1, \"cat2\":1.5}");
      handleRequest("{\"id\":2, \"title\": \"lucene 2\", \"field1\": \"lucene bbb ccc\", \"field2\": \"id2\", \"rank\": 4.5, \"cat1\":2, \"cat2\":2.5, \"tagnum\": 100}");
      handleRequest("{\"id\":3, \"title\": \"lucene 3\", \"field1\": \"ccc\", \"field2\": \"lucene id3\", \"rank\": 4.5, \"cat1\":3, \"cat2\":3.5, \"tagstr\":\"ok\"}");
      handleRequest("{\"type\": \"index\", \"content\": {\"id\":4, \"title\": \"lucene 4\", \"field1\": \"ddd\", \"field2\": \"lucene id4\", \"rank\": 10.3, \"cat1\":4, \"cat2\":4.5}}");
      handleRequest("{\"id\":5, \"title\": \"lucene 5\", \"field1\": \"ddd\", \"field2\": \"id5\", \"rank\": 10.3, \"cat1\":5, \"cat2\":5.5}");
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
    schema.addToFields(new LindenFieldSchema().setName("title").setIndexed(true).setStored(true).setTokenized(true).setOmitNorms(true));
    schema.addToFields(new LindenFieldSchema().setName("field1").setIndexed(true).setStored(true).setTokenized(true).setOmitNorms(true));
    schema.addToFields(new LindenFieldSchema().setName("field2").setIndexed(true).setStored(true).setTokenized(true).setOmitNorms(true));
    schema.addToFields(new LindenFieldSchema().setName("rank").setType(LindenType.FLOAT).setIndexed(true).setStored(true));
    schema.addToFields(new LindenFieldSchema().setName("cat1").setType(LindenType.INTEGER).setIndexed(true).setStored(true));
    schema.addToFields(new LindenFieldSchema().setName("cat2").setType(LindenType.DOUBLE).setIndexed(true).setStored(true));
    schema.addToFields(new LindenFieldSchema().setName("tagstr").setIndexed(true));
    schema.addToFields(new LindenFieldSchema().setName("tagnum").setType(LindenType.INTEGER).setIndexed(true));
    lindenConfig.setSchema(schema);
  }

  @Test
  public void testGlobalIdf() throws IOException {
    String bql = "select * from linden by flexible_query is 'lucene' global_idf in (title, field1)\n" +
        "using model test\n" +
        "begin\n" +
        "    return 1;\n" +
        "end\n" +
        "source explain;";

    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(result.getHits().get(0).getExplanation().getDetails().get(0).getDetails().get(0).getDescription(),
        result.getHits().get(1).getExplanation().getDetails().get(0).getDetails().get(0).getDescription());

  }

  @Test
  public void testGlobalIdfV2() throws IOException {
    String bql1 = "select * from linden by flexible_query is 'lucene' global_idf of (title, field1) in (field1)\n" +
        "using model test\n" +
        "begin\n" +
        "    return 1;\n" +
        "end\n" +
        "source explain;";
    LindenSearchRequest request1 = bqlCompiler.compile(bql1).getSearchRequest();
    LindenResult result1 = lindenCore.search(request1);
    String detail1 = result1.getHits().get(0).getExplanation().getDetails().get(0).getDetails().get(0).getDescription();
    Assert.assertEquals(detail1.split(" \\* ")[0], "0.82");

    String bql2 = "select * from linden by flexible_query is 'lucene' global_idf of (field1, field2) in (field1)\n" +
        "using model test\n" +
        "begin\n" +
        "    return 1;\n" +
        "end\n" +
        "source explain;";
    LindenSearchRequest request2 = bqlCompiler.compile(bql2).getSearchRequest();
    LindenResult result2 = lindenCore.search(request2);
    String detail2 = result2.getHits().get(0).getExplanation().getDetails().get(0).getDetails().get(0).getDescription();
    Assert.assertEquals(detail2.split(" \\* ")[0], "1.22");

    String bql3 = "select * from linden by flexible_query is 'lucene' global_idf in (field1)\n" +
        "using model test\n" +
        "begin\n" +
        "    return 1;\n" +
        "end\n" +
        "source explain;";
    LindenSearchRequest request3 = bqlCompiler.compile(bql3).getSearchRequest();
    LindenResult result3 = lindenCore.search(request3);
    String detail3 = result3.getHits().get(0).getExplanation().getDetails().get(0).getDetails().get(0).getDescription();
    Assert.assertEquals(detail3.split(" \\* ")[0], "1.51");  }
}

