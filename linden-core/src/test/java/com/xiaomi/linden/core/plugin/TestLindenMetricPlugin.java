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

package com.xiaomi.linden.core.plugin;

import java.io.IOException;

import com.google.common.base.Throwables;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.core.LindenSchemaBuilder;
import com.xiaomi.linden.core.TestLindenCoreBase;
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

public class TestLindenMetricPlugin extends TestLindenMetricBase{

  public TestLindenMetricPlugin() throws Exception {
    try {
      handleRequest(
          "{\"id\":1, \"title\": \"lucene 1\", \"field1\": \"aaa\", \"field2\": \"aaa aaa\", \"rank\": 1.2, \"cat1\":1, \"cat2\":1.5, \"hotwords\":\"hello\"}");
      handleRequest(
          "{\"id\":2, \"title\": \"lucene 2\", \"field1\": \"bbb\", \"rank\": 4.5, \"cat1\":2, \"cat2\":2.5, \"tagnum\": 100}");
      handleRequest(
          "{\"id\":3, \"title\": \"lucene 3\", \"field1\": \"ccc\", \"rank\": 4.5, \"cat1\":3, \"cat2\":3.5, \"tagstr\":\"ok\"}");
      handleRequest(
          "{\"type\": \"index\", \"content\": {\"id\":4, \"title\": \"lucene 4\", \"field1\": \"ddd\", \"rank\": 10.3, \"cat1\":4, \"cat2\":4.5}}");
      handleRequest(
          "{\"id\":5, \"title\": \"lucene 5\", \"field1\": \"ddd\", \"rank\": 10.3, \"cat1\":5, \"cat2\":5.5}");
      handleRequest(
          "{\"id\":6, \"tagstr\":[\"MI4\", \"MI Note\"]}");
      handleRequest(
          "{\"id\":7, \"tagstr\":[\"MI4C\", \"MI Note Pro\"]}");
      handleRequest("{\"type\": \"delete\", \"id\" : \"5\"}");
      lindenCore.commit();
      lindenCore.refresh();
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
    schema.addToFields(new LindenFieldSchema().setName("tagstr").setIndexed(true));
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


}
