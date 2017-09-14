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

import com.alibaba.fastjson.JSONObject;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestLindenFacet extends TestLindenCoreBase {

  public TestLindenFacet() throws Exception {
    try {
      handleRequest(generateIndexRequest("1", "cn",
          "Bob book 1", "Bob", "2010/10/15"));
      handleRequest(generateIndexRequest("2", "cn",
          "Lisa book 1", "Lisa", "2010/10/20"));
      handleRequest(generateIndexRequest("3", "en",
          "Lisa book 2", "Lisa", "2012/1/1"));
      handleRequest(generateIndexRequest("4", "en",
          "Susan book 1", "Susan", "2012/1/7"));
      handleRequest(generateIndexRequest("5", "en",
          "Frank book 1", "Frank", "1999/5/5"));
      handleRequest(generateIndexRequest("6", "cn",
          "Bob book 2", "Bob", "2010/10/25"));
      handleRequest(generateIndexRequest("7", "cn",
          "Lisa book 3", "Lisa", "2010/11/20"));
      handleRequest(generateIndexRequest("8", "en",
          "Lisa book 4", "Lisa", "2012/2/1"));
      handleRequest(generateIndexRequest("9", "en",
          "Susan book 2", "Susan", "2013/1/7"));
      handleRequest(generateIndexRequest("10", "en",
          "Susan book 3", "Susan", "1999/5/15"));
      handleRequest(generateIndexRequest("11", "en",
          "Anonymous book", null, "1999/2/1"));
      handleRequest(generateIndexRequest("12", "en",
          "No PubDate book", "Susan", null));
      handleRequest(generateIndexRequest("13", "cn",
          "Book without any info", null, null));

      lindenCore.commit();
      lindenCore.refresh();
      bqlCompiler = new BQLCompiler(lindenConfig.getSchema());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String generateIndexRequest(String id, String language, String title,
      String author, String pubDate) {
    JSONObject json = new JSONObject();
    json.put("id", id);
    json.put("Language", language);
    json.put("Title", title);
    if (author != null) {
      json.put("Author", author);
    }
    if (pubDate != null) {
      json.put("PublishDate", pubDate);
    }
    return json.toString();
  }

  public static String generateDeleteRequest(String id) {
    JSONObject json = new JSONObject();
    json.put("type", "delete");
    json.put("id", id);
    return json.toString();
  }

  @Override
  public void init() throws Exception {
    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(new LindenFieldSchema().setName("Language").setTokenized(false).setIndexed(true));
    schema.addToFields(new LindenFieldSchema().setName("Title").setTokenized(true).setIndexed(true));
    schema.addToFields(new LindenFieldSchema().setName("Author").setType(LindenType.FACET).setStored(true).setIndexed(true));
    schema.addToFields(new LindenFieldSchema().setName("PublishDate").setType(LindenType.FACET).setStored(true));
    lindenConfig.setSchema(schema);

    lindenConfig.putToProperties("cluster.url", "localhost:2183/facettest");
    lindenConfig.putToProperties("port", "19090");
    lindenConfig.putToProperties("shard.id", "0");
    lindenConfig.putToProperties("index.directory", "/tmp/linden-facet-test/index/0");
    lindenConfig.putToProperties("log.path", "/tmp/linden-facet-test/log/0");
    lindenConfig.putToProperties("search.timeout", "0");
  }

  // No query, no filter
  @Test
  public void facetTest1() throws Exception {
    LindenSearchRequest request = new LindenSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(13, result.getTotalHits());

    String bql = "select * from linden browse by Author, PublishDate, PublishDate(2), PublishDate(10, '2010'), "
        + "PublishDate(10, '2010/10'), PublishDate(10, '2010/10/15'),"
        + "PublishDate(10, '2010/13'), Author(10, 'non-existent-path')";

    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(13, result.getTotalHits());
    Assert.assertEquals(8, result.getFacetResultsSize());

    // Author facet
    Assert.assertEquals("LindenFacetResult(dim:Author, value:11, childCount:4, " +
            "labelValues:[LindenLabelAndValue(label:Lisa, value:4), " +
            "LindenLabelAndValue(label:Susan, value:4), " +
            "LindenLabelAndValue(label:Bob, value:2), " +
            "LindenLabelAndValue(label:Frank, value:1)])",
        result.getFacetResults().get(0).toString());

    // PublishDate facet
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, value:11, childCount:4, " +
            "labelValues:[LindenLabelAndValue(label:2010, value:4), " +
            "LindenLabelAndValue(label:2012, value:3), " +
            "LindenLabelAndValue(label:1999, value:3), " +
            "LindenLabelAndValue(label:2013, value:1)])",
        result.getFacetResults().get(1).toString());

    // Top 2 PublishDate facet
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, value:11, childCount:4, " +
            "labelValues:[LindenLabelAndValue(label:2010, value:4), " +
            "LindenLabelAndValue(label:2012, value:3)])",
        result.getFacetResults().get(2).toString());

    // PublishDate facet under path 2010
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010, value:4, childCount:2, " +
            "labelValues:[LindenLabelAndValue(label:10, value:3), " +
            "LindenLabelAndValue(label:11, value:1)])",
        result.getFacetResults().get(3).toString());

    // PublishDate facet under path 2010,10
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010/10, value:3, childCount:3, " +
            "labelValues:[LindenLabelAndValue(label:15, value:1), " +
            "LindenLabelAndValue(label:20, value:1), " +
            "LindenLabelAndValue(label:25, value:1)])",
        result.getFacetResults().get(4).toString());


    // PublishDate facet under path 2010,10,15
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010/10/15, value:0, childCount:0)",
        result.getFacetResults().get(5).toString());

    // Non-existent-path
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010/13, value:0, childCount:0)",
        result.getFacetResults().get(6).toString());

    // Non-existent-path
    Assert.assertEquals("LindenFacetResult(dim:Author, path:non-existent-path, value:0, childCount:0)",
        result.getFacetResults().get(7).toString());
  }

  // test with query
  @Test
  public void facetTest2() throws Exception {
    LindenSearchRequest request = new LindenSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(13, result.getTotalHits());

    String bql = "select * from linden by query is 'Title:Lisa' browse by Author, PublishDate";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals(2, result.getFacetResultsSize());

    // Author facet
    Assert.assertEquals("LindenFacetResult(dim:Author, value:4, childCount:1, " +
            "labelValues:[LindenLabelAndValue(label:Lisa, value:4)])",
        result.getFacetResults().get(0).toString());

    // PublishDate facet
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, value:4, childCount:2, " +
            "labelValues:[LindenLabelAndValue(label:2010, value:2), LindenLabelAndValue(label:2012, value:2)])",
        result.getFacetResults().get(1).toString());
  }

  // test with query and filter
  @Test
  public void facetTest3() throws Exception {
    LindenSearchRequest request = new LindenSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(13, result.getTotalHits());

    String bql = "select * from linden by query is 'Title:Lisa' where Language='en' browse by Author, PublishDate";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getTotalHits());
    Assert.assertEquals(2, result.getFacetResultsSize());

    // Author facet
    Assert.assertEquals("LindenFacetResult(dim:Author, value:2, childCount:1, " +
            "labelValues:[LindenLabelAndValue(label:Lisa, value:2)])",
        result.getFacetResults().get(0).toString());

    // PublishDate facet
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, value:2, childCount:1, " +
            "labelValues:[LindenLabelAndValue(label:2012, value:2)])",
        result.getFacetResults().get(1).toString());
  }


  // Drill down
  @Test
  public void facetTest4() throws Exception {
    LindenSearchRequest request = new LindenSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(13, result.getTotalHits());

    String bql = "select * from Linden browse by Author, PublishDate drill down PublishDate('2010')";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals(2, result.getFacetResultsSize());

    // Author facet
    Assert.assertEquals("LindenFacetResult(dim:Author, value:4, childCount:2, "
            + "labelValues:[LindenLabelAndValue(label:Bob, value:2), LindenLabelAndValue(label:Lisa, value:2)])",
        result.getFacetResults().get(0).toString());

    // PublishDate facet
    Assert.assertEquals(
        "LindenFacetResult(dim:PublishDate, value:4, childCount:1, labelValues:[LindenLabelAndValue(label:2010, value:4)])",
        result.getFacetResults().get(1).toString());
  }

  // Drill sideways
  @Test
  public void facetTest5() throws Exception {
    LindenSearchRequest request = new LindenSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(13, result.getTotalHits());

    String bql = "select * from Linden browse by Author, PublishDate drill sideways PublishDate('2010')";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals(2, result.getFacetResultsSize());

    // Author facet under PublishDate 2010
    Assert.assertEquals("LindenFacetResult(dim:Author, value:4, childCount:2, " +
            "labelValues:[LindenLabelAndValue(label:Bob, value:2), LindenLabelAndValue(label:Lisa, value:2)])",
        result.getFacetResults().get(0).toString());

    // PublishDate facet is not effected by drill down Path
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, value:11, childCount:4, " +
            "labelValues:[LindenLabelAndValue(label:2010, value:4), LindenLabelAndValue(label:2012, value:3), " +
            "LindenLabelAndValue(label:1999, value:3), LindenLabelAndValue(label:2013, value:1)])",
        result.getFacetResults().get(1).toString());
  }

  // Early termination
  @Test
  public void facetTest6() throws Exception {
    LindenSearchRequest request = new LindenSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(13, result.getTotalHits());

    String bql = "select * from Linden browse by Author, PublishDate in top 5";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(5, result.getTotalHits());
    Assert.assertEquals(2, result.getFacetResultsSize());

    // Author facet
    Assert.assertEquals("LindenFacetResult(dim:Author, value:5, childCount:4, "
                        + "labelValues:[LindenLabelAndValue(label:Lisa, value:2), LindenLabelAndValue(label:Bob, value:1), "
                        + "LindenLabelAndValue(label:Susan, value:1), LindenLabelAndValue(label:Frank, value:1)])",
        result.getFacetResults().get(0).toString());

    // PublishDate facet
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, value:5, childCount:3, "
                        + "labelValues:[LindenLabelAndValue(label:2010, value:2), "
                        + "LindenLabelAndValue(label:2012, value:2), "
                        + "LindenLabelAndValue(label:1999, value:1)])",
        result.getFacetResults().get(1).toString());
  }

  // test with delete
  @Test
  public void facetTest7() throws Exception {
    LindenSearchRequest request = new LindenSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(13, result.getTotalHits());

    handleRequest(generateDeleteRequest("1"));
    lindenCore.commit();
    lindenCore.refresh();

    String bql = "select * from Linden browse by Author, PublishDate";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(12, result.getTotalHits());
    Assert.assertEquals(2, result.getFacetResultsSize());

    // Author facet
    Assert.assertEquals("LindenFacetResult(dim:Author, value:10, childCount:4, " +
            "labelValues:[LindenLabelAndValue(label:Lisa, value:4), " +
            "LindenLabelAndValue(label:Susan, value:4), " +
            "LindenLabelAndValue(label:Bob, value:1), " +
            "LindenLabelAndValue(label:Frank, value:1)])",
        result.getFacetResults().get(0).toString());

    // PublishDate facet
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, value:10, childCount:4, " +
            "labelValues:[LindenLabelAndValue(label:2010, value:3), " +
            "LindenLabelAndValue(label:2012, value:3), " +
            "LindenLabelAndValue(label:1999, value:3), " +
            "LindenLabelAndValue(label:2013, value:1)])",
        result.getFacetResults().get(1).toString());

    handleRequest(generateIndexRequest("1", "cn",
        "Bob book 1", "Bob", "2010/10/15"));
    lindenCore.commit();
    lindenCore.refresh();
  }

  // test facet source
  @Test
  public void facetTest8() throws Exception {
    String bql = "select * from Linden browse by Author, PublishDate source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(13, result.getTotalHits());
    Assert.assertEquals(2, result.getFacetResultsSize());
    Assert.assertEquals("LindenHit(id:1, score:1.0, "
                        + "source:{\"Author\":\"Bob\",\"Language\":\"cn\",\"PublishDate\":\"2010/10/15\",\"id\":\"1\"})",
                        result.getHits().get(0).toString());
    bql = "select Author from Linden browse by Author, PublishDate source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(result.getHits().get(0).toString(),
        "LindenHit(id:1, score:1.0, source:{\"Author\":\"Bob\",\"id\":\"1\"})");
  }

  // test facet field index as normal field
  @Test
  public void facetTest9() throws Exception {
    String bql = "select * from Linden where Author in ('Bob', 'Frank') source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(3, result.getHitsSize());
    bql = "select * from Linden where Author not in ('Bob', 'Frank') source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(10, result.getHitsSize());

    bql = "select * from Linden query Author = 'Susan' source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getHitsSize());

    bql = "select * from Linden where Author > 'A' and Author < 'G' source";
    // Bob and Frank
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(3, result.getHitsSize());

    bql = "select * from Linden where Author is null source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getHitsSize());

    bql = "select * from Linden where Author is not null source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(10, result.getHitsSize());
  }

  // dynamic field
  @Test
  public void facetTest10() throws Exception {
    String bql = "select * from Linden browse by Author.FACET, PublishDate.facet source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(13, result.getTotalHits());
    Assert.assertEquals(2, result.getFacetResultsSize());
    Assert.assertEquals("LindenHit(id:1, score:1.0, "
                        + "source:{\"Author\":\"Bob\",\"Language\":\"cn\",\"PublishDate\":\"2010/10/15\",\"id\":\"1\"})",
                        result.getHits().get(0).toString());
    bql = "select Author from Linden browse by Author, PublishDate source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(result.getHits().get(0).toString(),
                        "LindenHit(id:1, score:1.0, source:{\"Author\":\"Bob\",\"id\":\"1\"})");

    bql = "select * from Linden browse by Author.FACET, PublishDate.FACET drill down PublishDate.FACET('2010')";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals(2, result.getFacetResultsSize());

    // PublishDate facet
    Assert.assertEquals(
        "LindenFacetResult(dim:PublishDate, value:4, childCount:1, labelValues:[LindenLabelAndValue(label:2010, value:4)])",
        result.getFacetResults().get(1).toString());

    bql = "select * from Linden browse by Author.FACET, PublishDate.FACET drill sideways PublishDate.FACET('2010')";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(4, result.getTotalHits());
    Assert.assertEquals(2, result.getFacetResultsSize());

    // PublishDate facet is not effected by drill down Path
    Assert.assertEquals("LindenFacetResult(dim:PublishDate, value:11, childCount:4, " +
                        "labelValues:[LindenLabelAndValue(label:2010, value:4), LindenLabelAndValue(label:2012, value:3), " +
                        "LindenLabelAndValue(label:1999, value:3), LindenLabelAndValue(label:2013, value:1)])",
                        result.getFacetResults().get(1).toString());
  }
}
