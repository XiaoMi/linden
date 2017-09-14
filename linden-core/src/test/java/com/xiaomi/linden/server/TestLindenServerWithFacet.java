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

package com.xiaomi.linden.server;

import java.io.File;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.client.LindenClient;
import com.xiaomi.linden.core.ZooKeeperService;
import com.xiaomi.linden.service.LindenServer;
import com.xiaomi.linden.thrift.common.LindenResult;

public class TestLindenServerWithFacet {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestLindenServerWithFacet.class);
  private static LindenServer server1;
  private static LindenServer server2;
  private static Thread serverThread1;
  private static Thread serverThread2;
  private static LindenClient clusterClient;
  private static LindenClient client1;

  public static void startLindenServer() throws InterruptedException {
    serverThread1 = new Thread() {
      @Override
      public void run() {
        try {
          LOGGER.info("Server 1 started.");
          server1 = new LindenServer(TestLindenServer.class.getClassLoader().getResource("facet-service1").getFile());
          server1.startServer();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    serverThread2 = new Thread() {
      @Override
      public void run() {
        try {
          LOGGER.info("Server 2 started.");
          server2 = new LindenServer(TestLindenServer.class.getClassLoader().getResource("facet-service2").getFile());
          server2.startServer();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    serverThread1.start();
    serverThread2.start();
    clusterClient = new LindenClient("localhost:2181/facettest", 0);
    client1 = new LindenClient("localhost", 19092, 0);
    Thread.sleep(1000);
  }

  public static void shutdownLindenServer() {
    if (null != server1) {
      server1.close();
    }
    if (null != server2) {
      server2.close();
    }
  }

  @BeforeClass
  public static void init() {
    try {
      FileUtils.deleteDirectory(new File("./target/linden_server_facet_data"));
      ZooKeeperService.start();
      startLindenServer();
      index();
      server1.refresh();
      server2.refresh();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static String[] indexRequest = {
      generateIndexRequest("1", "cn", "Bob", "2010/10/15", 10),
      generateIndexRequest("2", "cn", "Lisa", "2010/10/20", 12),
      generateIndexRequest("3", "en", "Lisa", "2012/1/1", 13),
      generateIndexRequest("4", "en", "Susan", "2012/1/7", 21),
      generateIndexRequest("5", "en", "Frank", "1999/5/5", 22),
      generateIndexRequest("6", "cn", "Bob", "2010/10/25", 33),
      generateIndexRequest("7", "en", "Susan", "2012/1/7", 45),
      generateIndexRequest("8", "cn", "Lisa", "2010/11/20", 110),
      generateIndexRequest("9", "en", "Lisa", "2012/2/1", 20),
      generateIndexRequest("10", "en", "Susan", "2013/1/7", 9),
      generateIndexRequest("11", "en", "Sue", "1999/5/15", 78),
      generateIndexRequest("12", "en", "Bob", "2010/12/25", 123),
      generateIndexRequest("13", "cn", "Bob", "2010/11/25", 9),
      generateIndexRequest("14", "cn", "Susan", "2011/10/25", 14),
      generateIndexRequest("15", "en", null, "1999/2/1", 23),
      generateIndexRequest("16", "en", "Susan", null, 26),
      generateIndexRequest("17", "cn", null, null, 46),
      };

  public static void index() throws Exception {
    for (int i = 0; i < indexRequest.length; ++i) {
      client1.index(indexRequest[i]);
    }
  }

  public static String generateIndexRequest(String id, String language, String author, String pubDate, long price) {
    JSONObject jsonContent = new JSONObject();
    jsonContent.put("id", id);
    jsonContent.put("Language", language);
    if (author != null) {
      jsonContent.put("Author", author);
    }
    if (pubDate != null) {
      jsonContent.put("PublishDate", pubDate);
    }
    jsonContent.put("Price", price);
    JSONObject indexRequestJSON = new JSONObject();
    indexRequestJSON.put("type", "index");
    JSONArray array = new JSONArray();
    array.add(0);
    array.add(1);
    indexRequestJSON.put("route", array);
    indexRequestJSON.put("content", jsonContent);
    return indexRequestJSON.toJSONString();
  }

  @Test
  public void basicTest() throws Exception {
    String bql = "select * from linden browse by PublishDate, Author, Author(2), PublishDate(10, '2010'), "
                 + "PublishDate(10, '2010/10'), PublishDate(10, '2010/10/15'), PublishDate(10, '2010/13')";
    LindenResult result = clusterClient.search(bql);
    Assert.assertEquals(17, result.getTotalHits());
    Assert.assertEquals(7, result.getFacetResultsSize());

    Assert.assertEquals("LindenFacetResult(dim:PublishDate, value:15, childCount:5, " +
                        "labelValues:[LindenLabelAndValue(label:2010, value:6), " +
                        "LindenLabelAndValue(label:2012, value:4), " +
                        "LindenLabelAndValue(label:1999, value:3), " +
                        "LindenLabelAndValue(label:2011, value:1), " +
                        "LindenLabelAndValue(label:2013, value:1)])", result.getFacetResults().get(0).toString());

    Assert.assertEquals("LindenFacetResult(dim:Author, value:15, childCount:5, " +
                        "labelValues:[LindenLabelAndValue(label:Susan, value:5), " +
                        "LindenLabelAndValue(label:Bob, value:4), " +
                        "LindenLabelAndValue(label:Lisa, value:4), " +
                        "LindenLabelAndValue(label:Frank, value:1), " +
                        "LindenLabelAndValue(label:Sue, value:1)])", result.getFacetResults().get(1).toString());

    Assert.assertEquals("LindenFacetResult(dim:Author, value:15, childCount:4, "
        + "labelValues:[LindenLabelAndValue(label:Lisa, value:4), LindenLabelAndValue(label:Susan, value:4)])",
        result.getFacetResults().get(2).toString());

    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010, value:6, childCount:3, " +
                        "labelValues:[LindenLabelAndValue(label:10, value:3), " +
                        "LindenLabelAndValue(label:11, value:2), " +
                        "LindenLabelAndValue(label:12, value:1)])", result.getFacetResults().get(3).toString());

    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010/10, value:3, childCount:3, " +
                        "labelValues:[LindenLabelAndValue(label:15, value:1), " +
                        "LindenLabelAndValue(label:20, value:1), " +
                        "LindenLabelAndValue(label:25, value:1)])", result.getFacetResults().get(4).toString());

    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010/10/15, value:0, childCount:0)",
                        result.getFacetResults().get(5).toString());

    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010/13, value:0, childCount:0)",
                        result.getFacetResults().get(6).toString());

    bql = "select * from Linden query Language='en' browse by Author";
    result = clusterClient.search(bql);
    Assert.assertEquals(10, result.getTotalHits());
    Assert.assertEquals(1, result.getFacetResultsSize());

    Assert.assertEquals("LindenFacetResult(dim:Author, value:9, childCount:5, " +
                        "labelValues:[LindenLabelAndValue(label:Susan, value:4), " +
                        "LindenLabelAndValue(label:Lisa, value:2), " +
                        "LindenLabelAndValue(label:Bob, value:1), " +
                        "LindenLabelAndValue(label:Frank, value:1), " +
                        "LindenLabelAndValue(label:Sue, value:1)])", result.getFacetResults().get(0).toString());
  }

  @Test
  public void earlyTermination() throws Exception {
    String bql = "select * from linden browse by PublishDate, Author, Author(2), PublishDate(10, '2010'), "
                 + "PublishDate(10, '2010/10'), PublishDate(10, '2010/10/15'), PublishDate(10, '2010/13') in top 5";
    LindenResult result = clusterClient.search(bql);

    Assert.assertEquals(10, result.getTotalHits());
    Assert.assertEquals(7, result.getFacetResultsSize());

    Assert.assertEquals(
        "LindenFacetResult(dim:PublishDate, value:10, childCount:3, "
        + "labelValues:[LindenLabelAndValue(label:2010, value:4), LindenLabelAndValue(label:2012, value:4), "
        + "LindenLabelAndValue(label:1999, value:2)])",
        result.getFacetResults().get(0).toString());

    Assert.assertEquals("LindenFacetResult(dim:Author, value:10, childCount:5, "
                        + "labelValues:[LindenLabelAndValue(label:Lisa, value:4), "
                        + "LindenLabelAndValue(label:Bob, value:2), "
                        + "LindenLabelAndValue(label:Susan, value:2), "
                        + "LindenLabelAndValue(label:Frank, value:1), "
                        + "LindenLabelAndValue(label:Sue, value:1)])",
                        result.getFacetResults().get(1).toString());

    Assert.assertEquals("LindenFacetResult(dim:Author, value:10, childCount:4, "
                        + "labelValues:[LindenLabelAndValue(label:Lisa, value:4), "
                        + "LindenLabelAndValue(label:Bob, value:1)])", result.getFacetResults().get(2).toString());

    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010, value:4, childCount:2, "
                        + "labelValues:[LindenLabelAndValue(label:10, value:3), "
                        + "LindenLabelAndValue(label:11, value:1)])", result.getFacetResults().get(3).toString());

    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010/10, value:3, childCount:3, " +
                        "labelValues:[LindenLabelAndValue(label:15, value:1), " +
                        "LindenLabelAndValue(label:20, value:1), " +
                        "LindenLabelAndValue(label:25, value:1)])", result.getFacetResults().get(4).toString());

    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010/10/15, value:0, childCount:0)",
                        result.getFacetResults().get(5).toString());

    Assert.assertEquals("LindenFacetResult(dim:PublishDate, path:2010/13, value:0, childCount:0)",
                        result.getFacetResults().get(6).toString());
  }


  @Test
  public void aggregationTest() throws Exception {
    String bql = "select * from linden aggregate by Price({*, 10}, [10, 20}, [20, 30}, [50, 100}, [100, *})";
    LindenResult result = clusterClient.search(bql);
    Assert.assertEquals("[AggregationResult(field:Price, labelValues:[LindenLabelAndValue(label:{*,10}, value:2), "
                        + "LindenLabelAndValue(label:[10,20}, value:4), LindenLabelAndValue(label:[20,30}, value:5), "
                        + "LindenLabelAndValue(label:[50,100}, value:1), LindenLabelAndValue(label:[100,*}, value:2)])]",
                        result.getAggregationResults().toString());
  }
}
