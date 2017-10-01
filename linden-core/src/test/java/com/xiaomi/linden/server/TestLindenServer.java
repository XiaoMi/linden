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
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.client.LindenClient;
import com.xiaomi.linden.core.ZooKeeperService;
import com.xiaomi.linden.service.LindenServer;
import com.xiaomi.linden.thrift.common.LindenResult;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestLindenServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestLindenServer.class);
  private static final String INDEX_FILE = "sanity.data";

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
          server1 = new LindenServer(TestLindenServer.class.getClassLoader().getResource("service1").getFile());
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
          server2 = new LindenServer(TestLindenServer.class.getClassLoader().getResource("service2").getFile());
          server2.startServer();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    serverThread1.start();
    serverThread2.start();
    clusterClient = new LindenClient("localhost:2181/sanitytest", 0, false);
    client1 = new LindenClient("localhost", 19090, 0);
    Thread.sleep(1000);
  }

  public static void shutdownLindenServer() {
    server1.close();
    server2.close();
  }

  @BeforeClass
  public static void init() {
    try {
      FileUtils.deleteDirectory(new File("./target/linden_server_data"));
      ZooKeeperService.start();
      startLindenServer();
      index();
      server1.refresh();
      server2.refresh();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void index() throws IOException {
    String dataFile = TestLindenServer.class.getClassLoader().getResource(INDEX_FILE).getFile();
    List<String> lines = FileUtils.readLines(new File(dataFile), Charset.forName("UTF-8"));
    for (String line : lines) {
      JSONObject indexRequestJSON = new JSONObject();
      indexRequestJSON.put("type", "index");
      JSONObject json = JSONObject.parseObject(line);
      String sName = json.getString("sName");
      // sNameStored has same value with sName, but different field type
      json.put("sNameStored", sName);
      indexRequestJSON.put("content", json);
      try {
        client1.index(indexRequestJSON.toJSONString());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    JSONObject command = new JSONObject();
    command.put("type", "MERGE_INDEX");
    JSONObject options = new JSONObject();
    options.put("count", 1);
    command.put("options", options);
    try {
      client1.executeCommand(command.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void basicTest() throws Exception {
    assertHits("select * from linden by query is 'sName:Hotel' source", 10, 53, null);

    // search by id
    assertHits("select * from linden where id = '5113'", 1, 1, "5113");

    // test in
    assertHits("select * from linden where id in ('6181','10503')", 2, 2, null);
    assertHits("select * from linden where catId in (3)", 4, 4, null);
    assertHits("select * from linden where catId in (3, 14) limit 0,4", 4, 8, null);
  }

  @Test
  public void spatialSearchTest() throws Exception {
    assertHits("select * from linden by query is 'sName:Hotel' where distance(116.398, 39.890) in 100"
               + " source order by catId, distance",
               5, 5, "7287");

    assertHits("select * from linden by query is 'sName:Hotel' where distance(116.398, 39.890) in 100 source"
               + " order by catId asc, distance",
               5, 5, "7795");
  }

  @Test
  public void flexibleQueryTest() throws Exception {
    // full match
    String bql = "select * from linden by flexible_query is 'bank of china' " +
                 "full_match in (sName^0.9,address^0.1) \n" +
                 "using model func\n" +
                 "begin\n" +
                 "    int fieldsLength = getFieldLength();\n" +
                 "    float sum = 0f, nameAdjacentBase = 1.2f;\n" +
                 "    for (int i = 0; i < fieldsLength; ++i) {\n" +
                 "        int matched = 0, numAdjacent = 0;\n" +
                 "        float subSum = 0;\n" +
                 "        for (int j = 0; j < getTermLength(); ++j) {\n" +
                 "            if (isMatched(i, j)) {\n" +
                 "                subSum += getScore(i, j);\n" +
                 "                ++matched;\n" +
                 "            }\n" +
                 "            if (j > 0 && isMatched(i, j - 1)) {\n" +
                 "                if (position(i, j) - position(i, j - 1)==1) {\n" +
                 "                    numAdjacent += 1;\n" +
                 "                }\n" +
                 "            }\n" +
                 "        }\n" +
                 "        subSum *= (float) matched / getTermLength();\n" +
                 "        if (getTermLength() > 1) {\n" +
                 "            subSum *= (1.0f + nameAdjacentBase * numAdjacent / (getTermLength() - 1));\n" +
                 "        }\n" +
                 "        sum += subSum;\n" +
                 "    }\n" +
                 "    return sum; \n" +
                 "end\n" +
                 "where distance(119.4758, 26.0034) in 50 or catId = 8\n" +
                 "explain\n" +
                 "source";
    assertHits(bql, 2, 2, "201", 1.801f, null);
    bql = "select * from linden by flexible_query is 'Cloud^5 Hotel^2' \n" +
          "full_match in (sName^0.9,address^0.1) \n" +
          "using model func\n" +
          "begin\n" +
          "    float sum = 0f;\n" +
          "    for (int i = 0; i < getFieldLength(); ++i) {\n" +
          "        for (int j = 0; j < getTermLength(); ++j) {\n" +
          "            sum += getScore(i, j);\n" +
          "        }\n" +
          "    }\n" +
          "    return sum;" +
          "end\n" +
          "source explain";
    LindenResult result = assertHits(bql, 1, 1, null);
    Assert.assertEquals("0.91 * 0.10 * 5.00 -- (cloud 3)", result.getHits().get(0).getExplanation().getDetails()
        .get(1).getDetails().get(0).getDescription());
  }


  @Test
  public void earlyTerminationTest() throws Exception {
    // test result estimates.
    assertHits("select * from linden by query is 'sName:(fashion hotel)' OP(AND) source", 4, 4, null);
    assertHits("select * from linden by query is 'sName:(fashion hotel)' OP(AND) source in top 10", 4, 4, null);
    assertHits("select * from linden by query is 'sName:hotel' source", 10, 53, null);
    assertHits("select * from linden by query is 'sName:hotel' source in top 20", 10, 40, null);

    // early termination with sort
    assertHits("select * from linden by query is 'sName:hotel' source in top 20 order by catId,id", 10, 40, null);
  }

  @Test
  public void searchRouteTest() throws Exception {
    assertHits("select * from linden by query is 'sName:hotel' route by 0,1", 10, 53, null);

    int n0 = assertHits("select * from linden by query is 'sName:hotel' route by 0", -1, 0, null).getTotalHits();
    int n1 = assertHits("select * from linden by query is 'sName:hotel' route by 1", -1, 0, null).getTotalHits();
    Assert.assertEquals(53, n0 + n1);

    // early termination.
    assertHits("select * from linden by query is 'sName:hotel' route by 0 in top 10, 1", 10, 36, null);
    assertHits("select * from linden by query is 'sName:hotel' route by 0, 1 in top 10", 10, 37, null);
    assertHits("select * from linden by query is 'sName:hotel' in top 10", 10, 20, null);
  }

  @Test
  public void scoreModelTest() throws Exception {
    String bql = "select sName from linden by query is 'sName:hotel' "
                 + "using score model test "
                 + "begin "
                 + "    return 10f;"
                 + "end limit 0,6 source";
    assertHits(bql, 6, 53, null, 10f, null);

    bql = "select sName from linden by query is 'sName:hotel' "
          + "using score model test "
          + "begin "
          + "    return 100f; "
          + "end source";
    assertHits(bql, 10, 53, null, 100f, null);

    bql = "select sName from linden by query is 'sName:hotel' "
          + "using score model test1 "
          + "begin "
          + "    return 100f; "
          + "end source";
    assertHits(bql, 10, 53, null, 100f, null);

    bql = "select sName from linden by query is 'sName:hotel' "
          + "using score model test(Float a = 10, Integer b = 100) "
          + "begin "
          + "    writeExplanation(\"catId:%s, a:%.2f\", catId(), a); "
          + "    return catId() * 10 * a + b; "
          + "end "
          + "order by score desc,id asc source";
    assertHits(bql, 10, 53, "29963", 1300f, null);

    bql = "select sName from linden by query is 'sName:hotel' "
          + "using score model test(Integer b = 100, Float a = 10, String str = \"Heart\") "
          + "begin "
          + " if (sName().indexOf(str)>=0) {"
          + "    return 10000f + b;"
          + " }"
          + "writeExplanation(\"catId:%s, a:%.2f\", catId(), a); "
          + "return catId() * 10 * a + b; "
          + "end source";
    assertHits(bql, 10, 53, "30967", 10100f, null);
  }

  @Test
  public void rangeQueryTest() throws Exception {
    assertHits("select * from linden where catId = 4 source", 6, 6, null);
    assertHits("select * from linden where catId > 3 limit 0,12 source", 12, 92, null);
    assertHits("select * from linden where catId > 3 source", 10, 92, null);
    assertHits("select * from linden where catId >= 3 source", 10, 96, null);
    assertHits("select * from linden where catId < 3 source", 10, 15, null);
    assertHits("select * from linden where catId between 1 AND 4 source", 10, 13, null);
    // not
    assertHits("select * from linden where id <> '4006' source", 10, 110, null);
  }

  @Test
  public void testQueryString() throws Exception {
    // op, default OR
    assertHits("select * from linden by query is 'sName:(holiday hotel)' source", 10, 53, null);
    assertHits("select * from linden by query is 'sName:(holiday hotel)' OP(AND) source", 7, 7, null);

    // phrase query
    assertHits("select * from linden by query is 'sName:\"holiday hotel\"' source", 6, 6, null);

    // range query
    assertHits("select * from linden where query is 'catId:[4 TO 4]' source", 6, 6, null);
    assertHits("select * from linden where query is 'catId:{3 TO 4]' source", 6, 6, null);
  }

  @Test
  public void sortTest() throws Exception {
    assertHits("select * from linden by query is \"sName:KFC\" order by catId, id desc source", 4, 4, "3452,2898");

    assertHits("select * from linden by query is \"sName:KFC\" order by catId, id asc source", 4, 4, "2898,3452");

    // score sort
    assertHits("select * from linden by query is \"sName:KFC\" order by catId, score source", 4, 4, "3452,2898");

    // spatial search sort
    assertHits("select * from linden by query is 'sName:KFC' where distance(120.126, 30.258) in 100 source"
               + " order by catId, distance",
               4, 4, "2898,3452,7260,8880");
  }

  @Test
  public void lindenCacheTest() throws Exception {
    String bql0 = "select * from linden where catId = -1 source";
    String bql1 = "select * from linden by query is 'sName:hotel' source explain";
    long cacheHitCount = clusterClient.getServiceInfo().getCacheInfo().getHitCount();

    assertHits(bql0, -1, -1, null);
    Assert.assertEquals(cacheHitCount, clusterClient.getServiceInfo().getCacheInfo().getHitCount());

    assertHits(bql0, -1, -1, null);
    Assert.assertEquals(++cacheHitCount, clusterClient.getServiceInfo().getCacheInfo().getHitCount());

    assertHits(bql0, -1, -1, null);
    Assert.assertEquals(++cacheHitCount, clusterClient.getServiceInfo().getCacheInfo().getHitCount());

    assertHits(bql1, -1, -1, null);
    Assert.assertEquals(cacheHitCount, clusterClient.getServiceInfo().getCacheInfo().getHitCount());

    // hit cache 10 times
    for (int i = 0; i < 10; ++i) {
      assertHits(bql1, -1, -1, null);
      Assert.assertEquals(++cacheHitCount, clusterClient.getServiceInfo().getCacheInfo().getHitCount());
    }
    // search new and unique queries 200 times, evict all old cache since cache size is 100
    for (int i = 0; i < 200; ++i) {
      assertHits("select * from linden where catId = " + (100 + i) + " source", -1, -1, null);
      Assert.assertEquals(cacheHitCount, clusterClient.getServiceInfo().getCacheInfo().getHitCount());
    }

    // no cache hit
    assertHits("select * from linden by query is 'sName:hotel' source", -1, -1, null);
    Assert.assertEquals(cacheHitCount, clusterClient.getServiceInfo().getCacheInfo().getHitCount());

    // later 50 queries should hit cache
    for (int i = 150; i < 200; ++i) {
      assertHits("select * from linden where catId = " + (100 + i) + " source", -1, -1, null);
    }
    cacheHitCount += 50;
    Assert.assertEquals(cacheHitCount, clusterClient.getServiceInfo().getCacheInfo().getHitCount());
  }


  @Test // zIndexTest will be last case
  public void zIndexTest() throws Exception {
    assertHits("select * from linden where id='8880' source route by 0", 1, 1, "8880");
    bqlDelete("delete from linden where id='8880' route by 0");
    Thread.sleep(1000);
    assertHits("select * from linden where id='8880' source", 0, 0, null);

    String doc = "{\"address\":\"Street 65\",\"catId\":0,\"id\":\"8880\",\"latitude\":30.285716,\"longitude\":120.162325,\"sName\":\"KFC\"}";
    JSONObject jsonRequst = new JSONObject();
    jsonRequst.put("type", "index");
    jsonRequst.put("content", JSONObject.parse(doc));
    clusterClient.index(jsonRequst.toString());
    Thread.sleep(1000);
    assertHits("select * from linden where id='8880' source route by 0", 1, 1, "8880");
  }


  private static LindenResult assertHits(String bql, int hits, int totalHits, String msg) throws Exception {
    LindenResult result = clusterClient.search(bql);
    if (result.isSuccess()) {
      try {
        if (hits >= 0) {
          Assert.assertEquals(hits, result.getHitsSize());
          Assert.assertEquals(totalHits, result.getTotalHits());
        }
        if (msg != null) {
          if (msg.startsWith("{")) {
            Assert.assertEquals(msg, result.getHits().get(0).getSource());
          } else {
            String[] ids = msg.split(",");
            for (int i = 0; i < ids.length; i++) {
              Assert.assertEquals(ids[i], result.getHits().get(i).getId());
            }
          }
        }
      } catch (Throwable t) {
        System.out.println(result);
        throw t;
      }
    } else {
      throw new Exception(result.getError());
    }
    return result;
  }

  private void assertHits(String bql, int hits, int totalHits, String msg, Float score, String expl) throws Exception {
    LindenResult result = assertHits(bql, hits, totalHits, msg);
    try {
      if (score != null) {
        Assert.assertEquals(score, result.getHits().get(0).getScore(), 0.01f);
      }
      if (expl != null) {
        Assert.assertEquals(expl, result.getHits().get(0).getExplanation().toString());
      }
    } catch (Exception e) {
      System.out.println(result);
      throw e;
    }
  }

  private void bqlDelete(String bql) throws Exception {
    clusterClient.delete(bql);
  }
}
