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
import com.xiaomi.linden.core.indexing.LindenIndexRequestParser;
import com.xiaomi.linden.core.indexing.DocNumLimitMultiIndexStrategy;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.core.search.MultiLindenCoreImpl;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenServiceInfo;
import com.xiaomi.linden.util.DateUtils;
import mockit.Deencapsulation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestMultiLindenCore {
  public LindenCore lindenCore;
  public LindenConfig lindenConfig;
  public BQLCompiler bqlCompiler;
  private static final String INDEX_DIR = TestMultiLindenCore.class.getResource("./").getPath() + "multi_linden_core_index";

  public TestMultiLindenCore() throws Exception {
    lindenConfig = new LindenConfig().setIndexType(LindenConfig.IndexType.RAM).setClusterUrl("127.0.0.1:2181/test");
    lindenConfig.putToProperties("merge.policy.class", "org.apache.lucene.index.TieredMergePolicy");
    lindenConfig.putToProperties("merge.policy.segments.per.tier", "10");
    lindenConfig.putToProperties("merge.policy.max.merge.at.once", "10");
    lindenConfig.setPluginPath("./");
    ZooKeeperService.start();

    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(new LindenFieldSchema().setName("title").setIndexed(true).setStored(true).setTokenized(true));
    bqlCompiler = new BQLCompiler(schema);
    lindenConfig.setSchema(schema);
    lindenConfig.setIndexType(LindenConfig.IndexType.MMAP);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (new File(INDEX_DIR).exists()) {
      FileUtils.cleanDirectory(new File(INDEX_DIR));
    }
  }

  @Before
  public void setup() throws Exception {
    new MockUp<DateUtils>() {
      private Integer currentHour = 0;
      private Integer current = 0;

      @Mock
      public String getCurrentHour() {
        return "2015-04-27-" + (currentHour++ / 50);
      }

      @Mock
      public String getCurrentTime() {
        return String.valueOf(current++);
      }
    };
  }

  @Test
  public void testDocNumDivision() throws Exception {
    String path = FilenameUtils.concat(INDEX_DIR, "size/");
    lindenConfig.setIndexDirectory(path)
        .setLindenCoreMode(LindenConfig.LindenCoreMode.HOTSWAP)
        .setMultiIndexDivisionType(LindenConfig.MultiIndexDivisionType.DOC_NUM)
        .setMultiIndexDocNumLimit(100)
        .setMultiIndexMaxLiveIndexNum(4);

    Deencapsulation.setField(DocNumLimitMultiIndexStrategy.class, "INDEX_CHECK_INTERVAL_MILLISECONDS", -10);

    lindenCore = new MultiLindenCoreImpl(lindenConfig);

    for (int i = 0; i < 400; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      JSONObject content = new JSONObject();
      content.put("id", i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    lindenCore.commit();
    lindenCore.refresh();

    Assert.assertEquals(4, lindenCore.getServiceInfo().getIndexNamesSize());
    Assert.assertEquals(400, lindenCore.getServiceInfo().getDocsNum());

    String bql = "select * from linden source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(400, result.getTotalHits());

    for (int i = 0; i < 400; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      JSONObject content = new JSONObject();
      content.put("id", i + 400);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }

    lindenCore.commit();
    lindenCore.refresh();


    LindenServiceInfo serviceInfo = lindenCore.getServiceInfo();
    Assert.assertEquals(4, serviceInfo.getIndexNamesSize());
    Assert.assertEquals(400, serviceInfo.getDocsNum());
    lindenCore.close();
  }

  @Test
  public void testTimeDivision() throws Exception {
    String path = FilenameUtils.concat(INDEX_DIR, "time/");
    lindenConfig.setIndexDirectory(path)
            .setMultiIndexDivisionType(LindenConfig.MultiIndexDivisionType.TIME_HOUR)
            .setLindenCoreMode(LindenConfig.LindenCoreMode.MULTI)
            .setMultiIndexMaxLiveIndexNum(10);

    Deencapsulation.setField(DocNumLimitMultiIndexStrategy.class, "INDEX_CHECK_INTERVAL_MILLISECONDS", -10);
    lindenCore = new MultiLindenCoreImpl(lindenConfig);

    for (int i = 0; i < 400; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      JSONObject content = new JSONObject();
      content.put("id", i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    lindenCore.commit();
    lindenCore.refresh();

    Assert.assertEquals(8, lindenCore.getServiceInfo().getIndexNamesSize());
    Assert.assertEquals(400, lindenCore.getServiceInfo().getDocsNum());

    String bql = "select * from linden source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(400, result.getTotalHits());

    for (int i = 0; i < 400; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      JSONObject content = new JSONObject();
      content.put("id", i);
      content.put("title", "test " + (400 + i));
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    lindenCore.commit();
    lindenCore.refresh();

    result = lindenCore.search(request);
    Assert.assertEquals(500, result.getTotalHits());

    Assert.assertEquals(10, lindenCore.getServiceInfo().getIndexNamesSize());
    Assert.assertEquals(500, lindenCore.getServiceInfo().getDocsNum());


    bql = "delete from linden where query is 'title:400 title:401'";
    LindenDeleteRequest deleteRequest = bqlCompiler.compile(bql).getDeleteRequest();
    lindenCore.delete(deleteRequest);
    lindenCore.commit();
    lindenCore.refresh();
    Assert.assertEquals(498, lindenCore.getServiceInfo().getDocsNum());

    lindenCore.close();
  }

  @Test
  public void testIndexNameDivision() throws Exception {
    String path = FilenameUtils.concat(INDEX_DIR, "name/");
    lindenConfig.setIndexDirectory(path)
        .setLindenCoreMode(LindenConfig.LindenCoreMode.MULTI)
        .setMultiIndexDivisionType(LindenConfig.MultiIndexDivisionType.INDEX_NAME);

    lindenCore = new MultiLindenCoreImpl(lindenConfig);
    for (int i = 0; i < 400; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      json.put("index", "multi_" + i/50);
      JSONObject content = new JSONObject();
      content.put("id", i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    lindenCore.commit();
    lindenCore.refresh();

    Assert.assertEquals(8, lindenCore.getServiceInfo().getIndexNamesSize());
    Assert.assertEquals(400, lindenCore.getServiceInfo().getDocsNum());

    String bql = "select * from linden source";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(400, result.getTotalHits());

    bql = "select * from multi_0 source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(50, result.getTotalHits());

    bql = "select * from non-existed source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertFalse(result.isSuccess());

    bql = "select * from multi_1, multi_2 source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(100, result.getTotalHits());

    handleRequest("{\"type\":\"delete_index\", \"index\":\"multi_0\"}");

    bql = "select * from multi_0 source";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertFalse(result.isSuccess());

    handleRequest("{\"type\":\"delete_index\", \"index\":\"multi_2\"}");

    Assert.assertEquals(6, lindenCore.getServiceInfo().getIndexNamesSize());
    Assert.assertEquals(300, lindenCore.getServiceInfo().getDocsNum());

    // delete doc 50, 200, 300, 390, 397, doc 0 and doc 100 doesn't exist since multi_0 and multi_1 index are deleted
    bql = "delete from linden where query is 'title:0 title:50 title:100 title:200 title:300 title:390 title:397'";
    LindenDeleteRequest deleteRequest = bqlCompiler.compile(bql).getDeleteRequest();
    lindenCore.delete(deleteRequest);
    lindenCore.commit();
    lindenCore.refresh();

    Assert.assertEquals(295, lindenCore.getServiceInfo().getDocsNum());
    lindenCore.close();
  }

  public void handleRequest(String content) throws IOException {
    LindenIndexRequest indexRequest = LindenIndexRequestParser.parse(lindenConfig.getSchema(), content);
    lindenCore.index(indexRequest);
  }
}
