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

import java.io.File;
import java.io.IOException;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.core.indexing.LindenIndexRequestParser;
import com.xiaomi.linden.core.search.HotSwapLindenCoreImpl;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;
import com.xiaomi.linden.thrift.common.LindenSchema;

public class TestHotSwapLindenCore {
  public LindenCore lindenCore;
  public LindenConfig lindenConfig;

  public TestHotSwapLindenCore() throws Exception {
    ZooKeeperService.start();
    init();
  }

  public void init() throws Exception {
    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(new LindenFieldSchema().setName("title").setIndexed(true).setStored(true).setTokenized(true));

    lindenConfig = new LindenConfig();
    lindenConfig.setClusterUrl("127.0.0.1:2181/test");
    lindenConfig.setSchema(schema);
    String path = FilenameUtils.concat(TestMultiLindenCore.class.getResource("./").getPath(), "index/");
    FileUtils.deleteQuietly(new File(path));
    lindenConfig.setIndexDirectory(path);
    lindenConfig.setLindenCoreMode(LindenConfig.LindenCoreMode.HOTSWAP);
  }

  @Test
  public void ramIndexTest() throws Exception {
    lindenConfig.setIndexType(LindenConfig.IndexType.RAM);
    lindenCore = new HotSwapLindenCoreImpl(lindenConfig);

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
    Assert.assertEquals(400, lindenCore.getServiceInfo().getDocsNum());

    String indexName0 = "next_" + System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      json.put("index", indexName0);
      JSONObject content = new JSONObject();
      content.put("id", indexName0 + "_" + i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    Thread.sleep(1000);

    String indexName1 = "next_" + System.currentTimeMillis();
    for (int i = 0; i < 101; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      json.put("index", indexName1);
      JSONObject content = new JSONObject();
      content.put("id", indexName1 + "_" + i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    Thread.sleep(1000);

    String indexName2 = "next_" + System.currentTimeMillis();
    for (int i = 0; i < 102; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      json.put("index", indexName2);
      JSONObject content = new JSONObject();
      content.put("id", indexName2 + "_" + i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    Thread.sleep(1000);

    String indexName3 = "next_" + System.currentTimeMillis();
    for (int i = 0; i < 103; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      json.put("index", indexName3);
      JSONObject content = new JSONObject();
      content.put("id", indexName3 + "_" + i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }

    Assert.assertEquals(4, lindenCore.getServiceInfo().getIndexNames().size());
    // indexName0 should be retired, since MAX_NEXT_INDEX_NUM = 3;
    JSONObject json = new JSONObject();
    json.put("type", "swap_index");
    json.put("index", indexName0);
    handleRequest(json.toJSONString());
    // swap failed current index is not changed
    Assert.assertEquals(400, lindenCore.getServiceInfo().getDocsNum());

    // swap current index with indexName 1
    lindenCore.swapIndex(indexName1);
    Assert.assertEquals(3, lindenCore.getServiceInfo().getIndexNames().size());
    Assert.assertEquals(101, lindenCore.getServiceInfo().getDocsNum());

    // swap current index with indexName 3
    json = new JSONObject();
    json.put("type", "swap_index");
    json.put("index", indexName3);
    handleRequest(json.toJSONString());
    Assert.assertEquals(2, lindenCore.getServiceInfo().getIndexNames().size());
    Assert.assertEquals(103, lindenCore.getServiceInfo().getDocsNum());

    // bootstrap after swap is done
    for (int i = 103; i < 200; ++i) {
      json = new JSONObject();
      json.put("type", "index");
      json.put("index", indexName3);
      JSONObject content = new JSONObject();
      content.put("id", indexName3 + "_" + i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    lindenCore.commit();
    lindenCore.refresh();
    Assert.assertEquals(200, lindenCore.getServiceInfo().getDocsNum());
  }


  @Test
  public void diskIndexTest() throws Exception {
    lindenConfig.setIndexType(LindenConfig.IndexType.MMAP);
    lindenCore = new HotSwapLindenCoreImpl(lindenConfig);

    for (int i = 0; i < 400; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      JSONObject content = new JSONObject();
      content.put("id", i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }

    String indexName0 = "next_" + System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      json.put("index", indexName0);
      JSONObject content = new JSONObject();
      content.put("id", indexName0 + "_" + i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    Thread.sleep(1000);

    String indexName1 = "next_" + System.currentTimeMillis();
    for (int i = 0; i < 101; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      json.put("index", indexName1);
      JSONObject content = new JSONObject();
      content.put("id", indexName1 + "_" + i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    Thread.sleep(1000);

    String indexName2 = "next_" + System.currentTimeMillis();
    for (int i = 0; i < 102; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      json.put("index", indexName2);
      JSONObject content = new JSONObject();
      content.put("id", indexName2 + "_" + i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    Thread.sleep(1000);

    String indexName3 = "next_" + System.currentTimeMillis();
    for (int i = 0; i < 103; ++i) {
      JSONObject json = new JSONObject();
      json.put("type", "index");
      json.put("index", indexName3);
      JSONObject content = new JSONObject();
      content.put("id", indexName3 + "_" + i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }

    lindenCore.commit();
    lindenCore.refresh();
    Assert.assertEquals(400, lindenCore.getServiceInfo().getDocsNum());
    Assert.assertEquals(4, lindenCore.getServiceInfo().getIndexNames().size());
    // indexName0 should be retired, since MAX_NEXT_INDEX_NUM = 3;
    JSONObject json = new JSONObject();
    json.put("type", "swap_index");
    json.put("index", indexName0);
    handleRequest(json.toJSONString());
    // swap failed current index is not changed
    Assert.assertEquals(400, lindenCore.getServiceInfo().getDocsNum());

    // swap current index with indexName 1
    json = new JSONObject();
    json.put("type", "swap_index");
    json.put("index", indexName1);
    handleRequest(json.toJSONString());
    Assert.assertEquals(3, lindenCore.getServiceInfo().getIndexNames().size());
    Assert.assertEquals(101, lindenCore.getServiceInfo().getDocsNum());

    // swap current index with indexName 3
    lindenCore.swapIndex(indexName3);
    // swap multiple times
    handleRequest(json.toJSONString());
    Assert.assertEquals(2, lindenCore.getServiceInfo().getIndexNames().size());
    Assert.assertEquals(103, lindenCore.getServiceInfo().getDocsNum());

    Assert.assertEquals(1, lindenCore.getServiceInfo().getFileUsedInfosSize());
    Assert.assertEquals(20, lindenCore.getServiceInfo().getFileUsedInfos().get(0).getDiskUsage() /1024);

    // bootstrap after swap is done
    for (int i = 103; i < 200; ++i) {
      json = new JSONObject();
      json.put("type", "index");
      json.put("index", indexName3);
      JSONObject content = new JSONObject();
      content.put("id", indexName3 + "_" + i);
      content.put("title", "test " + i);
      json.put("content", content);
      handleRequest(json.toJSONString());
    }
    lindenCore.commit();
    lindenCore.refresh();
    Assert.assertEquals(200, lindenCore.getServiceInfo().getDocsNum());
  }

  public void handleRequest(String content) throws IOException {
    LindenIndexRequest indexRequest = LindenIndexRequestParser.parse(lindenConfig.getSchema(), content);
    lindenCore.index(indexRequest);
  }
}
