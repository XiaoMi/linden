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

package com.xiaomi.linden.demo.hadoop.indexing;

import java.io.File;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.LindenSchemaBuilder;
import com.xiaomi.linden.core.search.LindenCore;
import com.xiaomi.linden.core.search.LindenCoreImpl;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;

//Ignore this test since it requires local hadoop environment
@Ignore
public class TestHadoopIndexingInOneBox {

  public LindenCore lindenCore;
  public LindenConfig lindenConfig;
  public BQLCompiler bqlCompiler;

  public TestHadoopIndexingInOneBox() throws Exception {
    String target = new File(this.getClass().getResource("/").getPath()).getParent();
    Process p = Runtime.getRuntime().exec("sh " + target + "/../bin/cars-local.sh");
    Assert.assertEquals(0, p.waitFor());
    System.out.println("Hadoop job is done");
    lindenConfig = new LindenConfig().setIndexType(LindenConfig.IndexType.MMAP);
    lindenConfig.setIndexDirectory(target + "/index/shard0");
    File lindenSchema = new File(target + "/../conf/schema.xml");
    LindenSchema schema = LindenSchemaBuilder.build(lindenSchema);
    lindenConfig.setSchema(schema);
    bqlCompiler = new BQLCompiler(schema);
    lindenCore = new LindenCoreImpl(lindenConfig);
  }

  @Test
  public void searchTest() throws Exception {
    System.out.println("Start basicTest");
    LindenSearchRequest request = new LindenSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(15000, result.getTotalHits());
    request = bqlCompiler.compile("select * from linden where color='green'").getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(1084, result.getTotalHits());
    request = bqlCompiler.compile("select * from linden where price=7000").getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(148, result.getTotalHits());
    request =
        bqlCompiler.compile("select * from linden by query is \"contents:(compact AND green)\"").getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(589, result.getTotalHits());
    Assert.assertEquals(1.0581597089767456, result.getHits().get(0).getScore(), 1e-16);

    request =
        bqlCompiler.compile(
            "select * from linden browse by color(3), makemodel drill sideways makemodel('european/audi')  source")
            .getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getFacetResults().size());
    Assert.assertEquals(403, result.getFacetResults().get(0).getValue());
    Assert.assertEquals(8, result.getFacetResults().get(0).getChildCount());
    Assert.assertEquals("black", result.getFacetResults().get(0).getLabelValues().get(0).getLabel());
    Assert.assertEquals(131, result.getFacetResults().get(0).getLabelValues().get(0).getValue());
    Assert.assertEquals(15000, result.getFacetResults().get(1).getValue());
    Assert.assertEquals(3, result.getFacetResults().get(1).getChildCount());
    Assert.assertEquals("asian", result.getFacetResults().get(1).getLabelValues().get(0).getLabel());
    Assert.assertEquals(5470, result.getFacetResults().get(1).getLabelValues().get(0).getValue());
    lindenCore.close();
  }
}
