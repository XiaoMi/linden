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

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.cluster.ResultMerger;
import com.xiaomi.linden.thrift.builder.query.LindenQueryBuilder;
import com.xiaomi.linden.thrift.common.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestSpatialSearch extends TestLindenCoreBase {

  public TestSpatialSearch() throws Exception {
    try {
      handleRequest("{\"id\":1, \"rank\":1.2, \"title\": \"时尚码头美容美发热烫特价\", \"longitude\": 116.3838183, \"latitude\": 39.9629015}");
      handleRequest("{\"id\":2, \"rank\":2.2, \"title\": \"审美个人美容美发套餐\", \"longitude\": 116.386564, \"latitude\": 39.966102}");
      handleRequest(
          "{\"id\":3, \"title\": \"海底捞吃300送300\", \"longitude\": 116.38629, \"latitude\": 39.9629573}");
      handleRequest("{\"id\":4, \"title\": \"仅98元！享原价335元李老爹\", \"longitude\": 116.3846175, \"latitude\": 39.9629125}");
      handleRequest("{\"id\":5, \"rank\":5.2, \"title\": \"都美造型烫染美发护理套餐\", \"longitude\": 116.38629, \"latitude\": 39.9629573}");
      handleRequest("{\"id\":6, \"title\": \"仅售55元！原价80元的老舍茶馆相声下午场\", \"longitude\": 116.0799914, \"latitude\": 39.9655391}");
      handleRequest("{\"id\":7, \"title\": \"仅售55元！原价80元的新笑声客栈早场\", \"longitude\": 116.0799914, \"latitude\": 39.9655391}");
      handleRequest("{\"id\":8, \"title\": \"仅售39元（红色礼盒）！原价80元的平谷桃\", \"longitude\": 116.0799914, \"latitude\": 39.9655391}");
      handleRequest("{\"id\":9, \"title\": \"仅售38元！原价180元地质礼堂白雪公主\", \"longitude\": 116.0799914, \"latitude\": 39.9655391}");
      handleRequest("{\"id\":10, \"title\": \"仅99元！享原价342.7元自助餐\", \"longitude\": 116.0799914, \"latitude\": 39.9655391}");
      handleRequest("{\"id\":11, \"title\": \"桑海教育暑期学生报名培训九折优惠券\", \"longitude\": 116.0799914, \"latitude\": 39.9655391}");
      handleRequest("{\"id\":12, \"title\": \"全国发货：仅29元！贝玲妃超模粉红高光光\", \"longitude\": 116.0799914, \"latitude\": 39.9655391}");
      handleRequest(
          "{\"id\":13, \"title\": \"海之屿生态水族用品店抵用券\", \"longitude\": 116.0799914, \"latitude\": 39.9655391}");
      handleRequest("{\"id\":14, \"title\": \"小区东门时尚烫染个人护理美发套餐\", \"longitude\": 116.3799914, \"latitude\": 39.9655391}");
      handleRequest(
          "{\"id\":15, \"title\": \"《郭德纲相声专辑》CD套装\", \"longitude\": 116.0799914, \"latitude\": 39.9655391}");

      lindenCore.refresh();
      bqlCompiler = new BQLCompiler(lindenConfig.getSchema());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void init() throws Exception {
    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(new LindenFieldSchema().setName("longitude").setType(LindenType.DOUBLE).setIndexed(true).setStored(true));
    schema.addToFields(new LindenFieldSchema().setName("latitude").setType(LindenType.DOUBLE).setIndexed(true).setStored(true));
    schema.addToFields(new LindenFieldSchema().setName("rank").setType(LindenType.DOUBLE).setIndexed(true).setStored(true));
    lindenConfig.setSchema(schema);
  }

  @Test
  public void spatialSearchTest() throws IOException {
    // String func = " return 1 / (distance() + 1);";
    // LindenScoreModel model = new LindenScoreModel().setName("test").setFunc(func);

    Coordinate coord = new Coordinate().setLongitude(116.386564).setLatitude(39.966102);
    LindenQuery matchAllQuery = LindenQueryBuilder.buildMatchAllQuery();
    // matchAllQuery.setScoreModel(model);
    LindenSearchRequest request = new LindenSearchRequest()
        .setQuery(matchAllQuery)
        .setSpatialParam(new SpatialParam().setCoordinate(coord).setDistanceRange(1));

    LindenResult result = lindenCore.search(request);

    Assert.assertEquals(10, result.getHitsSize());
    request = new LindenSearchRequest()
        .setQuery(matchAllQuery)
        .setSpatialParam(new SpatialParam().setCoordinate(coord).setDistanceRange(1));
    result = lindenCore.search(request);
    Assert.assertEquals(10, result.getHitsSize());
  }

  @Test
  public void bqlSpatialSearchTest() throws IOException {
    String bql = "select * from linden where distance(116.386564, 39.966102) in 1 " +
        " using score model test " +
        " begin" +
        " return 1/distance();" +
        " end";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);

    Assert.assertEquals(6, result.getHitsSize());
    Assert.assertEquals(0, result.getHits().get(0).getDistance(), 0.01);
  }

  @Test
  public void bqlSortByDistance() throws IOException {
    String bql = "select * from linden where distance(116.386564, 39.966102) in 1 order by rank, distance";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);

    List<LindenResult> resultList = new ArrayList<>();
    resultList.add(result);
    result = ResultMerger.merge(request, resultList);
    Assert.assertEquals(6, result.getHitsSize());
    Assert.assertEquals(0.3504, result.getHits().get(0).getDistance(), 0.01);
    Assert.assertEquals("5", result.getHits().get(0).getId());
  }
}
