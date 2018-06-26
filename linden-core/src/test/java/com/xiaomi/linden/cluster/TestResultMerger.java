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

package com.xiaomi.linden.cluster;

import com.xiaomi.linden.thrift.common.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestResultMerger {
   private static final double DELTA = 1e-15;

  @Test
  public void ResultMergerBasicTest() {
    LindenSearchRequest lindenRequest = new LindenSearchRequest().setOffset(0).setLength(10);
    LindenResult result1 = new LindenResult();
    LindenResult result2 = new LindenResult();
    LindenResult result3 = new LindenResult();
    for (int i = 0; i < 10; ++i) {
      result1.addToHits(new LindenHit("1_" + (9 - i), 9-i));
      result2.addToHits(new LindenHit("2_" + (9 - i), (9 - i) * 2));
      result3.addToHits(new LindenHit("3_" + (9 - i), (9 - i) * 3));
    }
    result1.setTotalHits(10);
    result2.setTotalHits(10);
    result3.setTotalHits(10);
    List<LindenResult> resultList = new ArrayList<>();
    resultList.add(result1);
    resultList.add(result2);
    resultList.add(result3);

    LindenResult mergedResult = ResultMerger.merge(lindenRequest, resultList);
    Assert.assertEquals(10, mergedResult.getHitsSize());
    Assert.assertEquals(30, mergedResult.getTotalHits());
    Assert.assertEquals("3_9", mergedResult.getHits().get(0).getId());
    Assert.assertEquals(27f, mergedResult.getHits().get(0).getScore(), DELTA);

    lindenRequest = new LindenSearchRequest().setOffset(0).setLength(40);
    mergedResult = ResultMerger.merge(lindenRequest, resultList);
    Assert.assertEquals(30, mergedResult.getHitsSize());
    Assert.assertEquals(30, mergedResult.getTotalHits());

    lindenRequest = new LindenSearchRequest().setOffset(10).setLength(20);
    mergedResult = ResultMerger.merge(lindenRequest, resultList);
    Assert.assertEquals(20, mergedResult.getHitsSize());
    Assert.assertEquals(30, mergedResult.getTotalHits());
    Assert.assertEquals("2_5", mergedResult.getHits().get(0).getId());
    Assert.assertEquals(10f, mergedResult.getHits().get(0).getScore(), DELTA);

    lindenRequest = new LindenSearchRequest().setOffset(50).setLength(10);
    mergedResult = ResultMerger.merge(lindenRequest, resultList);
    Assert.assertEquals(0, mergedResult.getHitsSize());
    Assert.assertEquals(30, mergedResult.getTotalHits());
  }

  @Test
  public void TestSort() {
    LindenResult result1 = new LindenResult();
    LindenResult result2 = new LindenResult();
    LindenResult result3 = new LindenResult();
    for (int i = 0; i < 10; ++i) {
      LindenHit hit1 = new LindenHit("1_" + (9 - i), 9 - i);
      LindenHit hit2 = new LindenHit("2_" + (9 - i), 9 - i);
      LindenHit hit3 = new LindenHit("3_" + (9 - i), 9 - i);
      hit1.putToFields("rank", String.valueOf(9 - i + 10));
      hit2.putToFields("rank", String.valueOf(9 - i + 10));
      hit3.putToFields("rank", String.valueOf(9 - i + 10));

      hit1.putToFields("interval", String.valueOf(9 - i + 30));
      hit2.putToFields("interval", String.valueOf(9 - i + 20));
      hit3.putToFields("interval", String.valueOf(9 - i + 10));

      result1.addToHits(hit1);
      result2.addToHits(hit2);
      result3.addToHits(hit3);
    }
    result1.setTotalHits(10);
    result2.setTotalHits(10);
    result3.setTotalHits(10);
    List<LindenResult> resultList = new ArrayList<>();
    resultList.add(result1);
    resultList.add(result2);
    resultList.add(result3);

    LindenSearchRequest lindenRequest = new LindenSearchRequest().setOffset(0).setLength(10);
    LindenSort lindenSort = new LindenSort();
    lindenSort.addToFields(new LindenSortField("rank", LindenSortType.INTEGER));
    lindenSort.addToFields(new LindenSortField("interval", LindenSortType.LONG));
    lindenRequest.setSort(lindenSort);


    LindenResult mergedResult = ResultMerger.merge(lindenRequest, resultList);
    Assert.assertEquals("1_9", mergedResult.getHits().get(0).getId());
    Assert.assertEquals("2_9", mergedResult.getHits().get(1).getId());
    Assert.assertEquals("3_9", mergedResult.getHits().get(2).getId());
  }


  private LindenHit buildGroup(String groupName, int base) {
    LindenHit group = new LindenHit();
    //5 elements in a group
    List<LindenHit> docs = new ArrayList<>();
    group.setGroupHits(docs);
    for (int i = 0; i < 5; ++i) {
      docs.add(new LindenHit(base + "_" + (5 - i), 5 - i + base));
    }
    group.setFields(new HashMap<String,String>());
    group.getFields().put("group_name", groupName);
    group.setScore(5 + base);
    return group;
  }

  @Test
  public void testGroupSearch() {
    LindenResult result1 = new LindenResult();
    LindenResult result2 = new LindenResult();
    LindenResult result3 = new LindenResult();

    result1.setHits(new ArrayList<LindenHit>());
    result2.setHits(new ArrayList<LindenHit>());
    result3.setHits(new ArrayList<LindenHit>());

    for (int i = 0; i < 4; ++i) {
      result1.addToHits(buildGroup("group" + i, i));
      result2.addToHits(buildGroup("group" + i, i + 1));
      result3.addToHits(buildGroup("group" + i, i + 2));
    }

    result1.setTotalHits(30);
    result1.setTotalGroups(4);
    result1.setTotalGroupHits(20);
    result2.setTotalHits(30);
    result2.setTotalGroups(4);
    result2.setTotalGroupHits(20);
    result3.setTotalHits(30);
    result3.setTotalGroups(4);
    result3.setTotalGroupHits(20);

    List<LindenResult> resultList = new ArrayList<>();
    resultList.add(result1);
    resultList.add(result2);
    resultList.add(result3);

    LindenSearchRequest searchRequest = new LindenSearchRequest();
    GroupParam groupParam = new GroupParam("group_name");
    groupParam.setGroupInnerLimit(10);
    searchRequest.setGroupParam(groupParam);

    LindenResult result = ResultMerger.merge(searchRequest, resultList);
    Assert.assertEquals(90, result.getTotalHits());
    Assert.assertEquals(60, result.getTotalGroupHits());
    Assert.assertEquals(4, result.getTotalGroups());
    Assert.assertEquals(4, result.getHitsSize());
    List<LindenHit> groups = result.getHits();
    Assert.assertEquals("group3", groups.get(0).getFields().get("group_name"));
    Assert.assertEquals(10f, groups.get(0).score, DELTA);
    Assert.assertEquals(10f, groups.get(0).getGroupHits().get(0).score, DELTA);
    Assert.assertEquals(10, groups.get(0).getGroupHitsSize());
    Assert.assertEquals("group2", groups.get(1).getFields().get("group_name"));
    Assert.assertEquals(9f, groups.get(1).getGroupHits().get(0).score, DELTA);
  }
}
