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

import com.alibaba.fastjson.JSONArray;
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

public class TestLindenAggregation extends TestLindenCoreBase {

  public TestLindenAggregation() throws Exception {
    try {
      handleRequest(generateIndexRequest("1", 0, 1.0));
      handleRequest(generateIndexRequest("2", 1, 2.1));
      handleRequest(generateIndexRequest("3", -1, 3.4));
      handleRequest(generateIndexRequest("4", 11, 11.0));
      handleRequest(generateIndexRequest("5", -11, 21.01));
      handleRequest(generateIndexRequest("6", 4, 13.50));
      handleRequest(generateIndexRequest("7", 5, 23.0));
      handleRequest(generateIndexRequest("8", 6, 112.0));
      handleRequest(generateIndexRequest("9", 123, -1.0));
      handleRequest(generateIndexRequest("10", 456, -31.0));
      handleRequest(generateIndexRequest("11", -12, -21.80));
      handleRequest(generateIndexRequest("12", 342, -3.14));
      handleRequest(generateIndexRequest("13", 122, 5.12));
      handleRequest(generateIndexRequest("14", 1213, 9.20));
      handleRequest(generateIndexRequest("15", -133, 11.25));
      lindenCore.commit();
      lindenCore.refresh();
      bqlCompiler = new BQLCompiler(lindenConfig.getSchema());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String generateIndexRequest(String id, int intValue, double dValue) {
    JSONObject json = new JSONObject();
    json.put("id", id);
    json.put("intValue", intValue);
    JSONObject dValueField = new JSONObject();
    dValueField.put("dValue", dValue);
    dValueField.put("_type", "double");
    dValueField.put("_docValues", "true");
    JSONArray dynamic = new JSONArray();
    dynamic.add(dValueField);
    json.put("_dynamic", dynamic);
    return json.toString();
  }

  @Override
  public void init() throws Exception {
    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(new LindenFieldSchema().setName("intValue").setType(LindenType.INTEGER).setDocValues(true));
    lindenConfig.setSchema(schema);
  }

  @Test
  public void aggregationSearchTest() throws IOException {
    String bql =
        "SELECT * FROM linden aggregate by intValue({*, *}, {*, -200], {-200, 0}, [0, 10}), "
            + "dValue.double({*, -5.0}, [-21.79, -0.9}, [-21.80, -0.9}, {1000, *], {0, 11.0])";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(
        "[AggregationResult(field:intValue, labelValues:[LindenLabelAndValue(label:{*,*}, value:15), "
            + "LindenLabelAndValue(label:{*,-200], value:0), LindenLabelAndValue(label:{-200,0}, value:4), "
            + "LindenLabelAndValue(label:[0,10}, value:5)]), "
            + "AggregationResult(field:dValue, labelValues:[LindenLabelAndValue(label:{*,-5.0}, value:2), "
            + "LindenLabelAndValue(label:[-21.79,-0.9}, value:2), LindenLabelAndValue(label:[-21.80,-0.9}, value:3), "
            + "LindenLabelAndValue(label:{1000,*], value:0), LindenLabelAndValue(label:{0,11.0], value:6)])]",
        result.getAggregationResults().toString());
  }
}


