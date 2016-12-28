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

import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.core.indexing.LindenIndexRequestParser;
import com.xiaomi.linden.thrift.common.IndexRequestType;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;
import com.xiaomi.linden.thrift.common.LindenSchema;

public class TestLindenIndexRequestParser {

  @Test
  public void basicTest() throws Exception {
    String content = "{\"id\":\"18617\","
                     + "\"sName\":\"杭州萧山明达园艺场\","
                     + "\"catId\":4,"
                     + "\"latitude\":0,"
                     + "\"longitude\":0"
                     + "}";
    String schemaXml = TestLindenIndexRequestParser.class.getClassLoader().getResource("service1/schema.xml").getFile();
    LindenSchema schema = LindenSchemaBuilder.build(new File(schemaXml));
    LindenIndexRequest request = LindenIndexRequestParser.parse(schema, content);
    Assert.assertEquals(IndexRequestType.INDEX, request.getType());
    Assert.assertEquals(4, request.getDoc().getFieldsSize());

    // miss field.
    content = "{\"id\":\"18617\","
              + "\"sName\":\"杭州萧山明达园艺场\","
              + "\"catId\":4,"
              + "\"latitude\":0,"
              + "\"longitude\":0,"
              + "}";
    request = LindenIndexRequestParser.parse(schema, content);
    Assert.assertEquals(IndexRequestType.INDEX, request.getType());
    Assert.assertEquals(4, request.getDoc().getFieldsSize());

    // index type
    content = "{\"type\":\"index\","
              + "\"content\":{\"id\":\"18617\",\"sName\":\"杭州萧山明达园艺场\",\"catId\":4,\"latitude\":0,\"longitude\":0}"
              + "}";
    request = LindenIndexRequestParser.parse(schema, content);
    Assert.assertEquals(IndexRequestType.INDEX, request.getType());
    Assert.assertEquals(4, request.getDoc().getFieldsSize());

    // index request with route param
    content = "{\"type\":\"index\","
              + "\"route\":[0,1,2],"
              + "\"content\":{\"id\":\"18617\",\"sName\":\"杭州萧山明达园艺场\",\"catId\":4,\"latitude\":0,\"longitude\":0}"
              + "}";
    request = LindenIndexRequestParser.parse(schema, content);
    Assert.assertEquals(IndexRequestType.INDEX, request.getType());
    Assert.assertEquals(3, request.getRouteParam().getShardIdsSize());
    Assert.assertEquals(4, request.getDoc().getFieldsSize());

    // delete request
    content = "{\"type\":\"delete\", \"id\":\"123\"}";
    request = LindenIndexRequestParser.parse(schema, content);
    Assert.assertEquals(IndexRequestType.DELETE, request.getType());
    Assert.assertEquals("123", request.getId());

    // delete request with route param
    content = "{\"type\":\"delete\", \"route\":[0,1,2], \"id\":\"123\"}";
    request = LindenIndexRequestParser.parse(schema, content);
    Assert.assertEquals(IndexRequestType.DELETE, request.getType());
    Assert.assertEquals("123", request.getId());
    Assert.assertEquals(3, request.getRouteParam().getShardIdsSize());
  }

  @Test(expected = Exception.class)
  public void exceptionTest() throws Exception {
    // no id
    String content = "{\"sName\":\"杭州萧山明达园艺场\",\"catId\":4,\"latitude\":0,\"longitude\":0}";
    String schemaXml = TestLindenIndexRequestParser.class.getClassLoader().getResource("service1/schema.xml").getFile();
    LindenSchema schema = LindenSchemaBuilder.build(new File(schemaXml));
    LindenIndexRequestParser.parse(schema, content);
  }
}
