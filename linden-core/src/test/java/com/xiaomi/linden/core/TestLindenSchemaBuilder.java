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

import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenType;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TestLindenSchemaBuilder {

  @Test
  public void lindenSchemaBuilderTest() throws Exception {
    String schemaPath = TestLindenSchemaBuilder.class.getResource("/test_builder_schema.xml").getPath();
    LindenSchema schema = LindenSchemaBuilder.build(new File(schemaPath));

    Assert.assertEquals("aid", schema.getId());
    Assert.assertEquals(9, schema.getFieldsSize());
    Assert.assertTrue(schema.getFields().get(0).isSnippet());
    Assert.assertTrue(schema.getFields().get(0).isOmitFreqs());
    Assert.assertTrue(schema.getFields().get(1).isIndexed());
    Assert.assertFalse(schema.getFields().get(6).isIndexed());
    Assert.assertEquals(LindenType.STRING, schema.getFields().get(5).getType());
    Assert.assertEquals(LindenType.FACET, schema.getFields().get(8).getType());
  }
}
