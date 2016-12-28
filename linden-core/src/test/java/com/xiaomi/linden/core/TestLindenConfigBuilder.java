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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TestLindenConfigBuilder {
  @Test
  public void configurationTest() throws Exception {
    File file = new File(getClass().getResource("/conf_test.properties").getPath());
    LindenConfig lindenConf = LindenConfigBuilder.build(file);
    Assert.assertEquals("./target/tmp", lindenConf.getIndexDirectory());
    Assert.assertEquals("127.0.0.1:2181,127.0.0.2:2181/linden_test", lindenConf.getClusterUrl());
    Assert.assertEquals(9091, lindenConf.getPort());
    Assert.assertTrue(lindenConf.isEnableParallelSearch());

    Assert.assertEquals(LindenConfig.LindenCoreMode.MULTI, lindenConf.getLindenCoreMode());
    Assert.assertEquals(LindenConfig.MultiIndexDivisionType.TIME_MONTH, lindenConf.getMultiIndexDivisionType());
    Assert.assertEquals(10, lindenConf.getMultiIndexMaxLiveIndexNum());
  }
}
