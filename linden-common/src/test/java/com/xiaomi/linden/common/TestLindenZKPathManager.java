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

package com.xiaomi.linden.common;

import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.common.util.LindenZKPathManager;

public class TestLindenZKPathManager {

  @Test
  public void zkPathManagerBasicTest() {
    LindenZKPathManager zkPathManager = new LindenZKPathManager("127.0.0.1:2181/linden/linden_test");
    Assert.assertEquals("/linden/linden_test", zkPathManager.getPath());
    Assert.assertEquals("/linden/linden_test/nodes/all", zkPathManager.getAllNodesPath());
    Assert.assertEquals("/linden/linden_test/nodes/shards", zkPathManager.getClusterPath());
    Assert.assertEquals("/linden/linden_test/nodes/shards/0", zkPathManager.getShardPath("0"));
  }
}
