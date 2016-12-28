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


import com.github.zkclient.ZkClient;

import java.util.HashMap;
import java.util.Map;

public class ZKClientFactory {
  private static Map<String, ZkClient> zkClientMap = new HashMap<>();

  public static ZkClient getClient(String zk) {
    ZkClient zkClient = zkClientMap.get(zk);
    if (zkClient == null) {
      synchronized (ZKClientFactory.class) {
        zkClient = new ZkClient(zk);
        zkClient.waitUntilConnected();
        zkClientMap.put(zk, zkClient);
      }
    }
    return zkClient;
  }
}
