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

import com.xiaomi.linden.common.EmbeddedZooKeeper;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class ZooKeeperService {
  private static EmbeddedZooKeeper zkServer;
  private static final String ZK_DATA_DIR = "./target/mock_zk";

  public static synchronized void start() {
    if (zkServer == null) {
      File file = new File(ZK_DATA_DIR);
      if (file.exists()) {
        try {
          FileUtils.deleteDirectory(new File(ZK_DATA_DIR));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      zkServer = new EmbeddedZooKeeper(file, 2181);
      try {
        zkServer.start();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
