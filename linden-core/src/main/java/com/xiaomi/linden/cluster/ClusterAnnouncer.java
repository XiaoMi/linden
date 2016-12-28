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

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.twitter.finagle.Announcement;
import com.twitter.finagle.zookeeper.ZkAnnouncer;
import com.twitter.finagle.zookeeper.ZkClientFactory;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.xiaomi.linden.common.util.LindenZKPathManager;
import com.xiaomi.linden.common.util.CommonUtils;
import com.xiaomi.linden.core.LindenConfig;
import scala.Option;

public class ClusterAnnouncer {
  private Future<Announcement> allClusterStatus;
  private Future<Announcement> shardClusterStatus;
  private ZkAnnouncer announcer;
  private final LindenZKPathManager zkPathManager;
  private final LindenConfig lindenConf;

  public ClusterAnnouncer(LindenConfig lindenConf) {
    this.lindenConf = lindenConf;
    zkPathManager = new LindenZKPathManager(lindenConf.getClusterUrl());
    ZkClientFactory factory = new ZkClientFactory(Duration.apply(30, TimeUnit.SECONDS));
    announcer = new ZkAnnouncer(factory);
  }

  public void announce() {
    allClusterStatus = announcer.announce(
        zkPathManager.getZK(),
        zkPathManager.getAllNodesPath(),
        lindenConf.getShardId(),
        new InetSocketAddress(CommonUtils.getLocalHostIp(), lindenConf.getPort()),
        Option.<String>empty());
    shardClusterStatus = announcer.announce(
        zkPathManager.getZK(),
        zkPathManager.getShardPath(String.valueOf(lindenConf.getShardId())),
        lindenConf.getShardId(),
        new InetSocketAddress(CommonUtils.getLocalHostIp(), lindenConf.getPort()),
        Option.<String>empty());
  }

  public void unannounce() throws Exception {
    Await.result(allClusterStatus).unannounce();
    Await.result(shardClusterStatus).unannounce();
    Await.result(allClusterStatus).close();
    Await.result(shardClusterStatus).close();
  }
}
