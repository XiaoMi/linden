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

package com.xiaomi.linden.common.util;

import com.google.common.base.Preconditions;

public class LindenZKPathManager {
  public static final String NODES = "nodes";
  public static final String SHARDS = "shards";
  public static final String ALL = "all";

  private String zk;
  private String path;

  /**
   *
   * @param zk zookeeper url.
   * @param path linden cluster base path.
   */
  public LindenZKPathManager(String zk, String path) {
    this.zk = zk;
    this.path = path;
    Preconditions.checkArgument(!zk.isEmpty() && !path.isEmpty(), "Cluster zk or path can not be empty");
  }

  /**
   * @param clusterUrl linden cluster url, 127.0.0.1:2181/linden, eg.
   */
  public LindenZKPathManager(String clusterUrl) {
    int pos = clusterUrl.indexOf('/');
    Preconditions.checkArgument(pos != -1, "Cluster path can not be empty");
    zk = clusterUrl.substring(0, pos);
    path = clusterUrl.substring(pos);
    Preconditions.checkArgument(!zk.isEmpty() && !path.isEmpty(), "Cluster zk or path can not be empty");
  }

  /**
   * @return zk url parsed from cluster url.
   */
  public String getZK() { return zk; }

  /**
   * @return path parsed from cluster url.
   */
  public String getPath() { return path; }

  /**
   * @param shardId shard id
   * @return shard zk cluster path that all the same shard id at.
   */
  public String getShardPath(String shardId) {
    return getClusterPath() + "/" + shardId;
  }

  /**
   * @return all shard nodes zk cluster path.
   */
  public String getAllNodesPath() {
    return String.format("%s/%s/%s", path, NODES, ALL);
  }

  /**
   * @return cluster shard path's parent path.
   */
  public String getClusterPath() {
    return String.format("%s/%s/%s", path, NODES, SHARDS);
  }
}
