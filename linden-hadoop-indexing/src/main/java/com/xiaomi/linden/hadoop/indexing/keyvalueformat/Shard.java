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

package com.xiaomi.linden.hadoop.indexing.keyvalueformat;

import com.xiaomi.linden.hadoop.indexing.util.LindenJobConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;

/**
 * This class represents the metadata of a shard. Version is the version number
 * of the entire index. Directory is the directory where this shard resides in.
 * Generation is the Lucene index's generation. Version and generation are
 * reserved for future use.
 *
 * Note: Currently the version number of the entire index is not used and
 * defaults to -1.
 */
@SuppressWarnings("rawtypes")
public class Shard implements WritableComparable {

  // This method is copied from Path.
  public static String normalizePath(String path) {
    // remove double slashes & backslashes
    path = path.replace("//", "/");
    path = path.replace("\\", "/");

    // trim trailing slash from non-root path (ignoring windows drive)
    if (path.length() > 1 && path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  public static void setIndexShards(Configuration conf, Shard[] shards) {
    StringBuilder shardsString = new StringBuilder(shards[0].toString());
    for (int i = 1; i < shards.length; i++) {
      shardsString.append(",");
      shardsString.append(shards[i].toString());
    }
    conf.set(LindenJobConfig.INDEX_SHARDS, shardsString.toString());
  }

  public static Shard[] getIndexShards(Configuration conf) {
    String shards = conf.get(LindenJobConfig.INDEX_SHARDS);
    if (shards != null) {
      ArrayList<Object> list = Collections.list(new StringTokenizer(shards, ","));
      Shard[] result = new Shard[list.size()];
      for (int i = 0; i < list.size(); i++) {
        result[i] = Shard.createShardFromString((String) list.get(i));
      }
      return result;
    } else {
      return null;
    }
  }

  private static Shard createShardFromString(String dir) {
    return new Shard(dir);
  }

  private String dir;

  public Shard() { dir = null; }

  /**
   * @param dir  the directory where this shard resides
   */
  public Shard(String dir) {
    this.dir = normalizePath(dir);
  }

  public String getDirectory() {
    return dir;
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return dir;
  }

  public String toFlatString() {
    return dir.replace("/", "_");
  }

  // ///////////////////////////////////
  // Writable
  // ///////////////////////////////////
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, dir);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    dir = Text.readString(in);
  }

  // ///////////////////////////////////
  // Comparable
  // ///////////////////////////////////
  /*
   * (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object o) {
    return compareTo((Shard) o);
  }

  public int compareTo(Shard other) {
    // compare dir
    return dir.compareTo(other.dir);
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Shard)) {
      return false;
    }
    Shard other = (Shard) o;
    return dir.equals(other.dir);
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return dir.hashCode();
  }
}
