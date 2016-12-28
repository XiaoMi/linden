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

package com.xiaomi.linden.hadoop.indexing.reduce;

import com.xiaomi.linden.hadoop.indexing.keyvalueformat.IntermediateForm;
import com.xiaomi.linden.hadoop.indexing.keyvalueformat.Shard;
import com.xiaomi.linden.hadoop.indexing.util.LindenConfigBuilder;
import com.xiaomi.linden.hadoop.indexing.util.MRJobConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.lucene.facet.FacetsConfig;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;


public class LindenReducer extends Reducer<Shard, IntermediateForm, Shard, Text> {

  private static final Logger logger = Logger.getLogger(LindenReducer.class);
  static final Text DONE = new Text("done");

  private Configuration conf;
  private String mapreduceuceTempDir;
  private FacetsConfig facetsConfig;

  @Override
  protected void reduce(Shard key, Iterable<IntermediateForm> values, Context context)
      throws IOException, InterruptedException {

    logger.info("Construct a shard writer for " + key);
    FileSystem fs = FileSystem.get(conf);
    // debug:
    logger.info("filesystem is: " + fs.getUri());
    String temp = mapreduceuceTempDir + Path.SEPARATOR + "shard_" + key.toFlatString() + "_"
        + System.currentTimeMillis();
    logger.info("mapreduceuceTempDir is: " + mapreduceuceTempDir);
    final ShardWriter writer = new ShardWriter(fs, key, temp, conf);

    // update the shard
    Iterator<IntermediateForm> iterator = values.iterator();
    while (iterator.hasNext()) {
      IntermediateForm form = iterator.next();
      writer.process(form, facetsConfig);
    }

    // close the shard
    new Closeable() {
      volatile boolean closed = false;
      @Override
      public void close() throws IOException {
        // spawn a thread to give progress heartbeats
        Thread prog = new Thread() {
          @Override
          public void run() {
            while (!closed) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                continue;
              } catch (Throwable e) {
                return;
              }
            }
          }
        };

        try {
          prog.start();
          if (writer != null) {
            writer.optimize(); // added this option to optimize after all the docs have been added;
            writer.close();
          }
        } finally {
          closed = true;
        }
      }
    }.close();
    logger.info("Closed the shard writer for " + key + ", writer = " + writer);
    context.write(key, DONE);
  }

  @Override
  public void cleanup(Context context) throws IOException {
    if (mapreduceuceTempDir != null) {
      File file = new File(mapreduceuceTempDir);
      if (file.exists()) deleteDir(file);
    }
  }

  static void deleteDir(File file) {
    if (file == null || !file.exists()) {
      return;
    }
    for (File f : file.listFiles()) {
      if (f.isDirectory()) {
        deleteDir(f);
      }
      else {
        f.delete();
      }
    }
    file.delete();
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();
    mapreduceuceTempDir = conf.get(MRJobConfig.TEMP_DIR);
    mapreduceuceTempDir = Shard.normalizePath(mapreduceuceTempDir);
    facetsConfig = LindenConfigBuilder.build().createFacetsConfig();
  }
}
