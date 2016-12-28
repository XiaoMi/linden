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

package com.xiaomi.linden.hadoop.indexing.job;

import com.xiaomi.linden.hadoop.indexing.keyvalueformat.IntermediateForm;
import com.xiaomi.linden.hadoop.indexing.keyvalueformat.Shard;
import com.xiaomi.linden.hadoop.indexing.map.LindenMapper;
import com.xiaomi.linden.hadoop.indexing.reduce.FileSystemDirectory;
import com.xiaomi.linden.hadoop.indexing.reduce.IndexUpdateOutputFormat;
import com.xiaomi.linden.hadoop.indexing.reduce.LindenCombiner;
import com.xiaomi.linden.hadoop.indexing.reduce.LindenReducer;
import com.xiaomi.linden.hadoop.indexing.util.LindenJobConfig;
import com.xiaomi.linden.hadoop.indexing.util.LuceneUtil;
import com.xiaomi.linden.hadoop.indexing.util.MRJobConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Comparator;

public class LindenJob extends Configured implements Tool {

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  private static final Logger logger = Logger.getLogger(LindenJob.class);

  public static void main(String[] args) throws Exception {
    System.out.println(Arrays.asList(args));
    int exitCode = ToolRunner.run(new LindenJob(), args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] strings) throws Exception {
    Configuration conf = getConf();
    String dir = conf.get(LindenJobConfig.INPUT_DIR, null);
    logger.info("input dir:" + dir);
    Path inputPath = new Path(StringUtils.unEscapeString(dir));
    Path outputPath = new Path(conf.get(LindenJobConfig.OUTPUT_DIR));
    String indexPath = conf.get(LindenJobConfig.INDEX_PATH);

    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    if (fs.exists(new Path(indexPath))) {
      fs.delete(new Path(indexPath), true);
    }

    int numShards = conf.getInt(LindenJobConfig.NUM_SHARDS, 1);
    Shard[] shards = createShards(indexPath, numShards);

    Shard.setIndexShards(conf, shards);

    //empty trash;
    (new Trash(conf)).expunge();

    Job job = Job.getInstance(conf, "linden-hadoop-indexing");
    job.setJarByClass(LindenJob.class);
    job.setMapperClass(LindenMapper.class);
    job.setCombinerClass(LindenCombiner.class);
    job.setReducerClass(LindenReducer.class);
    job.setMapOutputKeyClass(Shard.class);
    job.setMapOutputValueClass(IntermediateForm.class);
    job.setOutputKeyClass(Shard.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(IndexUpdateOutputFormat.class);
    job.setReduceSpeculativeExecution(false);
    job.setNumReduceTasks(numShards);

    String lindenSchemaFile = conf.get(LindenJobConfig.SCHEMA_FILE_URL);
    if (lindenSchemaFile == null) {
      throw new IOException("no schema file is found");
    }
    logger.info("Adding schema file: " + lindenSchemaFile);
    job.addCacheFile(new URI(lindenSchemaFile + "#lindenSchema"));
    String lindenPropertiesFile = conf.get(LindenJobConfig.LINDEN_PROPERTIES_FILE_URL);
    if (lindenPropertiesFile == null) {
      throw new IOException("no linden properties file is found");
    }
    logger.info("Adding linden properties file: " + lindenPropertiesFile);
    job.addCacheFile(new URI(lindenPropertiesFile + "#lindenProperties"));

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    Path[] inputs = FileInputFormat.getInputPaths(job);
    StringBuilder buffer = new StringBuilder(inputs[0].toString());
    for (int i = 1; i < inputs.length; i++) {
      buffer.append(",");
      buffer.append(inputs[i].toString());
    }
    logger.info("mapreduce.input.dir = " + buffer.toString());
    logger.info("mapreduce.output.dir = " + FileOutputFormat.getOutputPath(job).toString());
    logger.info("mapreduce.job.num.reduce.tasks = " + job.getNumReduceTasks());
    logger.info(shards.length + " shards = " + conf.get(LindenJobConfig.INDEX_SHARDS));
    logger.info("mapreduce.input.format.class = " + job.getInputFormatClass());
    logger.info("mapreduce.output.format.class = " + job.getOutputFormatClass());
    logger.info("mapreduce.cluster.temp.dir = " + conf.get(MRJobConfig.TEMP_DIR));

    job.waitForCompletion(true);
    if (!job.isSuccessful()) {
      throw new RuntimeException("Job failed");
    }
    return 0;
  }

  public static Shard[] createShards(String indexPath, int numShards) throws IOException {
    String indexSubDirPrefix = "shard";
    String parent = Shard.normalizePath(indexPath) + Path.SEPARATOR;
    Shard[] shards = new Shard[numShards];
    for (int i = 0; i < shards.length; i++) {
      shards[i] = new Shard(parent + indexSubDirPrefix + NUMBER_FORMAT.format(i));
    }
    return shards;
  }
}
