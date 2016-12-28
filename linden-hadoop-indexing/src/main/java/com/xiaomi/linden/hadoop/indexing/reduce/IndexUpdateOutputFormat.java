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

import com.xiaomi.linden.hadoop.indexing.keyvalueformat.Shard;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * The record writer of this output format simply puts a message in an output
 * path when a shard update is done.
 */
public class IndexUpdateOutputFormat extends FileOutputFormat<Shard, Text> {

  static final Text DONE = new Text("done");

  @Override public RecordWriter<Shard, Text> getRecordWriter(
      final TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {

    return new RecordWriter<Shard, Text>() {
      @Override
      public void write(Shard key, Text value) throws IOException {
        assert (DONE.equals(value));
      }

      @Override
      public void close(final TaskAttemptContext taskAttemptContext) throws IOException {
      }
    };
  }

  @Override public void checkOutputSpecs(JobContext jobContext)
      throws IOException {
  }

  @Override public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    return super.getOutputCommitter(taskAttemptContext);
  }
}
