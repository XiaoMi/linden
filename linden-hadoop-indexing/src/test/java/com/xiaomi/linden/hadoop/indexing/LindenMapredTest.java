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

package com.xiaomi.linden.hadoop.indexing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.xiaomi.linden.hadoop.indexing.job.LindenJob;
import com.xiaomi.linden.hadoop.indexing.keyvalueformat.IntermediateForm;
import com.xiaomi.linden.hadoop.indexing.keyvalueformat.Shard;
import com.xiaomi.linden.hadoop.indexing.map.LindenMapper;

public class LindenMapredTest {
  private static MapDriver<Object, Object, Shard, IntermediateForm> mDriver;
  private static String indexPath = "/tmp/LindenMapredTest";

  @BeforeClass
  public static void init() throws IOException {
    LindenMapper mapper = new LindenMapper();
    mDriver = MapDriver.newMapDriver(mapper);
    int numShards = 1;
    Shard[] shards = LindenJob.createShards(indexPath, numShards);
    Shard.setIndexShards(mDriver.getConfiguration(), shards);
  }

  @AfterClass
  public static void destroy() throws IOException {
    FileUtils.deleteDirectory(new File(indexPath));
  }

  @Test
  public void TestMapper() throws IOException {
    try {
      String propertiesFilePath = LindenMapredTest.class.getClassLoader().getResource("linden.properties").getFile();
      Files.copy(new File(propertiesFilePath).toPath(), Paths.get("lindenProperties"), StandardCopyOption.REPLACE_EXISTING);
      String schemaFilePath = LindenMapredTest.class.getClassLoader().getResource("schema.xml").getFile();
      Files.copy(new File(schemaFilePath).toPath(), Paths.get("lindenSchema"), StandardCopyOption.REPLACE_EXISTING);
      String json = "{\"id\":0,\"groupid\":\"0\",\"tags\":\"hybrid,leather,moon-roof,reliable\",\"category\":\"compact\",\"mileage\":14900,\"price\":7500,\"contents\":\"yellow compact hybrid leather moon-roof reliable u.s.a. florida tampa asian acura 1.6el \",\"color\":\"yellow\",\"year\":1994,\"makemodel\":\"asian/acura/1.6el\",\"city\":\"u.s.a./florida/tampa\"}";
      mDriver.withInput(new LongWritable(1L), new Text(json.getBytes()));
      mDriver.run();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      FileUtils.deleteQuietly(Paths.get("lindenProperties").toFile());
      FileUtils.deleteQuietly(Paths.get("lindenSchema").toFile());
    }
  }
}
