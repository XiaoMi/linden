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

package com.xiaomi.linden.file;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.io.Files;

import com.xiaomi.linden.common.util.DirectoryChangeWatcher;
import com.xiaomi.linden.common.util.FileChangeWatcher;

@Ignore
public class TestFileChangeWatcher {

  private FileChangeWatcher fileChangeWatcher;
  private DirectoryChangeWatcher directoryChangeWatcher;

  @Test
  public void testFileChangeWatcher() throws IOException, InterruptedException {
    final AtomicInteger events = new AtomicInteger(0);
    final Function<String,Object> callback = new Function<String,Object>() {
      @Override
      public String apply(String absolutePath) {
        events.incrementAndGet();
        return null;
      }
    };

    File dir = new File("tmp");
    if (!dir.exists()) {
      dir.mkdir();
    }

    File file = new File("tmp/test.txt");
    file.createNewFile();

    fileChangeWatcher = new FileChangeWatcher(file.getAbsolutePath(), callback, 10);
    fileChangeWatcher.start();
    directoryChangeWatcher = new DirectoryChangeWatcher(dir.getAbsolutePath(), callback, 10);
    directoryChangeWatcher.start();

    Files.write("Hello", file, Charsets.UTF_8);
    Thread.sleep(10 * 1000);
    Assert.assertEquals(2, events.get());

    FileUtils.deleteQuietly(file);
    FileUtils.deleteDirectory(dir);
  }

  @After
  public void destroy() throws IOException {
    if (fileChangeWatcher != null) {
      fileChangeWatcher.close();
    }
    if (directoryChangeWatcher != null) {
      directoryChangeWatcher.close();
    }

  }
}
