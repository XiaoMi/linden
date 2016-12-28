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

import com.google.common.base.Joiner;

import com.xiaomi.linden.thrift.common.FileDiskUsageInfo;
import com.xiaomi.linden.thrift.common.JVMInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class RuntimeInfoUtils {

  private static final Joiner joiner = Joiner.on(' ');
  private static final String WINDOWS_OS = "window";

  public static JVMInfo getJVMInfo() {
    JVMInfo jvmInfo = new JVMInfo();

    Runtime runtime = Runtime.getRuntime();
    jvmInfo.setFreeMemory(runtime.freeMemory());
    jvmInfo.setTotalMemory(runtime.totalMemory());
    jvmInfo.setMaxMemory(runtime.maxMemory());

    int gcCount = 0;
    long gcTime = 0;
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      long count = gc.getCollectionCount();
      if (count >= 0) {
        gcCount += count;
      }
      long time = gc.getCollectionTime();
      if (time >= 0) {
        gcTime += time;
      }
    }
    jvmInfo.setGcCount(gcCount);
    jvmInfo.setGcTime(gcTime);
    List<String> args = ManagementFactory.getRuntimeMXBean().getInputArguments();
    jvmInfo.setArguments(joiner.join(args));
    return jvmInfo;
  }

  public static List<FileDiskUsageInfo> getRuntimeFileInfo(List<String> paths) {
    List<FileDiskUsageInfo> runtimeInfos = new ArrayList<>();
    for (String path : paths) {
      long size;
      try {
        size = totalFileSize(path);
      } catch (Exception e) {
        continue;
      }
      FileDiskUsageInfo fileInfo = new FileDiskUsageInfo()
          .setDirName(path)
          .setDiskUsage(size);
      runtimeInfos.add(fileInfo);
    }
    return runtimeInfos;
  }

  public static long totalFileSize(String filePath) throws Exception {
    String realPath = System.getProperty("os.name").toLowerCase().contains(WINDOWS_OS)
            ? filePath.substring(1) : filePath;
    Path path = Paths.get(realPath);
    final AtomicLong size = new AtomicLong(0);
    final AtomicLong dirCount = new AtomicLong(0);
    Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        Objects.requireNonNull(attrs);
        size.addAndGet(attrs.size());
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        if (exc != null) {
          throw exc;
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        if (exc != null) {
          throw exc;
        }
        dirCount.getAndIncrement();
        return FileVisitResult.CONTINUE;
      }
    });
    return size.get() + 4 * 1024 * dirCount.get();
  }
}
