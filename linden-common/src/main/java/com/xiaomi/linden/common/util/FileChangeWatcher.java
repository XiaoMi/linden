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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.sun.nio.file.SensitivityWatchEventModifier;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileChangeWatcher extends Thread implements Closeable {
  private final Logger LOGGER = LoggerFactory.getLogger(FileChangeWatcher.class);
  private Function<String,?> callback;
  private String absolutePath;
  private int interval;
  private WatchService watcher;

  public FileChangeWatcher(String absolutePath, Function<String, ?> callback) {
    this(absolutePath, callback, 10 * 1000);
  }

  public FileChangeWatcher(String absolutePath, Function<String, ?> callback, int interval) {
    this.absolutePath = absolutePath;
    this.callback = callback;
    this.interval = interval;
  }

  @Override
  public void run() {
    try {
      watcher = FileSystems.getDefault().newWatchService();
      Path path = new File(absolutePath).toPath().getParent();
      String fileWatched = FilenameUtils.getName(absolutePath);
      path.register(watcher, new WatchEvent.Kind[] {StandardWatchEventKinds.ENTRY_MODIFY},
          SensitivityWatchEventModifier.HIGH);
      LOGGER.info("File watcher start to watch {}", absolutePath);

      while (isAlive()) {
        try {
          Thread.sleep(interval);
          WatchKey key = watcher.poll(1000l, TimeUnit.MILLISECONDS);
          if (key == null) {
            continue;
          }

          List<WatchEvent<?>> events = key.pollEvents();
          for (WatchEvent<?> event : events) {
            if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
              String file = event.context().toString();
              if (fileWatched.equals(file)) {
                doOnChange();
              }
            }
          }

          if (!key.reset()) {
            LOGGER.info("File watcher key not valid.");
          }
        } catch (InterruptedException e) {
          LOGGER.error("File watcher thread exit!");
          break;
        }
      }
    } catch (Throwable e) {
      LOGGER.error("File watcher error {}", Throwables.getStackTraceAsString(e));
    }
  }

  public void doOnChange() {
    this.callback.apply(absolutePath);
  }

  @Override
  public void close() throws IOException {
    if (watcher != null) {
      watcher.close();
    }
    this.interrupt();
  }
}
