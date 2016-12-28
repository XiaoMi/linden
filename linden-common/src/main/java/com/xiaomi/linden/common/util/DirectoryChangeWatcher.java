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
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.sun.nio.file.SensitivityWatchEventModifier;

public class DirectoryChangeWatcher extends Thread implements Closeable {
  private final Logger LOGGER = LoggerFactory.getLogger(DirectoryChangeWatcher.class);
  private Function<String,?> callback;
  private String directoryPath;
  private int interval;
  private WatchService watcher;

  public DirectoryChangeWatcher(String directoryPath, Function<String,?> callback) {
    this(directoryPath, callback, 10 * 1000);
  }

  public DirectoryChangeWatcher(String directoryPath, Function<String,?> callback, int interval) {
    this.directoryPath = directoryPath;
    this.callback = callback;
    this.interval = interval;
  }

  @Override
  public void run() {
    try {
      watcher = FileSystems.getDefault().newWatchService();
      File dir = new File(directoryPath);
      dir.toPath().register(
          watcher, new WatchEvent.Kind[] {
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_DELETE
          }, SensitivityWatchEventModifier.HIGH);
      LOGGER.info("Directory watcher start to watch {}", directoryPath);

      while (isAlive()) {
        try {
          Thread.sleep(interval);
          WatchKey key = watcher.poll(1000l, TimeUnit.MILLISECONDS);
          if (key == null) {
            continue;
          }

          List<WatchEvent<?>> events = key.pollEvents();
          for (WatchEvent<?> event : events) {
            if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE
                || event.kind() == StandardWatchEventKinds.ENTRY_MODIFY
                || event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
              String file = event.context().toString();
              if (!file.startsWith(".")) {
                doOnChange();
                break;
              }
            }
          }

          if (!key.reset()) {
            LOGGER.info("Directory watcher key not valid.");
          }
        } catch (InterruptedException e) {
          LOGGER.error("Directory watcher thread exit!");
          break;
        }
      }
    } catch (Throwable e) {
      LOGGER.error("Directory watcher error {}", Throwables.getStackTraceAsString(e));
    }
  }

  public void doOnChange() {
    this.callback.apply(directoryPath);
  }

  @Override
  public void close() throws IOException {
    if (watcher != null) {
      watcher.close();
    }
    this.interrupt();
  }
}
