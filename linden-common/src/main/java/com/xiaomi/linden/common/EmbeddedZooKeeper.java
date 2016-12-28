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

package com.xiaomi.linden.common;

import com.google.common.base.Throwables;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class EmbeddedZooKeeper extends ZooKeeperServerMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedZooKeeper.class);
  private Thread thread;
  private Properties zkConfig = null;
  private File zkDir;
  private int port = 2181;

  public EmbeddedZooKeeper(File dir) {
    this.zkDir = dir;
  }

  public EmbeddedZooKeeper(File dir, int clientPort) {
    this.port = clientPort;
    this.zkDir = dir;
  }

  public void start() throws Exception {
    try {
      // zkDir = genZookeeperDataDir();
      zkConfig = genZookeeperConfig(zkDir);
      port = Integer.valueOf(zkConfig.getProperty("clientPort"));
      QuorumPeerConfig qpConfig = new QuorumPeerConfig();
      qpConfig.parseProperties(zkConfig);
      final ServerConfig sConfig = new ServerConfig();
      sConfig.readFrom(qpConfig);


      thread = new Thread() {

        @Override
        public void run() {
          try {
            LOGGER.info("Starting ZK server");
            runFromConfig(sConfig);
          } catch (Throwable t) {
            LOGGER.error("Failure in embedded ZooKeeper", t);
          }
        }
      };

      thread.start();
      Thread.sleep(500);
    } catch (Throwable t) {
      throw new Exception("Cannot start embedded zookeeper", t);
    }
  }

  private File genZookeeperDataDir() {
    File zkDir = null;
    try {
      zkDir = File.createTempFile("zoo", "data");
      if (!zkDir.delete())
        throw new IOException("Can't rm zkDir " + zkDir.getCanonicalPath());
      if (!zkDir.mkdir())
        throw new IOException("Can't mkdir zkDir " + zkDir.getCanonicalPath());
    }catch(IOException e){
      LOGGER.error("Can't make zookeeper data dir");
    }
    return zkDir;
  }

  private Properties genZookeeperConfig(File zkDir) throws IOException {
    Properties props = new Properties();
    props.setProperty("timeTick", "2000");
    props.setProperty("initLimit", "10");
    props.setProperty("syncLimit", "5");
    try {
      props.setProperty("dataDir", zkDir.getCanonicalPath());
    } catch (IOException e){
      LOGGER.error("Can't create zkConfig, zkDir has no path");
    }

    props.setProperty("clientPort", String.valueOf(port));
    return props;
  }

  public void shutdown() {
    try {
      LOGGER.info("Stopping ZK server");
      thread.interrupt();
    } catch (Exception e) {
      LOGGER.error("Shutdonw failed : {}", Throwables.getStackTraceAsString(e));
    } finally {
      try {
        FileUtils.deleteDirectory(zkDir);
      } catch (Throwable t) {
        LOGGER.error("Cannot cleanup tmp dirs {}", zkDir.getAbsolutePath());
      }
    }
  }
}
