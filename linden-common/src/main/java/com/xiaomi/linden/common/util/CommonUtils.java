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

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommonUtils.class);

  public static String getLocalHost() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Couldn't find the local host.", e);
    }
  }

  private static final String DockerIP = "172.17.42.1";
  private static final String CONTAINER_ENV_KEY = "CONTAINER_ENV";
  private static final String HOST_IP_KEY = "HOST_IP";

  public static String getLocalHostIp() {
    // for container env
    if (System.getenv(CONTAINER_ENV_KEY) != null && System.getenv(CONTAINER_ENV_KEY).equals("true")
        && System.getenv(HOST_IP_KEY) != null) {
      LOGGER.debug("Using the IP address {} from container env", System.getenv(HOST_IP_KEY));
      return System.getenv(HOST_IP_KEY);
    }

    try {
      for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
        for (InetAddress addr : Collections.list(iface.getInetAddresses())) {
          LOGGER.debug("Checking ip address {}", addr);
          String hostAddress = addr.getHostAddress();
          // The docker virtual environment uses a virtual ip which should be skipped.
          if (addr.isSiteLocalAddress()
              && !addr.isLoopbackAddress()
              && !(addr instanceof Inet6Address)
              && !hostAddress.equals(DockerIP)) {
            LOGGER.debug("Ok, the ip {} will be used.", addr);
            return hostAddress;
          }
        }
      }
    } catch (SocketException e) {
      LOGGER.error("Couldn't find the local machine ip.", e);
    }
    throw new IllegalStateException("Couldn't find the local machine ip.");
  }
}
