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

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;

import java.io.IOException;
import java.net.*;
import java.util.Collections;
import java.util.Random;

public class CommonUtils {

  public static int getAvailablePort() {
    int port = new Random().nextInt(10000) + 10000;
    while (!isPortAvailable(port)) {
      port = new Random().nextInt(10000) + 10000;
    }
    return port;
  }

  public static boolean isPortAvailable(int port) {
    try {
      ServerSocket server = new ServerSocket(port);
      server.close();
      return true;
    } catch (IOException e) {
    }
    return false;
  }

  public static String getLocalHost() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
    }
    throw new IllegalStateException("Couldn't find the local host.");
  }

  public static <T extends TBase> String ThriftToJSON(T thrift) {
    TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
    try {
      return serializer.toString(thrift);
    } catch (TException e) {
    }
    throw new IllegalStateException("Convert to json failed : " + thrift);
  }

  public static String getLocalHostIp() {
    try {
      for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
        if (!iface.getName().startsWith("vmnet")) {
          for (InetAddress raddr : Collections.list(iface.getInetAddresses())) {
            if (raddr.isSiteLocalAddress() && !raddr.isLoopbackAddress() && !(raddr instanceof Inet6Address)) {
              return raddr.getHostAddress();
            }
          }
        }
      }
    } catch (SocketException e) {
    }
    throw new IllegalStateException("Couldn't find the local machine ip.");
  }
}
