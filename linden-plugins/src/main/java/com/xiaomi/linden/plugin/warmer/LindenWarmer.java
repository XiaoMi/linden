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

package com.xiaomi.linden.plugin.warmer;

import java.util.Map;

import com.xiaomi.linden.plugin.LindenPlugin;
import com.xiaomi.linden.thrift.service.LindenService;

public abstract class LindenWarmer implements LindenPlugin {

  public abstract void warmUp(LindenService.ServiceIface iface);

  public abstract void close();

  public static LindenWarmer DEFAULT = new LindenWarmer() {

    @Override
    public void warmUp(LindenService.ServiceIface iface) {
      // do nothing
    }

    @Override
    public void init(Map<String,String> config) {
      // do nothing
    }

    @Override
    public void close() {
      // do nothing
    }
  };
}
