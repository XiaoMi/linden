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

package com.xiaomi.linden.plugin.metrics;

import java.util.Map;

import com.xiaomi.linden.plugin.LindenPlugin;

public abstract class MetricsManager implements LindenPlugin {


  public abstract void mark(String tag);

  public abstract void time(long nano, String tag);

  public abstract void close();

  public static MetricsManager DEFAULT = new MetricsManager() {
    @Override
    public void mark(String tag) {
      // do nothing
    }

    @Override
    public void time(long nano, String tag) {
      // do nothing
    }

    @Override
    public void init(Map<String, String> config) {

    }

    @Override
    public void close() {
      // do nothing
    }
  };
}
