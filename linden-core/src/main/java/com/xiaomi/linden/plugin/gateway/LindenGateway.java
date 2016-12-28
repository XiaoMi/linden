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

package com.xiaomi.linden.plugin.gateway;


import java.io.IOException;
import java.util.Map;

import com.xiaomi.linden.plugin.LindenPlugin;

abstract public class LindenGateway implements LindenPlugin {

  protected Map<String, String> config;

  public abstract DataProvider buildDataProvider() throws IOException;

  @Override
  public void init(Map<String, String> config) {
    this.config = config;

  }

  @Override
  public void close() {
  }
}
