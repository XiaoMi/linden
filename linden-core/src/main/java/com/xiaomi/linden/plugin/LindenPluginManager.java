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

package com.xiaomi.linden.plugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LindenPluginManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LindenPluginManager.class);
  private final Map<String, PluginHolder> pluginMap = new HashMap<>();

  private static final String CLASS_SUFFIX = ".class";

  private LindenPluginManager() {
  }

  public static LindenPluginManager build(Map<String, String> conf) {
    LindenPluginManager pluginManager = new LindenPluginManager();
    if (conf == null) {
      return pluginManager;
    }

    List<PluginHolder> plugins = new ArrayList<>();
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key.endsWith(CLASS_SUFFIX)) {
        String pluginName = key.substring(0, key.lastIndexOf(CLASS_SUFFIX));
        plugins.add(new PluginHolder(pluginName, value));
      }
    }
    for (PluginHolder plugin : plugins) {
      pluginManager.pluginMap.put(plugin.getPluginName(), plugin);
      for (Map.Entry<String, String> entry : conf.entrySet()) {
        if (entry.getKey().startsWith(plugin.getPluginName())) {
          String property = entry.getKey().substring(plugin.getPluginName().length() + 1);
          plugin.addConfig(property, entry.getValue());
        }
      }
      LOGGER.info("Plugin : {}", plugin);
    }
    return pluginManager;
  }

  @SuppressWarnings("unchecked")
  public <T> T getInstance(String pluginTag, Class<T> type) throws IOException {
    String pluginName = pluginTag.substring(0, pluginTag.lastIndexOf(CLASS_SUFFIX));
    PluginHolder pluginHolder = pluginMap.get(pluginName);
    if (pluginHolder != null) {
      return (T) pluginHolder.getInstance();
    }
    return null;
  }
}
