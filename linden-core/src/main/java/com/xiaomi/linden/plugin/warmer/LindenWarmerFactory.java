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

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import com.xiaomi.linden.plugin.LindenPluginFactory;
import com.xiaomi.linden.plugin.ClassLoaderUtils;

public class LindenWarmerFactory implements LindenPluginFactory<LindenWarmer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LindenWarmerFactory.class);
  private static final String PLUGIN_PATH = "plugin.path";
  private static final String CLASS_NAME = "class.name";

  @Override
  public LindenWarmer getInstance(Map<String,String> config) throws IOException {
    try {
      String pluginPath = config.get(PLUGIN_PATH);
      if (pluginPath == null) {
        throw new IOException("Linden warmer plugin path is null");
      }

      String className = config.get(CLASS_NAME);
      Class<?> clazz = ClassLoaderUtils.load(pluginPath, className);
      if (clazz != null) {
        LindenWarmer lindenWarmer = (LindenWarmer) clazz.getConstructor().newInstance();
        lindenWarmer.init(config);
        return lindenWarmer;
      } else {
        throw new IOException("Linden warmer plugin " + className + " not found.");
      }
    } catch (Exception e) {
      LOGGER.warn("Get Linden warmer instance failed, use default, {}", Throwables.getStackTraceAsString(e));
    }
    return LindenWarmer.DEFAULT;
  }
}
