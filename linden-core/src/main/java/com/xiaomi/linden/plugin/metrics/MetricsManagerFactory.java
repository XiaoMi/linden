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

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import com.xiaomi.linden.plugin.LindenPluginFactory;
import com.xiaomi.linden.plugin.ClassLoaderUtils;

public class MetricsManagerFactory implements LindenPluginFactory<MetricsManager> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsManagerFactory.class);
  private static final String PLUGIN_PATH = "plugin.path";
  private static final String CLASS_NAME = "class.name";
  private static final String REPORT_PERIOD = "report.period";
  private static final String REPORT_TAG = "report.tag";

  @Override
  public MetricsManager getInstance(Map<String,String> config) {
    try {
      String pluginPath = config.get(PLUGIN_PATH);
      if (pluginPath == null) {
        throw new IOException("Plugin path is null");
      }

      String className = config.get(CLASS_NAME);
      Class<?> clazz = ClassLoaderUtils.load(pluginPath, className);
      if (clazz != null) {
        String tag = config.get(REPORT_TAG);
        String periodStr = config.get(REPORT_PERIOD);
        int period = Integer.valueOf(periodStr);
        return (MetricsManager) clazz.getConstructor(int.class, String.class).newInstance(period, tag);
      } else {
        throw new IOException("Plugin " + className + " not found.");
      }
    } catch (Exception e) {
      LOGGER.warn("Get MetricsManager Instance failed, Use Default, {}", Throwables.getStackTraceAsString(e));
    }
    return MetricsManager.DEFAULT;
  }
}
