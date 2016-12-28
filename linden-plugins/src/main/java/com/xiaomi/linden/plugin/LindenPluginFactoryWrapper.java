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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LindenPluginFactoryWrapper<T> implements LindenPluginFactory<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LindenPluginFactoryWrapper.class);
  private static final String FACTORY_PATH = "factory.path";
  private static final String CLASS_NAME = "class.name";

  @Override
  public T getInstance(Map<String, String> config) throws IOException {
    try {
      String pluginPath = config.get(FACTORY_PATH);
      if (pluginPath == null) {
        throw new IOException("Factory path is null");
      }
      String className = config.get(CLASS_NAME);
      Class<?> clazz = ClassLoaderUtils.load(pluginPath, className);
      if (clazz != null) {
        LindenPluginFactory<T> pluginFactory = (LindenPluginFactory) clazz.getConstructor().newInstance();
        return pluginFactory.getInstance(config);
      } else {
        throw new IOException("Factory " + className + " not found.");
      }
    } catch (Exception e) {
      LOGGER.warn("Get Linden plugin factory failed, ", e);
      throw new IOException(e);
    }
  }
}
