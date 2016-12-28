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
import java.util.HashMap;
import java.util.Map;


public class PluginHolder {

  protected String pluginName;
  protected String className;
  protected Map<String, String> config = new HashMap<>();
  protected volatile Object instance;
  protected volatile Object factoryCreatedInstance;
  private Object lock = new Object();

  public PluginHolder(String pluginName, String className) {
    this.pluginName = pluginName;
    this.className = className;
  }

  public void addConfig(String key, String value) {
    config.put(key, value);
  }

  public Object getInstance() throws IOException {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = loadClass(className);
        }
      }
    }
    if (instance instanceof LindenPluginFactory) {
      if (factoryCreatedInstance == null) {
        synchronized (lock) {
          if (factoryCreatedInstance == null) {
            factoryCreatedInstance = ((LindenPluginFactory) instance).getInstance(config);
          }
        }
      }
      return factoryCreatedInstance;
    }
    return instance;
  }

  protected synchronized Object loadClass(String className) {
    try {
      try {
        instance = Class.forName(className).newInstance();
      } catch (Exception e) {
        throw e;
      }
      if (instance instanceof LindenPlugin) {
        ((LindenPlugin) instance).init(config);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return instance;
  }

  public String getPluginName() {
    return pluginName;
  }

  public String getClassName() {
    return className;
  }

  @Override
  public String toString() {
    return "PluginHolder{" +
           "pluginName='" + pluginName + '\'' +
           ", className='" + className + '\'' +
           ", config=" + config +
           ", instance=" + instance +
           ", factoryCreatedInstance=" + factoryCreatedInstance +
           '}';
  }
}
