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

package com.xiaomi.linden.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class LindenConfigBuilder {

  public static final String PORT = "port";
  public static final String INDEX_ANALYZER = "index.analyzer.class";
  public static final String SEARCH_ANALYZER = "search.analyzer.class";
  public static final String SEARCH_SIMILARITY = "search.similarity.class";
  public static final String CLUSTER_FUTURE_AWAIT_TIMEOUT = "cluster.future.await.timeout";
  public static final String CLUSTER_FUTURE_POOL_WAIT_TIMEOUT = "cluster.future.pool.wait.timeout";
  public static final String INSTANCE_FUTURE_POOL_WAIT_TIMEOUT = "instance.future.pool.wait.timeout";
  public static final String ADMIN_PORT = "admin.port";
  public static final String INDEX_DIRECTORY = "index.directory";
  public static final String CLUSTER_URL = "cluster.url";
  public static final String SHARD_ID = "shard.id";
  public static final String LOG_PATH = "log.path";
  public static final String PLUGIN_PATH = "plugin.path";
  public static final String GATEWAY = "gateway.class";
  public static final String ENABLE_PARALLEL_SEARCH = "enable.parallel.search";
  public static final String MERGE_POLICY = "merge.policy.class";
  public static final String USE_CACHE = "enable.cache";
  public static final String CACHE_DURATION = "cache.duration";
  public static final String CACHE_SIZE = "cache.size";
  public static final String INDEX_REFRESH_TIME = "index.refresh.time";
  public static final String LINDEN_CORE_MODE = "linden.core.mode";
  public static final String WEBAPP = "webapp";
  public static final String MULTI_INDEX_DIVISION_TYPE = "multi.index.division.type";
  public static final String MULTI_INDEX_PREFIX_NAME = "multi.index.prefix.name";
  public static final String MULTI_INDEX_DOC_NUM_LIMIT = "multi.index.doc.num.limit";
  public static final String MULTI_INDEX_MAX_LIVE_INDEX_NUM = "multi.index.max.live.index.num";
  public static final String SEARCH_TIME_LIMIT = "search.time.limit";
  public static final String LINDEN_METRIC_FACTORY = "linden.metric.class";
  public static final String LINDEN_WARMER_FACTORY = "linden.warmer.class";
  public static final String INDEX_MANAGER_THREAD_NUM = "index.manager.thread.num";
  public static final String SEARCH_THREAD_POOL_JSON_CONFIG = "search.thread.pool.json.config";
  public static final String ENABLE_SOURCE_FIELD_CACHE = "enable.source.field.cache";

  protected static class FieldInfo {

    private String method;
    private Class<?> type;

    public FieldInfo(String method, Class<?> type) {
      this.method = method;
      this.type = type;
    }

    public String setMethod() {
      return "set" + method;
    }

    public String getMethod() {
      return "get" + method;
    }

    public Class<?> getType() {
      return type;
    }
  }

  private static Map<String, FieldInfo> fieldMap = new HashMap<>();

  static {
    fieldMap.put(INDEX_DIRECTORY, new FieldInfo("IndexDirectory", String.class));
    fieldMap.put(CLUSTER_URL, new FieldInfo("ClusterUrl", String.class));
    fieldMap.put(PORT, new FieldInfo("Port", int.class));
    fieldMap.put(INDEX_ANALYZER, new FieldInfo("IndexAnalyzer", String.class));
    fieldMap.put(SEARCH_ANALYZER, new FieldInfo("SearchAnalyzer", String.class));
    fieldMap.put(SEARCH_SIMILARITY, new FieldInfo("SearchSimilarity", String.class));
    fieldMap.put(SHARD_ID, new FieldInfo("ShardId", int.class));
    fieldMap.put(LOG_PATH, new FieldInfo("LogPath", String.class));
    fieldMap.put(PLUGIN_PATH, new FieldInfo("PluginPath", String.class));
    fieldMap.put(GATEWAY, new FieldInfo("Gateway", String.class));
    fieldMap.put(CLUSTER_FUTURE_AWAIT_TIMEOUT, new FieldInfo("ClusterFutureAwaitTimeout", int.class));
    fieldMap.put(CLUSTER_FUTURE_POOL_WAIT_TIMEOUT, new FieldInfo("ClusterFuturePoolWaitTimeout", int.class));
    fieldMap.put(INSTANCE_FUTURE_POOL_WAIT_TIMEOUT, new FieldInfo("InstanceFuturePoolWaitTimeout", int.class));
    fieldMap.put(ADMIN_PORT, new FieldInfo("AdminPort", int.class));
    fieldMap.put(ENABLE_PARALLEL_SEARCH, new FieldInfo("EnableParallelSearch", boolean.class));
    fieldMap.put(CACHE_DURATION, new FieldInfo("CacheDuration", int.class));
    fieldMap.put(CACHE_SIZE, new FieldInfo("CacheSize", int.class));
    fieldMap.put(USE_CACHE, new FieldInfo("EnableCache", boolean.class));
    fieldMap.put(INDEX_REFRESH_TIME, new FieldInfo("IndexRefreshTime", int.class));
    fieldMap.put(LINDEN_CORE_MODE, new FieldInfo("LindenCoreMode", LindenConfig.LindenCoreMode.class));
    fieldMap.put(WEBAPP, new FieldInfo("Webapp", String.class));
    fieldMap.put(MULTI_INDEX_DIVISION_TYPE,
                 new FieldInfo("MultiIndexDivisionType", LindenConfig.MultiIndexDivisionType.class));
    fieldMap.put(MULTI_INDEX_PREFIX_NAME, new FieldInfo("MultiIndexPrefixName", String.class));
    fieldMap.put(MULTI_INDEX_DOC_NUM_LIMIT, new FieldInfo("MultiIndexDocNumLimit", int.class));
    fieldMap.put(MULTI_INDEX_MAX_LIVE_INDEX_NUM, new FieldInfo("MultiIndexMaxLiveIndexNum", int.class));
    fieldMap.put(SEARCH_TIME_LIMIT, new FieldInfo("SearchTimeLimit", int.class));
    fieldMap.put(LINDEN_METRIC_FACTORY, new FieldInfo("LindenMetricFactory", String.class));
    fieldMap.put(LINDEN_WARMER_FACTORY, new FieldInfo("LindenWarmerFactory", String.class));
    fieldMap.put(INDEX_MANAGER_THREAD_NUM, new FieldInfo("IndexManagerThreadNum", int.class));
    fieldMap.put(SEARCH_THREAD_POOL_JSON_CONFIG, new FieldInfo("SearchThreadPoolConfig", String.class));
    fieldMap.put(ENABLE_SOURCE_FIELD_CACHE, new FieldInfo("EnableSourceFieldCache", boolean.class));
  }

  public static LindenConfig build(File confFile) throws Exception {
    Properties props = new Properties();

    FileInputStream inputStream = new FileInputStream(confFile);
    props.load(inputStream);
    inputStream.close();
    LindenConfig lindenConf = new LindenConfig();
    Iterator<Object> iter = props.keySet().iterator();

    while (iter.hasNext()) {
      String key = (String) iter.next();
      FieldInfo fieldInfo = fieldMap.get(key);
      if (fieldInfo != null) {
        Method method = LindenConfig.class.getDeclaredMethod(fieldInfo.setMethod(), fieldInfo.getType());
        if (fieldInfo.type == int.class) {
          method.invoke(lindenConf, Integer.valueOf(props.getProperty(key)));
        } else if (fieldInfo.type == String.class) {
          method.invoke(lindenConf, props.getProperty(key));
        } else if (fieldInfo.type == boolean.class) {
          method.invoke(lindenConf, Boolean.valueOf(props.getProperty(key)));
        } else if (fieldInfo.type == LindenConfig.LindenCoreMode.class) {
          method.invoke(lindenConf, LindenConfig.LindenCoreMode.valueOf(props.getProperty(key).toUpperCase()));
        } else if (fieldInfo.type == LindenConfig.MultiIndexDivisionType.class) {
          method.invoke(lindenConf, LindenConfig.MultiIndexDivisionType.valueOf(props.getProperty(key).toUpperCase()));
        }
      }
      lindenConf.putToProperties(key, props.getProperty(key));
    }

    // LindenConfig validation
    if (lindenConf.getAdminPort() != 0 && lindenConf.getWebapp() == null) {
      throw new IOException("Webapp is required if admin port is set");
    }

    return lindenConf;
  }

}
