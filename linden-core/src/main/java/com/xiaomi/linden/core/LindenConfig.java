package com.xiaomi.linden.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.spatial4j.core.context.SpatialContext;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.common.schema.LindenSchemaConf;
import com.xiaomi.linden.plugin.LindenPluginManager;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenType;

public class LindenConfig {

  public enum IndexType {
    RAM,
    MMAP,
  }

  public enum MultiIndexDivisionType {
    TIME_HOUR,
    TIME_DAY,
    TIME_MONTH,
    TIME_YEAR,
    DOC_NUM,
    INDEX_NAME,
  }

  public enum LindenCoreMode {
    SIMPLE,
    MULTI,
    HOTSWAP,
  }

  public enum CommandType {
    SWAP_INDEX,
    MERGE_INDEX,
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LindenConfig.class);

  private Map<String, String> properties;
  private String indexDirectory;
  private int port;
  private String clusterUrl;
  private int shardId;
  private String searchSimilarity;
  private String searchAnalyzer;
  private String indexAnalyzer;
  private String gateway;
  private IndexType indexType;
  private String logPath;
  private int clusterFutureAwaitTimeout;
  private int clusterFuturePoolWaitTimeout;
  private int instanceFuturePoolWaitTimeout;
  private int adminPort;
  private com.xiaomi.linden.thrift.common.LindenSchema schema;
  private boolean enableParallelSearch;
  private int cacheDuration;
  private int cacheSize;
  private boolean enableCache;
  private String pluginPath;
  private int indexRefreshTime;
  private String webapp;
  private LindenCoreMode lindenCoreMode;
  private MultiIndexDivisionType multiIndexDivisionType;
  private int multiIndexDocNumLimit;
  private int multiIndexMaxLiveIndexNum;
  private int searchTimeLimit;
  private String lindenMetricFactory;
  private int indexManagerThreadNum;
  private String searchThreadPoolConfig;
  private boolean enableSourceFieldCache;
  private String lindenWarmerFactory;

  private Map<String, LindenFieldSchema> fieldSchemaMap = new HashMap<>();

  public LindenConfig() {
    this.port = 9090;
    this.shardId = 0;
    this.indexType = IndexType.MMAP;
    this.clusterFutureAwaitTimeout = 2000;
    this.clusterFuturePoolWaitTimeout = 200;
    this.instanceFuturePoolWaitTimeout = 200;
    this.enableParallelSearch = true;
    this.cacheDuration = 10;
    this.cacheSize = 50000;
    this.enableCache = false;
    this.indexRefreshTime = 60;
    this.lindenCoreMode = LindenCoreMode.SIMPLE;
    this.multiIndexDocNumLimit = 100000000;
    this.multiIndexMaxLiveIndexNum = -1;
    this.searchTimeLimit = -1;
    this.indexManagerThreadNum = 11;
    this.enableSourceFieldCache = false;
  }

  public void putToProperties(String key, String val) {
    if (this.properties == null) {
      this.properties = new HashMap<>();
    }
    this.properties.put(key, val);
  }

  public Map<String, String> getProperties() {
    return this.properties;
  }


  public String getIndexDirectory() {
    return this.indexDirectory;
  }

  public LindenConfig setIndexDirectory(String indexDirectory) {
    this.indexDirectory = indexDirectory;
    return this;
  }

  public int getPort() {
    return this.port;
  }

  public LindenConfig setPort(int port) {
    this.port = port;
    return this;
  }


  public String getClusterUrl() {
    return this.clusterUrl;
  }

  public LindenConfig setClusterUrl(String clusterUrl) {
    this.clusterUrl = clusterUrl;
    return this;
  }

  public int getShardId() {
    return this.shardId;
  }

  public LindenConfig setShardId(int shardId) {
    this.shardId = shardId;
    return this;
  }

  public String getSearchSimilarity() {
    return this.searchSimilarity;
  }

  public LindenConfig setSearchSimilarity(String searchSimilarity) {
    this.searchSimilarity = searchSimilarity;
    return this;
  }

  public String getSearchAnalyzer() {
    return this.searchAnalyzer;
  }

  public LindenConfig setSearchAnalyzer(String searchAnalyzer) {
    this.searchAnalyzer = searchAnalyzer;
    return this;
  }

  public String getIndexAnalyzer() {
    return this.indexAnalyzer;
  }

  public LindenConfig setIndexAnalyzer(String indexAnalyzer) {
    this.indexAnalyzer = indexAnalyzer;
    return this;
  }

  public String getGateway() {
    return this.gateway;
  }

  public LindenConfig setGateway(String gateway) {
    this.gateway = gateway;
    return this;
  }

  public IndexType getIndexType() {
    return this.indexType;
  }

  public LindenConfig setIndexType(IndexType indexType) {
    this.indexType = indexType;
    return this;
  }

  public String getLogPath() {
    return this.logPath;
  }

  public LindenConfig setLogPath(String logPath) {
    this.logPath = logPath;
    return this;
  }

  public int getClusterFutureAwaitTimeout() {
    return this.clusterFutureAwaitTimeout;
  }

  public LindenConfig setClusterFutureAwaitTimeout(int timeout) {
    this.clusterFutureAwaitTimeout = timeout;
    return this;
  }

  public int getClusterFuturePoolWaitTimeout() {
    return this.clusterFuturePoolWaitTimeout;
  }

  public LindenConfig setClusterFuturePoolWaitTimeout(int timeout) {
    this.clusterFuturePoolWaitTimeout = timeout;
    return this;
  }

  public int getInstanceFuturePoolWaitTimeout() {
    return this.instanceFuturePoolWaitTimeout;
  }

  public LindenConfig setInstanceFuturePoolWaitTimeout(int timeout) {
    this.instanceFuturePoolWaitTimeout = timeout;
    return this;
  }

  public int getAdminPort() {
    return this.adminPort;
  }

  public LindenConfig setAdminPort(int adminPort) {
    this.adminPort = adminPort;
    return this;
  }

  public com.xiaomi.linden.thrift.common.LindenSchema getSchema() {
    return this.schema;
  }

  public LindenConfig setSchema(com.xiaomi.linden.thrift.common.LindenSchema schema) {
    this.schema = schema;
    for (LindenFieldSchema fieldSchema : schema.getFields()) {
      fieldSchemaMap.put(fieldSchema.getName(), fieldSchema);
    }
    LindenFieldSchema idSchema = new LindenFieldSchema(schema.getId(), LindenType.STRING);
    idSchema.setIndexed(true).setStored(true).setOmitNorms(true);
    fieldSchemaMap.put(idSchema.getName(), idSchema);
    return this;
  }

  public boolean isEnableParallelSearch() {
    return this.enableParallelSearch;
  }

  public LindenConfig setEnableParallelSearch(boolean enableParallelSearch) {
    this.enableParallelSearch = enableParallelSearch;
    return this;
  }

  public int getCacheDuration() {
    return this.cacheDuration;
  }

  public LindenConfig setCacheDuration(int cacheDuration) {
    this.cacheDuration = cacheDuration;
    return this;
  }

  public int getCacheSize() {
    return this.cacheSize;
  }

  public LindenConfig setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
    return this;
  }

  public boolean isEnableCache() {
    return this.enableCache;
  }

  public LindenConfig setEnableCache(boolean enableCache) {
    this.enableCache = enableCache;
    return this;
  }

  public String getPluginPath() {
    return this.pluginPath;
  }

  public LindenConfig setPluginPath(String pluginPath) {
    this.pluginPath = pluginPath;
    return this;
  }

  public int getIndexRefreshTime() {
    return this.indexRefreshTime;
  }

  public LindenConfig setIndexRefreshTime(int indexRefreshTime) {
    this.indexRefreshTime = indexRefreshTime;
    return this;
  }

  public String getWebapp() {
    return this.webapp;
  }

  public LindenConfig setWebapp(String webapp) {
    this.webapp = webapp;
    return this;
  }

  public LindenCoreMode getLindenCoreMode() {
    return this.lindenCoreMode;
  }

  public LindenConfig setLindenCoreMode(LindenCoreMode lindenCoreMode) {
    this.lindenCoreMode = lindenCoreMode;
    return this;
  }

  public MultiIndexDivisionType getMultiIndexDivisionType() {
    return this.multiIndexDivisionType;
  }

  public LindenConfig setMultiIndexDivisionType(MultiIndexDivisionType multiIndexDivisionType) {
    this.multiIndexDivisionType = multiIndexDivisionType;
    return this;
  }

  public int getMultiIndexDocNumLimit() {
    return this.multiIndexDocNumLimit;
  }

  public LindenConfig setMultiIndexDocNumLimit(int multiIndexDocNumLimit) {
    this.multiIndexDocNumLimit = multiIndexDocNumLimit;
    return this;
  }

  public int getMultiIndexMaxLiveIndexNum() {
    return this.multiIndexMaxLiveIndexNum;
  }

  public LindenConfig setMultiIndexMaxLiveIndexNum(int multiIndexMaxLiveIndexNum) {
    this.multiIndexMaxLiveIndexNum = multiIndexMaxLiveIndexNum;
    return this;
  }

  public int getSearchTimeLimit() {
    return this.searchTimeLimit;
  }

  public LindenConfig setSearchTimeLimit(int searchTimeLimit) {
    this.searchTimeLimit = searchTimeLimit;
    return this;
  }

  public String getLindenMetricFactory() {
    return this.lindenMetricFactory;
  }

  public LindenConfig setLindenMetricFactory(String lindenMetricFactory) {
    this.lindenMetricFactory = lindenMetricFactory;
    return this;
  }

  public int getIndexManagerThreadNum() {
    return this.indexManagerThreadNum;
  }

  public LindenConfig setIndexManagerThreadNum(int indexManagerThreadNum) {
    this.indexManagerThreadNum = indexManagerThreadNum;
    return this;
  }

  public String getSearchThreadPoolConfig() {
    return this.searchThreadPoolConfig;
  }

  public LindenConfig setSearchThreadPoolConfig(String searchThreadPoolConfig) {
    this.searchThreadPoolConfig = searchThreadPoolConfig;
    return this;
  }

  public boolean isEnableSourceFieldCache() {
    return this.enableSourceFieldCache;
  }

  public LindenConfig setEnableSourceFieldCache(boolean enableSourceFieldCache) {
    this.enableSourceFieldCache = enableSourceFieldCache;
    return this;
  }

  public String getLindenWarmerFactory() {
    return this.lindenWarmerFactory;
  }

  public LindenConfig setLindenWarmerFactory(String lindenWarmerFactory) {
    this.lindenWarmerFactory = lindenWarmerFactory;
    return this;
  }

  public LindenFieldSchema getFieldSchema(String field) {
    LindenFieldSchema fieldSchema = fieldSchemaMap.get(field);
    if (fieldSchema != null) {
      return fieldSchema;
    }
    // parse dynamic field
    return LindenUtil.parseDynamicFieldSchema(field);
  }

  private LindenPluginManager pluginManager;
  @JsonIgnore
  public LindenPluginManager getPluginManager() {
    if (pluginManager != null) {
      return pluginManager;
    }
    synchronized (this) {
      if (pluginManager == null) {
        pluginManager = LindenPluginManager.build(properties);
      }
    }
    return pluginManager;
  }

  private Analyzer searchAnalyzerInstance;
  @JsonIgnore
  public Analyzer getSearchAnalyzerInstance() throws IOException {
    if (searchAnalyzerInstance != null) {
      return searchAnalyzerInstance;
    }
    synchronized (this) {
      if (searchAnalyzerInstance == null) {
        searchAnalyzerInstance = getPluginManager().getInstance(LindenConfigBuilder.SEARCH_ANALYZER, Analyzer.class);
        if (searchAnalyzerInstance == null) {
          searchAnalyzerInstance = new StandardAnalyzer();
        }
        LOGGER.info("Search analyzer  : {}", searchAnalyzerInstance);
      }
    }
    return searchAnalyzerInstance;
  }

  private Analyzer indexAnalyzerInstance;
  @JsonIgnore
  public Analyzer getIndexAnalyzerInstance() throws IOException {
    if (indexAnalyzerInstance != null) {
      return indexAnalyzerInstance;
    }
    synchronized (this) {
      if (indexAnalyzerInstance == null) {
        indexAnalyzerInstance = getPluginManager().getInstance(LindenConfigBuilder.INDEX_ANALYZER, Analyzer.class);
        if (indexAnalyzerInstance == null) {
          indexAnalyzerInstance = new StandardAnalyzer();
        }
        LOGGER.info("Index analyzer  : {}", indexAnalyzerInstance);
      }
    }
    return indexAnalyzerInstance;
  }

  private Similarity searchSimilarityInstance;
  @JsonIgnore
  public Similarity getSearchSimilarityInstance() throws IOException {
    if (searchSimilarityInstance != null) {
      return searchSimilarityInstance;
    }
    synchronized (this) {
      if (searchSimilarityInstance == null) {
        searchSimilarityInstance =
            getPluginManager().getInstance(LindenConfigBuilder.SEARCH_SIMILARITY, Similarity.class);
        if (searchSimilarityInstance == null) {
          searchSimilarityInstance = new DefaultSimilarity();
        }
        LOGGER.info("Search similarity : {}", searchSimilarityInstance);
      }
    }
    return searchSimilarityInstance;
  }

  private SpatialStrategy spatialStrategy;
  @JsonIgnore
  public SpatialStrategy getSpatialStrategy() {
    if (spatialStrategy != null) {
      return spatialStrategy;
    }
    synchronized (this) {
      if (spatialStrategy == null) {
        int maxLevels = 11;
        SpatialPrefixTree grid = new GeohashPrefixTree(SpatialContext.GEO, maxLevels);
        spatialStrategy = new RecursivePrefixTreeStrategy(grid, LindenSchemaConf.GEO_FIELD);
      }
    }
    return spatialStrategy;
  }

  public IndexWriterConfig createIndexWriterConfig() throws IOException {
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LATEST, getIndexAnalyzerInstance());
    indexWriterConfig.setRAMBufferSizeMB(48);

    MergePolicy mergePolicy = getPluginManager().getInstance(LindenConfigBuilder.MERGE_POLICY, MergePolicy.class);
    if (mergePolicy != null) {
      indexWriterConfig.setMergePolicy(mergePolicy);
    }
    LOGGER.info("Merge policy : {}", mergePolicy == null ? "Default" : mergePolicy);

    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    cms.setMaxMergesAndThreads(8, 1);
    indexWriterConfig.setMergeScheduler(cms);
    return indexWriterConfig;
  }

  public FacetsConfig createFacetsConfig() {
    FacetsConfig facetsConfig = null;
    for (LindenFieldSchema fieldSchema : schema.getFields()) {
      if (fieldSchema.type == LindenType.FACET) {
        if (facetsConfig == null) {
          facetsConfig = new FacetsConfig();
        }
        facetsConfig.setHierarchical(fieldSchema.getName(), true);
        if (fieldSchema.isMulti()) {
          facetsConfig.setMultiValued(fieldSchema.getName(), true);
        }
      }
    }
    return facetsConfig;
  }

  public void deconstruct() {
    pluginManager = null;
    searchAnalyzerInstance.close();
    searchAnalyzerInstance = null;
    indexAnalyzerInstance.close();
    indexAnalyzerInstance = null;
    searchSimilarityInstance = null;
    spatialStrategy = null;
  }
}
