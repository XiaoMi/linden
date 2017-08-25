Linden provides user the ability to rewrite some linden component in plugin mode, for example Analyzer, Merge Policy, Metric Manager, Index Warmer and Similarity. Linden will initialize linden plugin automatically when linden loads config from linden.properties. Of course you need make sure JVM could find the plugin jar.

### Plugin Mode
Linden plugin has 2 modes: Direct and Factory.

<br>

### Direct Mode
In direct mode, the plugin class can be initialized directly by the function init.
 
	public interface LindenPlugin {
	  void init(Map<String,String> config);
	  void close();
	}
 
Your plugin class should implement LindenPlugin interface. init function is called by LindenPluginManager and the config parameter is generated from linden.properties by LindenPluginManager.

##### Gateway
Linden implements KafkaGateway and SimpleFileGateway in plugin mode, so linden supports indexing data from kafka and text file natively.  Index data is in JSON string format, please see [Linden Work Mode Document](LindenWorkMode.md) for detail.

***com.xiaomi.linden.plugin.gateway.LindenGateway***:
 
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
 
 
***com.xiaomi.linden.plugin.gateway.kafkaKafkaGateway***:
 
	public class KafkaGateway extends LindenGateway {
	  private static final String ZOOKEEPER = "zookeeper";
	  private static final String GROUP = "group";
	  private static final String TOPIC = "topic";
	  @Override
	  public DataProvider buildDataProvider() throws IOException {
	    Preconditions.checkNotNull(config.get(ZOOKEEPER));
	    Preconditions.checkNotNull(config.get(GROUP));
	    Preconditions.checkNotNull(config.get(TOPIC));
	    return new KafkaDataProvider(config.get(ZOOKEEPER), config.get(TOPIC), config.get(GROUP));
	  }
	}
 
If you want to enable kafka gateway, gateway config should be like this in linden.properties
 
	gateway.class=com.xiaomi.linden.plugin.gateway.kafka.KafkaGateway
	gateway.zookeeper=zk1.xiaomi.bj:2181,zk2.xiaomi.bj:2181,zk3.xiaomi.bj:2181/kafka
	gateway.topic=kafka-topic
	gateway.group=kafka-group
 
The “class” suffix in config name indicates the config value is a linden plugin class name.
All config with “gateway” prefix will be stripped the prefix and passed to plugin init function in map format. The config parameter of this example is
 
	{
	    "zookeeper":"zk1.xiaomi.bj:2181,zk2.xiaomi.bj:2181,zk3.xiaomi.bj:2181/kafka",
	    "topic":"kafka-topic",
	    "group":"kafka-group"
	}
 
Then Linden will consume data from DataProvider built from KafkaGateway.

##### MetricsManager
Linden provides ***com.xiaomi.linden.plugin.metrics.MetricsManager*** which is designed to report linden working status. This is an abstract class, you can report linden status to your own report system by extending MetricsManager and enable it in direct mode.

##### LindenWarmer
Linden provides ***com.xiaomi.linden.plugin.warmer.LindenWarmer*** which is designed to warm linden before linden serves traffic. This is an abstract class, you can warm linden in your own way by extending LindenWarmer and enable it in direct mode.
 
<br>

### Factory Mode
In factory mode, you need create a sub class of LindenPluginFactory which implements getInstance to load properties and initialize the plugin class instance.
 
	public interface LindenPluginFactory<T> {
	  T getInstance(Map<String,String> params) throws IOException;
	}
	 

<br>

Linden provides all lucene native analyzers and two Chinese analyzers(MMSeg4J and Jieba) as index and search analyzer options.
Index analyzer is used to analyze text when indexing data and search analyzer is used to analyze query when searching. Generally, the two analyzers should be the same one. You can implement your own analyzer in plugin mode by extending ***org.apache.lucene.analysis.Analyzer***.

##### LindenMMSeg4jAnalyzerFactory
 
	public class LindenMMSeg4jAnalyzerFactory implements LindenPluginFactory<LindenMMSeg4jAnalyzer> {
	  private static final String MODE = "mode";
	  private static final String DICT = "dict";
	  private static final String STOPWORDS = "stopwords";
	  private static final String CUT_LETTER_DIGIT = "cut_letter_digit";
	 
	  @Override
	  public LindenMMSeg4jAnalyzer getInstance(Map<String, String> params) throws IOException {
	    String mode = params.get(MODE);
	    String dictPath = params.get(DICT);
	    String stopWordPath = params.get(STOPWORDS);
	    boolean cut = params.containsKey(CUT_LETTER_DIGIT) && params.get(CUT_LETTER_DIGIT).equalsIgnoreCase("TRUE");
	    return new LindenMMSeg4jAnalyzer(mode, dictPath, stopWordPath, cut);
	  }
	}
 
Now you can use MMSegAnalyzer to extracting index terms by configuration:
 
	index.analyzer.class=com.xiaomi.linden.lucene.analyzer.LindenMMSeg4jAnalyzerFactory
	index.analyzer.dict=/data/mmseg4j_dict
	index.analyzer.mode=Complex
	index.analyzer.cut_letter_digit=true

See MMSeg4J analyzer detail: [https://github.com/chenlb/mmseg4j-solr](https://github.com/chenlb/mmseg4j-solr)
	
##### LindenJiebaAnalyzerFactory
	
	public class LindenJiebaAnalyzerFactory implements LindenPluginFactory<LindenJiebaAnalyzer> {
	
	  private static final String MODE = "mode";
	  private static final String USER_DICT = "user.dict";
	
	  @Override
	  public LindenJiebaAnalyzer getInstance(Map<String, String> params) throws IOException {
	    String mode = params.get(MODE);
	    String userDict = params.get(USER_DICT);
	    JiebaSegmenter.SegMode segMode = JiebaSegmenter.SegMode.SEARCH;
	
	    if (mode != null && mode.equalsIgnoreCase("index")) {
	      segMode = JiebaSegmenter.SegMode.INDEX;
	    } else {
	      segMode = JiebaSegmenter.SegMode.SEARCH;
	    }
	    return new LindenJiebaAnalyzer(segMode,userDict);
	  }
	}

Jieba Analyzer config example:
	
	search.analyzer.class=com.xiaomi.linden.lucene.analyzer.LindenJiebaAnalyzerFactory
	search.analyzer.mode=search
	
See Jieba analyzer detail: [https://github.com/huaban/jieba-analysis](https://github.com/huaban/jieba-analysis)

##### LindenStandardAnalyzerFactory
***com.xiaomi.linden.lucene.analyzer.LindenStandardAnalyzerFactory*** is a very simple plugin factory which provides ***org.apache.lucene.analysis.standard.StandardAnalyzer*** with stopwords switch.

##### SortingMergePolicyFactory
***com.xiaomi.linden.lucene.merge.SortingMergePolicyFactory*** provides a configurable sort merge policy which can be used to overwrite default merge policy.

##### TieredMergePolicyFactory
***com.xiaomi.linden.lucene.merge.TieredMergePolicyFactory*** provides a configurable tiered merge policy which can be used to overwrite default merge policy.

##### LindenSimilarityFactory
***com.xiaomi.linden.lucene.similarity.LindenSimilarityFactory*** provides a customized linden similarity which extends TFIDFSimilarity. IDF value of linden similarity is read from data file, not calculated from index.

##### MetricsManagerFactory
***com.xiaomi.linden.plugin.metrics.MetricsManagerFactory*** provides MetricsManager factory, so you can use it for MetricsManager in factory mode.

##### LindenWarmerFactory
***com.xiaomi.linden.plugin.warmer.LindenWarmerFactory*** provides LindenWarmer factory, so you can use it for LindenWarmer in factory mode.
 
<br>
 
### LindenPluginFactoryWrapper
***com.xiaomi.linden.plugin.LindenPluginFactoryWrapper*** is a plugin factory wrapper. It allows you to import any outside linden factory.
 
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
 
An example configuration:

	index.analyzer.class=com.xiaomi.linden.plugin.LindenPluginFactoryWrapper
	index.analyzer.class.name=com.xiaomi.oakbay.linden.plugins.analyzer.IKAnalyzerFactory
	index.analyzer.factory.path=/home/work/soft/linden/plugins/
	index.analyzer.use.smart=false
 
This example specifies linden index analyzer with IKAnalyzer.
***com.xiaomi.oakbay.linden.plugins.analyzer.IKAnalyzerFactory*** is plugin factory class name.

***index.analyzer.factory.path specifies*** where to load the factory.

***index.analyzer.use.smart*** is IKAnalyzer internal property, passed to IKAnalyzerFactory with use.smart as property name.