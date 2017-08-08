
When Linden starts, it need a configuration file named "linden.properties" in configuration dir. It specifies all configurable properties in linden.
 
Properties | Default Value | Description | Required
------------- | ------------- | ------------- | -------------
port | 9090 | Linden thrift server port | N
admin.port | 0 | Linden admin http server port. Set admin.port will enable admin http server and linden http API | N
webapp | null | Web app path of linden admin server | Required by admin.port 
cluster.url | null | Linden zookeeper cluster path | Y
shard.id | 0 | Index shard id | N
index.directory | null | Path of index directory | Y
index.refresh.time | 60 | Seconds between documents is injected to linden and visible to user. | N
plugin.path | null | Score model plugin directory path, see score model plugin part at [BQL document](BQL.md#score-model) | N
cluster.future.await.timeout | 1000 | Collection timeout of linden instances results in milliseconds | N
cluster.future.pool.wait.timeout | 200 | Waiting timeout of linden cluster request future in ExecutorServiceFuturePool | N
instance.future.pool.wait.timeout | 200 | Waiting timeout of linden instance request future in ExecutorServiceFuturePool | N
enable.parallel.search | true | If enable parallel search | N
log.path | null | Linden log directory path | Y
enable.cache | false | If enable search cache | N
cache.duration | 10 | Cache expiration limit in second | N
cache.size | 50000 | Cache size indicate how many recent results are cached | N
index.manager.thread.num | 11 | Indexing thread number in index manager | N
linden.core.mode | SIMPLE | One linden core corresponds to one physical index directory. There are three linden core modes: <br> <br> SIMPLE: the default model used in linden, only one active linden core and it will never be replaced <br> <br> HOTSWAP: there is always only one active linden core and another linden core is in preparation status. After preparation, prepared linden core can be swapped to active status to replace the previous one <br> <br> MULTI: one shard index is divided to several sub-shard indexes according some rules. Each sub-shard index corresponds to one linden core. See more in [Linden Work Mode Document](LindenWorkMode.md) | N
multi.index.division.type | null | The type of multi-core linden mode <br> <br> TIME\_HOUR: the index is split in time by hour to sub-index <br> <br> TIME\_DAY: the index is split in time by day to sub-index <br> <br> TIME\_MONTH: the index is split in time by month to sub-index <br> <br> TIME\_YEAR: the index is split in time by year to sub-index <br> <br> DOC\_NUM: the index is split by document number, which is defined by multi.index.doc.num.limit <br> <br> INDEX_NAME: the index is split by user customized index name | N
multi.index.doc.num.limit | 10M | Index split document number threshold in multi-core DOC\_NUM mode |N
multi.index.max.live.index.num | -1 | Linden sub-index number limit of TIME and DOC\_NUM division type in multi-index mode, while it doesn’t work for INDEX\_NAME division type | N
search.time.limit | -1 | Time limit in lucene doc collecting stage, -1 means no limit | N
enable.source.field.cache | false | If true prefer fetching source field value in field cache way than stored field from document | N
search.thread.pool.json.config | null | Linden cluster and instance search thread pool config, for example: {"cluster":{"min":10,"max":20,"queueSize":1000},"instance":{"min":15,"max":30,"queueSize":2000}} | N
gateway.class | null | Gateway plugin, specify the source of index data <br>  <br> ***com.xiaomi.linden.plugin.gateway.kafka.KafkaGateway***, which means that linden fetching index data from Kafka. <br>  <br> ***com.xiaomi.linden.plugin.gateway.file.SimpleFileGateway***, which means that linden fetching index data from local file. See [Linden Plugin Document](LindenPlugin.md) | N
search.similarity.class | null | Similarity plugin, default similarity is <br>  <br> ***org.apache.lucene.search.similarities.DefaultSimilarity***  <br>  <br> ***org.apache.lucene.search.similarities.BM25Similarity*** is another option. See [Linde Plugin Document](LindenPlugin.md) | N
index.analyzer.class | null | Index analyzer plugin, default index analyzer is lucene standard analyzer. See [Linden Plugin Document](LindenPlugin.md) | N
search.analyzer.class | null | Search analyzer plugin, default search analyzer is lucene standard analyzer. See [Linden Plugin Document](LindenPlugin.md) | N
linden.metric.class | null | Linden metric plugin, which report linden performance data. See [Linden Plugin Document](LindenPlugin.md) | N
linden.warmer.class | null | Linden warmer plugin, which will warm linden before linden really serves traffic in restart stage. See [Linden Plugin Document](LindenPlugin.md) | N
merge.policy.class | null | Index merge policy plugin. See [Linden Plugin Document](LindenPlugin.md) | N
