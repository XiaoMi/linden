namespace java com.xiaomi.linden.thrift.common

enum LindenType {
    STRING = 0,
    INTEGER,
    LONG,
    DOUBLE,
    FLOAT,
    FACET,
}

struct LindenFieldSchema {
    1: required string name,
    2: required LindenType type = LindenType.STRING,
    3: optional bool stored = 0,
    4: optional bool tokenized = 0;
    5: optional bool indexed = 0,
    6: optional bool omitNorms = 0,
    7: optional bool snippet = 0,
    8: optional bool docValues = 0;
    9: optional bool multi = 0;
    10: optional bool omitFreqs = 0,
    11: optional bool dynamicSchema = 0,
}

struct LindenSchema {
    1: required string id,
    2: required list<LindenFieldSchema> fields,
}

struct LindenExplanation {
}

struct LindenExplanation {
    1: required double value,
    2: required string description,
    3: optional list<LindenExplanation> details,
}

struct LindenSnippet {
    1: required string snippet,
}

struct LindenHit {
}

struct LindenHit {
    1: required string id,
    2: required double score,
    3: optional string source,
    4: optional double distance,
    5: optional LindenExplanation explanation,
    6: optional map<string, string> fields,
    7: optional map<string, LindenSnippet> snippets,
    8: optional list<LindenHit> groupHits
}

struct QueryInfo {
    1: required string query,
    2: optional string filter,
    3: optional string sort,
}

struct LindenLabelAndValue {
    1: required string label,
    2: required i32 value,
}

struct LindenFacetResult {
    1: required string dim,
    2: optional string path,
    3: required i32 value = 0,
    4: required i32 childCount = 0,
    5: optional list<LindenLabelAndValue> labelValues,
}

struct AggregationResult {
    1: required string field,
    2: required list<LindenLabelAndValue> labelValues,
}

struct LindenResult {
    1: required bool success = 1,
    2: optional string error,
    3: optional list<LindenHit> hits,
    4: optional i32 totalHits
    5: optional i32 cost,
    6: optional QueryInfo queryInfo,
    7: optional list<LindenFacetResult> facetResults,
    8: optional i32 totalGroups,
    9: optional i32 totalGroupHits;
    10: optional list<AggregationResult> aggregationResults,
}

struct CacheInfo {
    1: required double hitRate,
    2: optional i64 hitCount,
    3: optional i64 missCount,
    4: optional i64 loadSuccessCount,
    5: optional i64 loadExceptionCount,
    6: optional i64 totalLoadTime,
    7: optional double missRate,
    8: optional double averageLoadPenalty,
}

struct JVMInfo {
    1: optional i64 totalMemory,
    2: optional i64 freeMemory,
    3: optional i64 maxMemory,
    4: optional string arguments,
    5: optional i32 gcCount,
    6: optional i64 gcTime,
}

struct FileDiskUsageInfo {
    1: required string dirName,
    2: optional i64 diskUsage
}

struct LindenServiceInfo {
    1: required i32 docsNum,
    2: optional CacheInfo cacheInfo,
    3: optional JVMInfo jvmInfo
    4: optional list<string> indexNames,
    5: optional list<FileDiskUsageInfo> fileUsedInfos,
}
