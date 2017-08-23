namespace java com.xiaomi.linden.thrift.common

include "LindenCommon.thrift"

######################################################################
#                   Query Description Begin
######################################################################

struct LindenQuery {
}

enum LindenBooleanClause {
    SHOULD = 0,
    MUST,
    MUST_NOT,
}

enum Operator {
  OR = 0,
  AND = 1,
}

struct LindenRange {
    1: required string field,
    2: required LindenCommon.LindenType type,
    3: optional string startValue,
    4: optional string endValue,
    5: required bool startClosed,
    6: required bool endClosed,
}

struct LindenSearchField {
    1: required string name,
    2: optional double boost = 1,
}

struct LindenTerm {
    1: required string field,
    2: required string value,
}

struct Coordinate {
    1: required double longitude,
    2: required double latitude,
}

struct SpatialParam {
    1: required Coordinate coordinate,
    2: required double distanceRange,
}

struct LindenBooleanSubQuery {
    1: optional LindenBooleanClause clause = LindenBooleanClause.SHOULD,
    2: optional LindenQuery query,
}

struct LindenBooleanQuery {
    1: required list<LindenBooleanSubQuery> queries,
    2: required double minMatch = 1,
    3: required bool disableCoord = 0
}

struct LindenDisMaxQuery {
    1: required double tie,
    2: required list<LindenQuery> queries,
}

struct LindenTermQuery {
    1: required LindenTerm term
}

struct LindenRangeQuery {
    1: required LindenRange range
}

struct LindenQueryStringQuery {
    1: required string query,
    2: optional Operator operator = Operator.OR,
    3: optional bool disableCoord = 1
}

struct LindenValue {
}

struct LindenValue {
    1: optional string stringValue,
    2: optional i64 longValue,
    3: optional double doubleValue,
    4: optional list<string> stringValues,
    5: optional list<double> doubleValues,
    6: optional list<i64> longValues,
    7: optional map<LindenValue, LindenValue> mapValue,
}

struct LindenInputParam {
    1: required string name,
    2: optional LindenValue value,
}

struct LindenFilter {
}

struct LindenFilteredQuery {
    1: required LindenQuery lindenQuery,
    2: required LindenFilter lindenFilter,
}

struct LindenWildcardQuery {
    1: required string field,
    2: required string query,
}

struct LindenMatchAllQuery {
}

struct LindenScoreModel {
    1: required string name,
    2: optional string func,
    3: optional list<LindenInputParam> params,
    4: optional Coordinate coordinate,
    6: optional bool override = 0,
    7: optional bool plugin = 0,
}

struct LindenFlexibleQuery {
    1: required list<LindenSearchField> fields,
    2: required string query,
    3: required LindenScoreModel model,
    4: optional bool fullMatch = 0,
    5: optional bool globalIDF = 0,
    6: optional list<LindenSearchField> globalFields
    7: optional double matchRatio,
}

struct LindenQuery {
    1: required double boost = 1,
    2: optional LindenScoreModel scoreModel,
    3: optional LindenBooleanQuery booleanQuery,
    4: optional LindenTermQuery termQuery,
    5: optional LindenDisMaxQuery disMaxQuery,
    6: optional LindenQueryStringQuery queryString,
    7: optional LindenFlexibleQuery flexQuery,
    8: optional LindenRangeQuery rangeQuery,
    9: optional LindenMatchAllQuery matchAllQuery,
    10: optional LindenFilteredQuery filteredQuery,
    11: optional LindenWildcardQuery wildcardQuery,
}

######################################################################
#                   Query Description End
######################################################################

######################################################################
#                   Filter Description Begin
######################################################################

struct LindenQueryFilter {
    1: required LindenQuery query,
}

struct LindenBooleanSubFilter {
    1: optional LindenBooleanClause clause = LindenBooleanClause.SHOULD,
    2: optional LindenFilter filter,
}

struct LindenBooleanFilter {
    1: required list<LindenBooleanSubFilter> filters,
}

struct LindenTermFilter {
    1: required LindenTerm term
}

struct LindenRangeFilter {
    1: required LindenRange range
}

struct LindenSpatialFilter {
    1: required SpatialParam spatialParam
}

struct LindenNotNullFieldFilter {
    1: required string field,
    2: required bool reverse,
}

struct LindenFilter {
    1: optional LindenQueryFilter queryFilter,
    2: optional LindenBooleanFilter booleanFilter,
    3: optional LindenTermFilter termFilter,
    4: optional LindenRangeFilter rangeFilter,
    5: optional LindenSpatialFilter spatialFilter,
    6: optional LindenNotNullFieldFilter notNullFieldFilter,
}

######################################################################
#                   Filter Description End
######################################################################

######################################################################
#                   Sort Description Begin
######################################################################

enum LindenSortType {
    STRING = 0,
    INTEGER,
    LONG,
    DOUBLE,
    FLOAT,
    SCORE,
    DISTANCE,
}

struct LindenSortField {
    1: required string name,
    2: optional bool reverse = 1,
    3: required LindenSortType type,
}

struct LindenSort {
    1: list<LindenSortField> fields,
}
######################################################################
#                   Sort Description End
######################################################################

struct SnippetField {
    1: required string field,
}

struct SnippetParam {
    1: required list<SnippetField> fields,
}

struct EarlyParam {
    1: required i32 maxNum,
}

struct GroupParam {
    1: required string groupField,
    2: optional i32 groupInnerLimit = 1,
    3: optional bool includeMaxScore = 1,
    4: optional bool cacheScores = 1,
    5: optional double maxCacheRAMMB = 8.0
}

struct Bucket {
    1: required string startValue,
    2: required string endValue,
    3: required bool startClosed,
    4: required bool endClosed,
}

struct Aggregation {
    1: required string field,
    2: required LindenCommon.LindenType type,
    3: required list<Bucket> buckets,
}

struct ShardRouteParam {
    1: required i32 shardId,
    2: optional EarlyParam earlyParam,
}

struct SearchRouteParam {
    1: optional list<ShardRouteParam> shardParams,
    2: optional string replicaRouteKey,
}

enum FacetDrillingType {
    DRILLDOWN = 0,
    DRILLSIDEWAYS,
}

struct LindenFacetDimAndPath {
    1: required string dim,
    2: optional string path,
}

struct LindenFacetParam {
    1: required LindenFacetDimAndPath facetDimAndPath,
    2: required i32 topN = 10,
}

struct LindenFacet {
    1: required FacetDrillingType facetDrillingType = FacetDrillingType.DRILLDOWN,
    2: optional list<LindenFacetDimAndPath> drillDownDimAndPaths,
    3: optional list<LindenFacetParam> facetParams,
    4: optional list<Aggregation> aggregations,
}

struct LindenSearchRequest {
    1: optional LindenQuery query,
    2: optional LindenFilter filter,
    3: optional LindenSort sort,
    4: required i32 offset = 0,
    5: required i32 length = 10,
    6: required bool explain = 0,
    7: required bool source = 0,
    8: optional SpatialParam spatialParam,
    9: optional SnippetParam snippetParam,
    10: optional EarlyParam earlyParam,
    11: optional SearchRouteParam routeParam,
    12: optional LindenFacet facet,
    14: optional list<string> sourceFields,
    15: optional list<string> indexNames,
    16: optional GroupParam groupParam,
}

struct LindenDeleteRequest {
    1: required LindenQuery query,
    2: optional SearchRouteParam routeParam,
    3: optional list<string> indexNames,
}

struct LindenRequest {
    1: optional LindenSearchRequest searchRequest,
    2: optional LindenDeleteRequest deleteRequest
}

struct LindenField {
    1: required LindenCommon.LindenFieldSchema schema,
    2: required string value,
}

struct LindenDocument {
    1: required string id,
    2: required list<LindenField> fields,
    3: optional Coordinate coordinate,
}

struct IndexRouteParam {
    1: required set<i32> shardIds,
}

enum IndexRequestType {
    INDEX = 0,
    DELETE,
    UPDATE,
    REPLACE,
    DELETE_INDEX,
    SWAP_INDEX,
}

struct LindenIndexRequest {
    1: required IndexRequestType type,
    2: required string id,
    3: optional IndexRouteParam routeParam,
    4: optional LindenDocument doc,
    5: optional string indexName,
}