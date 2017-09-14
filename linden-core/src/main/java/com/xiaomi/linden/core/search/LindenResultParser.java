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

package com.xiaomi.linden.core.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.spatial4j.core.distance.DistanceUtils;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.DoubleRangeFacetCounts;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.LongRangeFacetCounts;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;

import com.xiaomi.linden.common.schema.LindenSchemaConf;
import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.LindenUtil;
import com.xiaomi.linden.lucene.collector.EarlyTerminationCollector;
import com.xiaomi.linden.thrift.common.Aggregation;
import com.xiaomi.linden.thrift.common.AggregationResult;
import com.xiaomi.linden.thrift.common.Bucket;
import com.xiaomi.linden.thrift.common.LindenExplanation;
import com.xiaomi.linden.thrift.common.LindenFacetParam;
import com.xiaomi.linden.thrift.common.LindenFacetResult;
import com.xiaomi.linden.thrift.common.LindenHit;
import com.xiaomi.linden.thrift.common.LindenLabelAndValue;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenSortField;
import com.xiaomi.linden.thrift.common.LindenSortType;
import com.xiaomi.linden.thrift.common.LindenType;
import com.xiaomi.linden.thrift.common.QueryInfo;
import com.xiaomi.linden.thrift.common.SnippetParam;

public class LindenResultParser {

  private final LindenConfig config;
  private final LindenSearchRequest request;
  private final IndexSearcher indexSearcher;
  private final LindenSnippetGenerator snippetGenerator;
  private final Query query;
  private final Filter filter;
  private final Sort sort;
  private final int sortScoreFieldPos;
  private final List<AtomicReaderContext> leaves;

  public LindenResultParser(LindenConfig config, LindenSearchRequest request,
                            IndexSearcher indexSearcher, LindenSnippetGenerator snippetGenerator, Query query,
                            Filter filter, Sort sort) {
    this.config = config;
    this.request = request;
    this.indexSearcher = indexSearcher;
    this.snippetGenerator = snippetGenerator;
    this.query = query;
    this.filter = filter;
    this.sort = sort;
    this.sortScoreFieldPos = getSortScoreFieldPos(sort);
    this.leaves = indexSearcher.getIndexReader().leaves();
  }

  private int getSortScoreFieldPos(Sort sort) {
    int sortScoreField = -1;
    if (sort != null) {
      for (int i = 0; i < sort.getSort().length; ++i) {
        if (sort.getSort()[i].getType() == SortField.Type.SCORE) {
          sortScoreField = i;
          break;
        }
      }
    }
    return sortScoreField;
  }

  //parse ScoreDoc to LindenHit
  private List<LindenHit> parseLindenHits(ScoreDoc[] hits) throws IOException {
    List<LindenHit> lindenHits = new ArrayList<>();
    String idFieldName = config.getSchema().getId();
    for (ScoreDoc hit : hits) {
      LindenHit lindenHit = new LindenHit();
      if (Double.isNaN(hit.score)) {
        // get score for cluster result merge
        if (sortScoreFieldPos != -1) {
          lindenHit.setScore(Double.valueOf(((FieldDoc) hit).fields[sortScoreFieldPos].toString()));
        } else {
          lindenHit.setScore(1);
        }
      } else {
        lindenHit.setScore(hit.score);
      }
      String id = LindenUtil.getFieldStringValue(leaves, hit.doc, idFieldName);
      lindenHit.setId(id);
      lindenHit = this.parseSpatial(hit.doc, lindenHit);
      lindenHit = this.parseSort(hit, lindenHit);
      lindenHit = this.parseSource(hit.doc, lindenHit);
      lindenHit = this.parseExplain(hit.doc, lindenHit);
      lindenHits.add(lindenHit);
    }
    lindenHits = this.parseSnippets(lindenHits, hits);
    return lindenHits;
  }

  private LindenHit parseSpatial(int doc, LindenHit lindenHit) throws IOException {
    if (request.isSetSpatialParam()) {
      Double lat = LindenUtil.getFieldDoubleValue(leaves, doc, LindenSchemaConf.LATITUDE);
      Double lon = LindenUtil.getFieldDoubleValue(leaves, doc, LindenSchemaConf.LONGITUDE);
      if (lat != 0.0 && lon != 0.0) {
        double distance = DistanceUtils.distLawOfCosinesRAD(
            DistanceUtils.toRadians(lat),
            DistanceUtils.toRadians(lon),
            DistanceUtils.toRadians(request.getSpatialParam().getCoordinate().getLatitude()),
            DistanceUtils.toRadians(request.getSpatialParam().getCoordinate().getLongitude()));
        distance = DistanceUtils.radians2Dist(distance, DistanceUtils.EARTH_MEAN_RADIUS_KM);
        lindenHit.setDistance(distance);
      }
    }
    return lindenHit;
  }

  private List<LindenHit> parseSnippets(List<LindenHit> lindenHits, ScoreDoc[] hits) throws IOException {
    if (request.isSetSnippetParam()) {
      SnippetParam param = request.getSnippetParam();
      if (hits != null) {
        lindenHits = snippetGenerator.generateSnippet(lindenHits, param, query, indexSearcher, hits);
      }
    }
    return lindenHits;
  }

  private LindenHit parseSource(int doc, LindenHit lindenHit) throws IOException {
    if (request.isSource()) {
      String source = LindenUtil.getSource(indexSearcher, doc, lindenHit.getId(), request.getSourceFields(), config);
      lindenHit.setSource(source);
    }
    return lindenHit;
  }

  private LindenHit parseExplain(int doc, LindenHit lindenHit) throws IOException {
    if (request.isExplain()) {
      Explanation expl = indexSearcher.explain(query, doc);
      LindenExplanation lindenExpl = parseLindenExplanation(expl);
      lindenHit.setExplanation(lindenExpl);
    }
    return lindenHit;
  }

  private LindenHit parseSort(ScoreDoc hit, LindenHit lindenHit) {
    if (request.isSetSort()) {
      Map<String, String> fieldMap = new HashMap<>();
      for (int i = 0; i < request.getSort().getFields().size(); ++i) {
        LindenSortField field = request.getSort().getFields().get(i);
        if (field.type == LindenSortType.SCORE || field.type == LindenSortType.DISTANCE) {
          continue;
        }
        Object value = ((FieldDoc) hit).fields[i];
        if (value == null) {
          continue;
        }
        if (field.type == LindenSortType.STRING) {
          fieldMap.put(field.getName(), ((BytesRef) value).utf8ToString());
        } else {
          fieldMap.put(field.getName(), value.toString());
        }
      }
      lindenHit.setFields(fieldMap);
    }
    return lindenHit;
  }

  private LindenExplanation parseLindenExplanation(Explanation expl) {
    LindenExplanation lindenExpl = new LindenExplanation();
    lindenExpl.setDescription(expl.getDescription());
    lindenExpl.setValue(expl.getValue());
    if (expl.getDetails() != null) {
      for (Explanation subExpl : expl.getDetails()) {
        LindenExplanation subLindenExpl = parseLindenExplanation(subExpl);
        lindenExpl.addToDetails(subLindenExpl);
      }
    }
    return lindenExpl;
  }


  public LindenResult parse(TopDocs topDocs, TopGroups<TopDocs> topGroupedDocs,
                            Facets facets, FacetsCollector facetsCollector) throws IOException {
    LindenResult result = new LindenResult();
    List<LindenHit> lindenHits;
    int totalHits = 0;
    if (topDocs != null) {
      totalHits = topDocs.totalHits;
      lindenHits = parseLindenHits(topDocs.scoreDocs);
    } else if (topGroupedDocs != null) {
      lindenHits = new ArrayList<>();
      totalHits = topGroupedDocs.totalHitCount;
      for (GroupDocs<TopDocs> group : topGroupedDocs.groups) {
        List<LindenHit> groupHits = parseLindenHits(group.scoreDocs);
        LindenHit hitGroup = new LindenHit(groupHits.get(0)).setGroupHits(groupHits);
        String groupField = request.getGroupParam().getGroupField();
        String groupValue = LindenUtil.getFieldStringValue(leaves, group.scoreDocs[0].doc, groupField);
        if (!hitGroup.isSetFields()) {
          hitGroup.setFields(new HashMap<String, String>());
        }
        hitGroup.getFields().put(groupField, groupValue);
        lindenHits.add(hitGroup);
      }
      int groupTotal = topGroupedDocs.totalGroupCount == null ? 0 : topGroupedDocs.totalGroupCount;
      result.setTotalGroups(groupTotal);
      result.setTotalGroupHits(topGroupedDocs.totalGroupedHitCount);
    } else {
      lindenHits = new ArrayList<>();
    }
    result.setTotalHits(totalHits);
    result.setHits(lindenHits);
    parseFacets(result, facets, facetsCollector);
    result.setQueryInfo(new QueryInfo().setQuery(query.toString()));
    if (filter != null) {
      result.getQueryInfo().setFilter(filter.toString());
    }
    if (sort != null) {
      result.getQueryInfo().setSort(sort.toString());
    }
    return result;
  }

  private void parseFacets(LindenResult result, Facets facets, FacetsCollector facetsCollector) throws IOException {
    // Set facets
    if (request.isSetFacet()) {
      if (request.getFacet().isSetFacetParams() && facets != null) {
        List<LindenFacetParam> facetParams = request.getFacet().getFacetParams();
        for (LindenFacetParam facetParam : facetParams) {
          LindenFacetResult lindenFacetResult = new LindenFacetResult();
          lindenFacetResult.setDim(facetParam.facetDimAndPath.dim);
          lindenFacetResult.setPath(facetParam.facetDimAndPath.path);
          FacetResult facetResult;
          if (facetParam.facetDimAndPath.path != null) {
            facetResult = facets.getTopChildren(facetParam.topN, facetParam.facetDimAndPath.dim,
                                                facetParam.facetDimAndPath.path.split("/"));
          } else {
            facetResult = facets.getTopChildren(facetParam.topN, facetParam.facetDimAndPath.dim);
          }
          if (facetResult != null) {
            lindenFacetResult
                .setValue(facetResult.value.intValue());
            lindenFacetResult.setChildCount(facetResult.childCount);
            int sumValue = 0;
            for (int j = 0; j < facetResult.labelValues.length; ++j) {
              LindenLabelAndValue labelAndValue = new LindenLabelAndValue();
              labelAndValue.setLabel(facetResult.labelValues[j].label);
              int value = facetResult.labelValues[j].value.intValue();
              labelAndValue.setValue(value);
              sumValue += value;
              lindenFacetResult.addToLabelValues(labelAndValue);
            }
            if (sumValue > lindenFacetResult.getValue() || facetResult.labelValues.length < facetParam.topN) {
              lindenFacetResult.setValue(sumValue);
            }
          }
          result.addToFacetResults(lindenFacetResult);
        }
      } else if (request.getFacet().isSetAggregations() && facetsCollector != null) {
        List<Aggregation> aggregations = request.getFacet().getAggregations();
        for (int i = 0; i < aggregations.size(); ++i) {
          Aggregation aggregation = aggregations.get(i);
          String fieldName = aggregation.getField();
          LindenType type = aggregation.getType();
          AggregationResult aggregationResult = new AggregationResult();
          aggregationResult.setField(fieldName);
          Facets aggFacets;
          if (type == LindenType.INTEGER || type == LindenType.LONG) {
            LongRange[] ranges = new LongRange[aggregation.getBucketsSize()];
            for (int j = 0; j < ranges.length; ++j) {
              Bucket bucket = aggregation.getBuckets().get(j);
              String label = generateBucketLabel(bucket);
              long minValue = bucket.getStartValue().equals("*") ?
                              Long.MIN_VALUE :
                              Long.parseLong(bucket.getStartValue());
              long maxValue = bucket.getEndValue().equals("*") ?
                              Long.MAX_VALUE :
                              Long.parseLong(bucket.getEndValue());
              ranges[j] = new LongRange(label, minValue, bucket.isStartClosed(), maxValue, bucket.isEndClosed());
            }
            aggFacets = new LongRangeFacetCounts(fieldName, facetsCollector, ranges);
          } else if (type == LindenType.DOUBLE) {
            DoubleRange[] ranges = new DoubleRange[aggregation.getBucketsSize()];
            for (int j = 0; j < ranges.length; ++j) {
              Bucket bucket = aggregation.getBuckets().get(j);
              String label = generateBucketLabel(bucket);
              double minValue = bucket.getStartValue().equals("*") ?
                                -Double.MAX_VALUE :
                                Double.parseDouble(bucket.getStartValue());
              double maxValue = bucket.getEndValue().equals("*") ?
                                Double.MAX_VALUE :
                                Double.parseDouble(bucket.getEndValue());
              ranges[j] = new DoubleRange(label, minValue, bucket.isStartClosed(), maxValue, bucket.isEndClosed());
            }
            aggFacets = new DoubleRangeFacetCounts(fieldName, facetsCollector, ranges);
          } else {
            throw new IOException(type + " type is not supported in aggregation");
          }
          FacetResult facetResult = aggFacets.getTopChildren(aggregation.getBucketsSize(), fieldName);
          for (int j = 0; j < facetResult.labelValues.length; ++j) {
            LindenLabelAndValue labelAndValue = new LindenLabelAndValue();
            labelAndValue.setLabel(facetResult.labelValues[j].label);
            labelAndValue.setValue(facetResult.labelValues[j].value.intValue());
            aggregationResult.addToLabelValues(labelAndValue);
          }
          result.addToAggregationResults(aggregationResult);
        }
      }
    }
  }

  private String generateBucketLabel(Bucket bucket) {
    StringBuilder builder = new StringBuilder();
    if (bucket.isStartClosed()) {
      builder.append('[');
    } else {
      builder.append('{');
    }
    builder.append(bucket.getStartValue());
    builder.append(',');
    builder.append(bucket.getEndValue());
    if (bucket.isEndClosed()) {
      builder.append(']');
    } else {
      builder.append('}');
    }
    return builder.toString();
  }
}
