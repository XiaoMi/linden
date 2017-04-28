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

package com.xiaomi.linden.cluster;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.thrift.common.AggregationResult;
import com.xiaomi.linden.thrift.common.LindenFacetResult;
import com.xiaomi.linden.thrift.common.LindenHit;
import com.xiaomi.linden.thrift.common.LindenLabelAndValue;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenSortField;
import com.xiaomi.linden.thrift.common.QueryInfo;

public class ResultMerger {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResultMerger.class);
  private static final LindenResult EMPTY_RESULT = new LindenResult().setSuccess(false).
      setError("Linden result list is empty in ResultMerger").setHits(new ArrayList<LindenHit>())
      .setCost(0).setQueryInfo(new QueryInfo("error")).setTotalHits(0);

  private static final LindenResult ALL_SHARDS_FAILED_RESULT = new LindenResult().setSuccess(false).
      setError("All shards failed").setHits(new ArrayList<LindenHit>())
      .setCost(0).setQueryInfo(new QueryInfo("error")).setTotalHits(0);

  public static LindenResult merge(final LindenSearchRequest lindenRequest, final List<LindenResult> resultList) {
    if (resultList == null || resultList.isEmpty()) {
      return EMPTY_RESULT;
    }

    Iterator<LindenResult> iterator = resultList.iterator();
    int failureCount = 0;
    LindenResult failedResult = null;
    while (iterator.hasNext()) {
      LindenResult result = iterator.next();
      if (!result.isSuccess()) {
        failureCount++;
        failedResult = result;
        iterator.remove();
      }
    }
    if (resultList.isEmpty()) {
      if (failureCount == 1) {
        LOGGER.error("The shard failed for search request {}", lindenRequest.toString());
        return failedResult;
      }
      LOGGER.error("All shards failed for search request {}", lindenRequest.toString());
      return ALL_SHARDS_FAILED_RESULT;
    }

    LindenResult mergedResult;
    if (lindenRequest.isSetGroupParam()) {
      //merge results for grouping search
      mergedResult = mergeGroupSearch(lindenRequest, resultList);
    } else {
      //merge results for normal searching
      mergedResult = new LindenResult().setTotalHits(0).setQueryInfo(resultList.get(0).queryInfo);
      List<List<LindenHit>> hits = new ArrayList<>();
      for (LindenResult result : resultList) {
        mergedResult.totalHits += result.getTotalHits();
        hits.add(result.getHits());
      }
      //merge LindenHit
      List<LindenSortField> sortFields = lindenRequest.isSetSort() ? lindenRequest.getSort().getFields() : null;
      Iterable<LindenHit> mergedHits = Iterables.mergeSorted(hits, new LindenHitCmp(sortFields));
      List<LindenHit> topNHits = Lists.newArrayList(mergedHits);
      if (lindenRequest.getOffset() <= topNHits.size()) {
        List<LindenHit>
            subHits =
            topNHits.subList(lindenRequest.getOffset(),
                             Math.min(lindenRequest.getOffset() + lindenRequest.getLength(), topNHits.size()));
        mergedResult.setHits(subHits);
      } else {
        mergedResult.setHits(new ArrayList<LindenHit>());
      }
    }

    // Merge facet result
    if (lindenRequest.isSetFacet()) {
      mergeFacet(lindenRequest, resultList, mergedResult);
    }

    if (failureCount > 0) {
      mergedResult.setError(failureCount + " shards failed.");
    }
    return mergedResult;
  }

  private static LindenResult mergeGroupSearch(LindenSearchRequest lindenRequest, List<LindenResult> resultList) {
    LindenResult mergedResult = resultList.get(0);
    if (!mergedResult.isSetHits()) {
      mergedResult.setHits(new ArrayList<LindenHit>());
    }
    String groupField = lindenRequest.getGroupParam().getGroupField();
    int innerLimit = lindenRequest.getGroupParam().getGroupInnerLimit();
    //traverse LindenResults from shards
    for (int i = 1; i < resultList.size(); ++i) {
      LindenResult subResult = resultList.get(i);
      if (!subResult.isSetHits()) {
        continue;
      }
      mergedResult.totalHits += subResult.totalHits;
      mergedResult.totalGroups = Math.max(mergedResult.totalGroups, subResult.totalGroups);
      mergedResult.totalGroupHits += subResult.totalGroupHits;
      //traverse groups in waiting LindenResult
      for (LindenHit subGroup : subResult.getHits()) {
        String groupName = subGroup.getFields().get(groupField);
        boolean isFound = false;
        //find the group in the merged groupList
        for (LindenHit mergedHit : mergedResult.getHits()) {
          if (mergedHit.getFields().get(groupField).equals(groupName)) {
            Iterable<LindenHit> groupIterable = Iterables.mergeSorted(
                ImmutableList.of(subGroup.getGroupHits(), mergedHit.getGroupHits()), new LindenHitCmp(null));
            mergedHit.setGroupHits(Lists.newArrayList(Iterables.limit(groupIterable, innerLimit)));
            mergedHit.setScore(Math.max(mergedHit.score, subGroup.score));
            isFound = true;
            break;
          }
        }
        if (!isFound) {
          mergedResult.getHits().add(subGroup);
        }
      }
    }
    //sort the group by score
    Ordering<LindenHit> ordering = new Ordering<LindenHit>() {
      @Override
      public int compare(@Nullable LindenHit left, @Nullable LindenHit right) {
        return Double.compare(left.getScore(), right.getScore());
      }
    };
    List<LindenHit>
        orderedHits =
        ordering.greatestOf(mergedResult.getHits(), mergedResult.getHitsSize());    //offset -> offset+size groups
    int from = lindenRequest.getOffset();
    int size = lindenRequest.getLength();
    if (from < orderedHits.size()) {
      List<LindenHit> subHits = orderedHits.subList(from, Math.min(from + size, orderedHits.size()));
      mergedResult.setHits(subHits);
    } else {
      mergedResult.setHits(new ArrayList<LindenHit>());
    }
    return mergedResult;
  }

  private static void mergeFacet(final LindenSearchRequest lindenRequest, final List<LindenResult> resultList,
                                 LindenResult mergedResult) {
    if (resultList.size() == 1) {
      mergedResult.setFacetResults(resultList.get(0).getFacetResults());
      mergedResult.setAggregationResults(resultList.get(0).getAggregationResults());
      return;
    }
    Ordering<LindenLabelAndValue> labelAndValueOrdering = new Ordering<LindenLabelAndValue>() {
      @Override
      public int compare(@Nullable LindenLabelAndValue lv1, @Nullable LindenLabelAndValue lv2) {
        int cmp = Integer.compare(lv1.getValue(), lv2.getValue());
        if (cmp != 0) {
          return cmp;
        }
        return lv2.getLabel().compareTo(lv1.getLabel());
      }
    };

    // merge browse result
    for (int i = 0; i < lindenRequest.getFacet().getFacetParamsSize(); ++i) {
      LindenFacetResult mergedFacetResult = new LindenFacetResult();
      mergedFacetResult.setDim(lindenRequest.getFacet().getFacetParams().get(i).facetDimAndPath.dim);
      mergedFacetResult.setPath(lindenRequest.getFacet().getFacetParams().get(i).facetDimAndPath.path);
      Map<String, Integer> labelValueMap = new HashMap<>();
      for (int j = 0; j < resultList.size(); ++j) {
        if (resultList.get(j).getFacetResults() == null) {
          continue;
        }
        LindenFacetResult lindenFacetResult = resultList.get(j).getFacetResults().get(i);
        if (lindenFacetResult == null) {
          continue;
        }
        mergedFacetResult.setValue(mergedFacetResult.getValue() + lindenFacetResult.getValue());
        if (lindenFacetResult.getChildCount() > mergedFacetResult.getChildCount()) {
          mergedFacetResult.setChildCount(lindenFacetResult.getChildCount());
        }
        for (int k = 0; k < lindenFacetResult.getLabelValuesSize(); ++k) {
          String label = lindenFacetResult.getLabelValues().get(k).getLabel();
          int previous = labelValueMap.containsKey(label) ? labelValueMap.get(label) : 0;
          labelValueMap.put(label, previous + lindenFacetResult.getLabelValues().get(k).getValue());
        }
      }
      if (labelValueMap.size() > 0) {
        List<LindenLabelAndValue> labelAndValues = new ArrayList<>();
        Set<Map.Entry<String, Integer>> entrySet = labelValueMap.entrySet();
        for (Iterator<Map.Entry<String, Integer>> it = entrySet.iterator(); it.hasNext(); ) {
          Map.Entry<String, Integer> entry = it.next();
          labelAndValues.add(new LindenLabelAndValue(entry.getKey(), entry.getValue()));
        }
        if (labelAndValues.size() > mergedFacetResult.getChildCount()) {
          mergedFacetResult.setChildCount(labelAndValues.size());
        }
        List<LindenLabelAndValue> topLabelAndValues = labelAndValueOrdering.greatestOf(
            labelAndValues, lindenRequest.getFacet().getFacetParams().get(i).getTopN());
        mergedFacetResult.setLabelValues(topLabelAndValues);
      }
      mergedResult.addToFacetResults(mergedFacetResult);
    }

    // merge aggregation result
    for (int i = 0; i < lindenRequest.getFacet().getAggregationsSize(); ++i) {
      AggregationResult mergedAggregationResult = new AggregationResult();
      mergedAggregationResult.setField(
          lindenRequest.getFacet().getAggregations().get(i).getField());
      List<LindenLabelAndValue> lindenLabelAndValues = new ArrayList<>();
      for (int j = 0;
           j < lindenRequest.getFacet().getAggregations().get(i).getBucketsSize(); ++j) {
        LindenLabelAndValue lindenLabelAndValue = new LindenLabelAndValue();
        lindenLabelAndValue.setLabel(
            resultList.get(0).getAggregationResults().get(i).getLabelValues().get(j)
                .getLabel());
        int value = 0;
        for (int k = 0; k < resultList.size(); ++k) {
          value += resultList.get(k).getAggregationResults().get(i).getLabelValues().get(j)
              .getValue();
        }
        lindenLabelAndValue.setValue(value);
        lindenLabelAndValues.add(lindenLabelAndValue);
      }
      mergedAggregationResult.setLabelValues(lindenLabelAndValues);
      mergedResult.addToAggregationResults(mergedAggregationResult);
    }
  }
}
