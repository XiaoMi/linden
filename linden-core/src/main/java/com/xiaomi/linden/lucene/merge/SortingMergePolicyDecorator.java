package com.xiaomi.linden.lucene.merge;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.sorter.SortingMergePolicy;
import org.apache.lucene.search.Sort;

public class SortingMergePolicyDecorator extends MergePolicy {

  private SortingMergePolicy sortingMergePolicy;
  private Sort sort;

  public SortingMergePolicyDecorator(MergePolicy in, Sort sort) {
    this.sortingMergePolicy = new SortingMergePolicy(in, sort);
    this.sort = sort;
  }

  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos,
                                       IndexWriter writer) throws IOException {
    return sortingMergePolicy.findMerges(mergeTrigger, segmentInfos, writer);
  }

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount,
                                             Map<SegmentCommitInfo, Boolean> segmentsToMerge,
                                             IndexWriter writer) throws IOException {
    return sortingMergePolicy.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, writer);
  }

  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, IndexWriter writer)
      throws IOException {
    return sortingMergePolicy.findForcedDeletesMerges(segmentInfos, writer);
  }


  public Sort getSort() {
    return sort;
  }
}
