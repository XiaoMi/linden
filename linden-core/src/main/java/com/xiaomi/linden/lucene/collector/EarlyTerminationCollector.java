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

package com.xiaomi.linden.lucene.collector;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.sorter.SortingMergePolicy;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;

public class EarlyTerminationCollector extends Collector {

  // docs to collect per segment reader
  private final int numDocsToCollectPerSortedSegment;
  private int segmentTotalCollect;
  private int numCollected;
  private final Sort sort;
  private final TopDocsCollector collector;
  private boolean segmentSorted;
  private static CollectionTerminatedException TERMINATED_EXCEPTION = new CollectionTerminatedException();


  public EarlyTerminationCollector(TopDocsCollector collector, Sort sort, int numDocsToCollectPerSortedSegment) {
    if (numDocsToCollectPerSortedSegment <= 0) {
      throw new IllegalStateException(
          "numDocsToCollectPerSortedSegment must always be > 0, got " + numDocsToCollectPerSortedSegment);
    }
    this.collector = collector;
    this.sort = sort;
    this.numDocsToCollectPerSortedSegment = numDocsToCollectPerSortedSegment;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    collector.setScorer(scorer);
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    collector.setNextReader(context);
    if (sort != null) {
      segmentSorted = SortingMergePolicy.isSorted(context.reader(), sort);
      segmentTotalCollect = segmentSorted ? numDocsToCollectPerSortedSegment : 2147483647;
    } else {
      segmentTotalCollect = numDocsToCollectPerSortedSegment;
    }
    numCollected = 0;
  }

  @Override
  public void collect(int doc) throws IOException {
    collector.collect(doc);
    if (++numCollected >= segmentTotalCollect) {
      throw TERMINATED_EXCEPTION;
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return !segmentSorted && collector.acceptsDocsOutOfOrder();
  }

  public TopDocs topDocs() {
    return collector.topDocs();
  }
}
