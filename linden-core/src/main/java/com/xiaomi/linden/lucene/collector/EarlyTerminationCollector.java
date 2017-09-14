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
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;

public class EarlyTerminationCollector extends Collector {

  // docs to collect per segment reader
  private final int numDocsToCollectPerSegment;
  private int numCollected = 0;
  private final TopDocsCollector collector;

  public EarlyTerminationCollector(TopDocsCollector collector, int numDocsToCollectPerSegment) {
    if (numDocsToCollectPerSegment <= 0) {
      throw new IllegalStateException(
          "numDocsToCollectPerSegment must always be > 0, got " + numDocsToCollectPerSegment);
    }
    this.collector = collector;
    this.numDocsToCollectPerSegment = numDocsToCollectPerSegment;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    collector.setScorer(scorer);
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    collector.setNextReader(context);
    numCollected = 0;
  }

  @Override
  public void collect(int doc) throws IOException {
    collector.collect(doc);
    if (++numCollected >= numDocsToCollectPerSegment) {
      throw new CollectionTerminatedException();
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return collector.acceptsDocsOutOfOrder();
  }

  public TopDocs topDocs() {
    return collector.topDocs();
  }
}
