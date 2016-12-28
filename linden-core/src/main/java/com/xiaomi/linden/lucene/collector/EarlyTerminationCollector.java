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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;

import java.io.IOException;

public class EarlyTerminationCollector extends Collector {

  private final int maxDocsToCollect;
  private int collectedNum = 0;
  private final TopDocsCollector collector;
  private int minMatchedDoc = -1;
  private int maxMatchedDoc = -1;
  private int totalDocs = 0;
  private int docBase = 0;

  public EarlyTerminationCollector(TopDocsCollector collector, int maxDocsToCollect) {
    this.collector = collector;
    this.maxDocsToCollect = maxDocsToCollect;
    if (maxDocsToCollect < 1) {
      throw new RuntimeException("maxDocsToCollect < 1");
    }
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    collector.setScorer(scorer);
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    collector.setNextReader(context);
    totalDocs += context.reader().maxDoc();
    docBase = context.docBase;
  }

  @Override
  public void collect(int doc) throws IOException {
    if (collectedNum >= maxDocsToCollect) {
      throw new CollectionTerminatedException();
    }
    collector.collect(doc);
    ++collectedNum;
    if (minMatchedDoc == -1)
      minMatchedDoc = docBase + doc;
    maxMatchedDoc = docBase + doc;
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return collector.acceptsDocsOutOfOrder();
  }

  public TopDocs topDocs() {
    TopDocs topDocs = collector.topDocs();
    if (collectedNum < maxDocsToCollect) {
      topDocs.totalHits = collectedNum;
      return topDocs;
    }
    double ratio = 1.0 * collectedNum / (maxMatchedDoc - minMatchedDoc + 1);
    topDocs.totalHits = (int) (totalDocs * ratio);
    return topDocs;
  }

  public double getEarlyTerminationFactor() {
    if (collectedNum < maxDocsToCollect  || minMatchedDoc == maxMatchedDoc) {
      return 1.0;
    }
    return (double)totalDocs / (maxMatchedDoc - minMatchedDoc + 1);
  }
}
