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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;

import java.io.IOException;

/**
 * Created by yozhao on 2/16/15.
 */
public class LindenDocsCollector extends Collector {
  private Collector hitCollector;
  public Collector wrappedCollector;

  public LindenDocsCollector(Collector collector) {
    if (!(collector instanceof TopDocsCollector) && !(collector instanceof EarlyTerminationCollector)) {
      throw new RuntimeException("Unsupported collector class in LindenDocsCollector: " + collector.getClass().getName());
    }
    hitCollector = collector;
    wrappedCollector = collector;
  }

  public void wrap(Collector collector) {
    wrappedCollector = MultiCollector.wrap(collector, wrappedCollector);
  }

  public TopDocs topDocs() {
    if (hitCollector instanceof TopDocsCollector) {
      return ((TopDocsCollector) hitCollector).topDocs();
    }
    return ((EarlyTerminationCollector) hitCollector).topDocs();
  }

  @Override public void setScorer(Scorer scorer) throws IOException {
    wrappedCollector.setScorer(scorer);
  }

  @Override public void collect(int doc) throws IOException {
    wrappedCollector.collect(doc);
  }

  @Override public void setNextReader(AtomicReaderContext context) throws IOException {
    wrappedCollector.setNextReader(context);
  }

  @Override public boolean acceptsDocsOutOfOrder() {
    return wrappedCollector.acceptsDocsOutOfOrder();
  }
}
