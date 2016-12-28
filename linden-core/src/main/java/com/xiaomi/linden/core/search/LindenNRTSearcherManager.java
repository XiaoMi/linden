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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.linden.core.LindenConfig;

public class LindenNRTSearcherManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(LindenNRTSearcherManager.class);
  private static final int DEFAULT_THREAD_POOL_SIZE = 8;
  private final ReferenceManager<IndexSearcher> indexSearcherReferenceManager;
  private final ControlledRealTimeReopenThread<IndexSearcher> indexSearcherReopenThread;
  private final ReferenceManager<SearcherTaxonomyManager.SearcherAndTaxonomy> searcherAndTaxonomyReferenceManager;
  private final ControlledRealTimeReopenThread<SearcherTaxonomyManager.SearcherAndTaxonomy> searcherAndTaxonomyReopenThread;
  private ExecutorService executor;


  public LindenNRTSearcherManager(LindenConfig config,
      TrackingIndexWriter trackingIndexWriter,
      DirectoryTaxonomyWriter taxonomyWriter) throws IOException {
    SearcherFactory searcherFactory = null;
    if (config.isEnableParallelSearch()) {
      executor = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
      searcherFactory = new ParallelSearcherFactory(executor);
    }
    LOGGER.info("Parallel search enabled : {}", config.isEnableParallelSearch());

    if (taxonomyWriter != null) {
      indexSearcherReferenceManager = null;
      indexSearcherReopenThread = null;
      searcherAndTaxonomyReferenceManager = new SearcherTaxonomyManager(
          trackingIndexWriter.getIndexWriter(), true, searcherFactory, taxonomyWriter);
      searcherAndTaxonomyReopenThread =
          new ControlledRealTimeReopenThread<>(trackingIndexWriter,
                                               searcherAndTaxonomyReferenceManager,
                                               config.getIndexRefreshTime(),   // when there is nobody waiting
                                               0.1);    // when there is someone waiting
      searcherAndTaxonomyReopenThread.start();
    } else {
      indexSearcherReferenceManager = new SearcherManager(trackingIndexWriter.getIndexWriter(),
          true, searcherFactory);
      indexSearcherReopenThread =
          new ControlledRealTimeReopenThread<>(trackingIndexWriter,
              indexSearcherReferenceManager,
              config.getIndexRefreshTime(),   // when there is nobody waiting
              0.1);    // when there is someone waiting
      searcherAndTaxonomyReferenceManager = null;
      searcherAndTaxonomyReopenThread = null;
      indexSearcherReopenThread.start();
    }
  }

  public final SearcherTaxonomyManager.SearcherAndTaxonomy acquire() throws IOException {
    if (searcherAndTaxonomyReferenceManager != null) {
      return searcherAndTaxonomyReferenceManager.acquire();
    }
    return new SearcherTaxonomyManager.SearcherAndTaxonomy(indexSearcherReferenceManager.acquire(), null);
  }

  public final void release(SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy) throws IOException {
    if (searcherAndTaxonomyReferenceManager != null) {
      searcherAndTaxonomyReferenceManager.release(searcherAndTaxonomy);
      return;
    }
    indexSearcherReferenceManager.release(searcherAndTaxonomy.searcher);
  }

  public final boolean maybeRefresh() throws IOException {
    if (searcherAndTaxonomyReferenceManager != null) {
      return searcherAndTaxonomyReferenceManager.maybeRefresh();
    }
    return indexSearcherReferenceManager.maybeRefresh();
  }

  public void close() throws IOException {
    if (searcherAndTaxonomyReferenceManager != null) {
      searcherAndTaxonomyReopenThread.close();
      searcherAndTaxonomyReferenceManager.close();
    } else {
      indexSearcherReopenThread.close();
      indexSearcherReferenceManager.close();
    }
    executor.shutdown();
  }
}
