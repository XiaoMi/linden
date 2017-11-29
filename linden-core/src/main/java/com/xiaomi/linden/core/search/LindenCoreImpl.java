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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.RAMDirectory;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.LindenDocumentBuilder;
import com.xiaomi.linden.core.LindenUtil;
import com.xiaomi.linden.core.RuntimeInfoUtils;
import com.xiaomi.linden.core.search.query.QueryConstructor;
import com.xiaomi.linden.core.search.query.filter.FilterConstructor;
import com.xiaomi.linden.core.search.query.sort.SortConstructor;
import com.xiaomi.linden.lucene.collector.EarlyTerminationCollector;
import com.xiaomi.linden.lucene.collector.LindenDocsCollector;
import com.xiaomi.linden.lucene.merge.SortingMergePolicyDecorator;
import com.xiaomi.linden.thrift.common.FacetDrillingType;
import com.xiaomi.linden.thrift.common.FileDiskUsageInfo;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenDocument;
import com.xiaomi.linden.thrift.common.LindenFacet;
import com.xiaomi.linden.thrift.common.LindenFacetDimAndPath;
import com.xiaomi.linden.thrift.common.LindenField;
import com.xiaomi.linden.thrift.common.LindenIndexRequest;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenServiceInfo;
import com.xiaomi.linden.thrift.common.Response;
import com.xiaomi.linden.util.ResponseUtils;

public class LindenCoreImpl extends LindenCore {

  private final LindenConfig config;
  private final LindenNRTSearcherManager lindenNRTSearcherManager;
  private final TrackingIndexWriter trackingIndexWriter;
  private final IndexWriter indexWriter;
  private final DirectoryTaxonomyWriter taxoWriter;
  private final CommitStrategy commitStrategy;
  private final LindenSnippetGenerator snippetGenerator;
  private final FacetsConfig facetsConfig;
  private String idFieldName;

  public LindenCoreImpl(LindenConfig lindenConfig) throws IOException {
    this(lindenConfig, null);
  }

  public LindenCoreImpl(LindenConfig lindenConfig, String subIndexDirectory) throws IOException {
    this.config = lindenConfig;
    idFieldName = config.getSchema().getId();
    facetsConfig = config.createFacetsConfig();

    String directory = config.getIndexDirectory();
    if (subIndexDirectory != null) {
      directory = FilenameUtils.concat(config.getIndexDirectory(), subIndexDirectory);
    }

    indexWriter =
        new IndexWriter(createIndexDirectory(directory, config.getIndexType()), config.createIndexWriterConfig());
    trackingIndexWriter = new TrackingIndexWriter(indexWriter);

    taxoWriter =
        facetsConfig != null ? new DirectoryTaxonomyWriter(createTaxoIndexDirectory(directory, config.getIndexType()))
                             : null;
    commitStrategy = new CommitStrategy(indexWriter, taxoWriter);
    commitStrategy.start();

    lindenNRTSearcherManager = new LindenNRTSearcherManager(config,
                                                            trackingIndexWriter, taxoWriter);
    snippetGenerator = new LindenSnippetGenerator();
  }


  private static final double maxMergeSizeMB = 4;
  private static final double maxCachedMB = 48;

  public Directory createIndexDirectory(String directory, LindenConfig.IndexType indexType) throws IOException {
    switch (indexType) {
      case RAM:
        return new RAMDirectory();
      default:
        Preconditions.checkNotNull(directory, "index directory can not be null");
        return new NRTCachingDirectory(FSDirectory.open(new File(directory)), maxMergeSizeMB, maxCachedMB);
    }
  }

  public Directory createTaxoIndexDirectory(String directory, LindenConfig.IndexType indexType) throws IOException {
    switch (indexType) {
      case RAM:
        return new RAMDirectory();
      default:
        Preconditions.checkNotNull(directory, "index directory can not be null");
        return new NRTCachingDirectory(FSDirectory.open(new File(directory + ".taxonomy")),
                                       maxMergeSizeMB, maxCachedMB);
    }
  }

  public LindenResult search(LindenSearchRequest request) throws IOException {
    SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy = lindenNRTSearcherManager.acquire();
    try {
      IndexSearcher indexSearcher = searcherAndTaxonomy.searcher;
      Filter filter = FilterConstructor.constructFilter(request.getFilter(), config);
      Sort sort = SortConstructor.constructSort(request, indexSearcher, config);
      indexSearcher.setSimilarity(config.getSearchSimilarityInstance());

      Query query = QueryConstructor.constructQuery(request.getQuery(), config);
      if (filter != null) {
        query = new FilteredQuery(query, filter);
      }

      int from = request.getOffset();
      int size = request.getLength();
      LindenResultParser resultParser = new LindenResultParser(config, request,
                                                               indexSearcher, snippetGenerator, query, filter, sort);
      // very common search, no group, no facet, no early termination, no search time limit
      if (!request.isSetGroupParam() && !request.isSetFacet()
          && !request.isSetEarlyParam() && config.getSearchTimeLimit() <= 0) {
        TopDocs docs;
        if (sort != null) {
          docs = indexSearcher.search(query, from + size, sort);
        } else {
          docs = indexSearcher.search(query, from + size);
        }
        return resultParser.parse(docs, null, null, null);
      }

      // group param will suppress facet, group, early termination and search time limit parameters
      if (request.isSetGroupParam()) {
        String groupField = request.getGroupParam().getGroupField();
        GroupingSearch groupingSearch = new GroupingSearch(groupField);
        groupingSearch.setGroupDocsLimit(request.getGroupParam().getGroupInnerLimit());
        if (sort != null) {
          groupingSearch.setGroupSort(sort);
          groupingSearch.setSortWithinGroup(sort);
          groupingSearch.setFillSortFields(true);
        }
        groupingSearch.setCachingInMB(8.0, true);
        groupingSearch.setAllGroups(true);
        TopGroups<TopDocs> topGroupedDocs = groupingSearch.search(indexSearcher, query, 0, from + size);
        return resultParser.parse(null, topGroupedDocs, null, null);
      }

      TopDocsCollector topDocsCollector;
      if (sort != null) {
        topDocsCollector = TopFieldCollector.create(sort, from + size, null, true, false, false, false);
      } else {
        topDocsCollector = TopScoreDocCollector.create(from + size, false);
      }

      LindenDocsCollector lindenDocsCollector;
      if (request.isSetEarlyParam()) {
        MergePolicy mergePolicy = indexWriter.getConfig().getMergePolicy();
        Sort mergePolicySort = null;
        if (mergePolicy instanceof SortingMergePolicyDecorator) {
          mergePolicySort = ((SortingMergePolicyDecorator) mergePolicy).getSort();
        }
        EarlyTerminationCollector
            earlyTerminationCollector =
            new EarlyTerminationCollector(topDocsCollector, mergePolicySort, request.getEarlyParam().getMaxNum());
        lindenDocsCollector = new LindenDocsCollector(earlyTerminationCollector);
      } else {
        lindenDocsCollector = new LindenDocsCollector(topDocsCollector);
      }

      Collector collector = lindenDocsCollector;
      if (config.getSearchTimeLimit() > 0) {
        collector = new TimeLimitingCollector(lindenDocsCollector,
                                              TimeLimitingCollector.getGlobalCounter(),
                                              config.getSearchTimeLimit());
      }

      // no facet param
      if (!request.isSetFacet()) {
        indexSearcher.search(query, collector);
        return resultParser.parse(lindenDocsCollector.topDocs(), null, null, null);
      }

      // facet search
      LindenFacet facetRequest = request.getFacet();
      FacetsCollector facetsCollector = new FacetsCollector();
      lindenDocsCollector.wrap(facetsCollector);

      Facets facets = null;
      if (facetRequest.isSetDrillDownDimAndPaths()) {
        // drillDown or drillSideways
        DrillDownQuery drillDownQuery = new DrillDownQuery(facetsConfig, query);
        List<LindenFacetDimAndPath> drillDownDimAndPaths = facetRequest.getDrillDownDimAndPaths();
        for (int i = 0; i < drillDownDimAndPaths.size(); ++i) {
          String fieldName = drillDownDimAndPaths.get(i).dim;
          if (drillDownDimAndPaths.get(i).path != null) {
            drillDownQuery.add(fieldName, drillDownDimAndPaths.get(i).path.split("/"));
          } else {
            drillDownQuery.add(fieldName);
          }
        }

        // drillSideways
        if (facetRequest.getFacetDrillingType() == FacetDrillingType.DRILLSIDEWAYS) {
          DrillSideways dillSideways = new DrillSideways(indexSearcher, facetsConfig,
                                                         searcherAndTaxonomy.taxonomyReader);
          DrillSideways.DrillSidewaysResult drillSidewaysResult = dillSideways.search(drillDownQuery, collector);
          facets = drillSidewaysResult.facets;
        } else {
          // drillDown
          indexSearcher.search(drillDownQuery, collector);
          facets = new FastTaxonomyFacetCounts(searcherAndTaxonomy.taxonomyReader, facetsConfig, facetsCollector);
        }
      } else {
        indexSearcher.search(query, collector);
        // Simple facet browsing
        if (facetRequest.isSetFacetParams()) {
          facets = new FastTaxonomyFacetCounts(searcherAndTaxonomy.taxonomyReader, facetsConfig, facetsCollector);
        }
      }
      return resultParser.parse(lindenDocsCollector.topDocs(), null, facets, facetsCollector);
    } catch (Exception e) {
      throw new IOException(Throwables.getStackTraceAsString(e));
    } finally {
      lindenNRTSearcherManager.release(searcherAndTaxonomy);
    }
  }

  @Override
  public Response delete(LindenDeleteRequest request) throws IOException {
    SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy = lindenNRTSearcherManager.acquire();
    try {
      Query query = QueryConstructor.constructQuery(request.getQuery(), config);
      trackingIndexWriter.deleteDocuments(query);
      return ResponseUtils.SUCCESS;
    } catch (Exception e) {
      return ResponseUtils.buildFailedResponse(Throwables.getStackTraceAsString(e));
    } finally {
      lindenNRTSearcherManager.release(searcherAndTaxonomy);
    }
  }

  @Override
  public void close() throws IOException {
    commitStrategy.close();
    indexWriter.close();
    if (taxoWriter != null) {
      taxoWriter.close();
    }
    lindenNRTSearcherManager.close();
  }

  @Override
  public LindenServiceInfo getServiceInfo() throws IOException {
    int docNum = trackingIndexWriter.getIndexWriter().numDocs();
    List<String> paths = new ArrayList<>();
    if (config.getIndexDirectory() != null) {
      paths.add(config.getIndexDirectory());
    }
    if (config.getLogPath() != null) {
      paths.add(config.getLogPath());
    }
    List<FileDiskUsageInfo> fileDiskUsageInfos = RuntimeInfoUtils.getRuntimeFileInfo(paths);
    return new LindenServiceInfo().setDocsNum(docNum).setJvmInfo(RuntimeInfoUtils.getJVMInfo())
        .setFileUsedInfos(fileDiskUsageInfos);
  }

  @Override
  public Response mergeIndex(int maxNumSegments) throws IOException {
    indexWriter.forceMerge(maxNumSegments);
    return ResponseUtils.SUCCESS;
  }

  // refresh right now
  public void refresh() throws IOException {
    lindenNRTSearcherManager.maybeRefresh();
  }

  @Override
  public Response index(LindenIndexRequest request) throws IOException {
    if (request == null) {
      return ResponseUtils.FAILED;
    }
    switch (request.getType()) {
      case INDEX:
      case REPLACE:
        return index(request.getDoc());
      case DELETE:
        return delete(request.getId());
      case UPDATE:
        return update(request.getDoc());
      default:
        return ResponseUtils.buildFailedResponse("IndexRequestType " + request.getType() + " is not supported.");
    }
  }

  public Response index(LindenDocument lindenDoc) throws IOException {
    Document doc = LindenDocParser.parse(lindenDoc, config);
    if (doc != null) {
      if (facetsConfig != null) {
        trackingIndexWriter.updateDocument(new Term(idFieldName, lindenDoc.getId()),
                                           facetsConfig.build(taxoWriter, doc));
      } else {
        trackingIndexWriter.updateDocument(new Term(idFieldName, lindenDoc.getId()), doc);
      }
      return ResponseUtils.SUCCESS;
    } else {
      return ResponseUtils.FAILED;
    }
  }

  public JSONObject getInputDocument(Term term) throws IOException {
    SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomy = lindenNRTSearcherManager.acquire();
    try {
      IndexSearcher indexSearcher = searcherAndTaxonomy.searcher;
      TopDocs results = indexSearcher.search(new TermQuery(term), 1);
      if (results.scoreDocs.length == 0) {
        return null;
      }
      int docId = results.scoreDocs[0].doc;
      String source = LindenUtil.getSource(indexSearcher, docId, null, null, config);
      return JSONObject.parseObject(source);
    } finally {
      lindenNRTSearcherManager.release(searcherAndTaxonomy);
    }
  }

  protected Response updateIndexedFields(LindenDocument lindenDoc) throws IOException {
    JSONObject oldDoc = getInputDocument(new Term(idFieldName, lindenDoc.getId()));
    if (oldDoc == null) {
      // update failed for document not found.
      return ResponseUtils.FAILED;
    }

    for (LindenField field : lindenDoc.getFields()) {
      oldDoc.remove(field.getSchema().getName());
    }

    // merge new fields
    for (LindenField field : lindenDoc.getFields()) {
      // multi-value field is indexed as 2 parts.
      // one is each value in the specified schema,
      // the other is raw JSONArray in string format for source data and score model
      // so we need convert these 2 parts back to raw JSONArray format in the specified schema
      if (field.getSchema().isMulti()) {
        if (field.getSchema().isDocValues()) {
          oldDoc.put(field.getSchema().getName(), JSON.parseArray(field.getValue()));
        }
        continue;
      }
      String fieldName = field.getSchema().getName();
      Object val = LindenUtil.parseLindenValue(field.getValue(), field.schema.getType());
      oldDoc.put(fieldName, val);
    }

    LindenDocument newDoc = LindenDocumentBuilder.build(config.getSchema(), oldDoc);
    Document doc = LindenDocParser.parse(newDoc, config);
    if (doc == null) {
      return ResponseUtils.FAILED;
    }
    trackingIndexWriter.updateDocument(new Term(idFieldName, lindenDoc.getId()), doc);
    return ResponseUtils.SUCCESS;
  }

  public Response updateDocValues(LindenDocument lindenDoc) throws IOException {
    Document doc = LindenDocParser.parse(lindenDoc, config);
    if (doc != null) {
      IndexWriter writer = trackingIndexWriter.getIndexWriter();
      List<Field> fields = new ArrayList<>();
      for (int i = 0; i < doc.getFields().size(); ++i) {
        Field field = (Field) doc.getFields().get(i);
        final FieldInfo.DocValuesType dvType = field.fieldType().docValueType();
        if (dvType == FieldInfo.DocValuesType.NUMERIC || dvType == FieldInfo.DocValuesType.BINARY) {
          fields.add(field);
        }
      }
      if (!fields.isEmpty()) {
        writer.updateDocValues(
            new Term(idFieldName, lindenDoc.getId()), fields.toArray(new Field[fields.size()]));
      }
      return ResponseUtils.SUCCESS;
    } else {
      return ResponseUtils.FAILED;
    }
  }

  public Response update(LindenDocument lindenDoc) throws IOException {
    boolean isDocValuesUpdate = LindenDocParser.isDocValueFields(lindenDoc);
    if (isDocValuesUpdate) {
      return updateDocValues(lindenDoc);
    } else {
      return updateIndexedFields(lindenDoc);
    }
  }

  public Response delete(String id) throws IOException {
    if (id != null) {
      trackingIndexWriter.deleteDocuments(new TermQuery(new Term(idFieldName, id)));
      return ResponseUtils.SUCCESS;
    } else {
      return ResponseUtils.FAILED;
    }
  }

  @Override
  public void commit() throws IOException {
    indexWriter.commit();
    if (taxoWriter != null) {
      taxoWriter.commit();
    }
  }
}
