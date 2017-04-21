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

import com.xiaomi.linden.thrift.common.LindenHit;
import com.xiaomi.linden.thrift.common.LindenSnippet;
import com.xiaomi.linden.thrift.common.SnippetParam;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.postingshighlight.PostingsHighlighter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LindenSnippetGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(LindenSnippetGenerator.class);
  private final PostingsHighlighter highlighter;

  public LindenSnippetGenerator() {
    highlighter = new PostingsHighlighter();
  }

  public List<LindenHit> generateSnippet(List<LindenHit> lindenHits, SnippetParam snippetParam, Query query,
                                         IndexSearcher searcher, ScoreDoc[] hits) {
    if (!snippetParam.isSetFields())
      return lindenHits;
    String[] fields = new String[snippetParam.getFieldsSize()];
    for (int i = 0; i < snippetParam.getFieldsSize(); ++i) {
      fields[i] = snippetParam.getFields().get(i).getField();
    }
    int docIds[] = new int[hits.length];
    for (int i = 0; i < docIds.length; i++) {
      docIds[i] = hits[i].doc;
    }
    int maxPassages[] = new int[snippetParam.getFieldsSize()];
    Arrays.fill(maxPassages, 10);
    try {
      Map<String, String[]> snippets = highlighter.highlightFields(fields, query, searcher, docIds, maxPassages);
      lindenHits = getSnippetHits(lindenHits, snippets);
    } catch (Exception ex) {
      LOGGER.error("Hit exception in PostingsHighlighter, ", ex);
    }
    return lindenHits;
  }

  private List<LindenHit> getSnippetHits(List<LindenHit> hits, Map<String, String[]> snippets) {
    if (snippets != null) {
      for (Map.Entry<String, String[]> entry : snippets.entrySet()) {
        String field = entry.getKey();
        String[] fieldSnippets = entry.getValue();
        for (int i = 0; i < fieldSnippets.length && i < hits.size(); ++i) {
          if (fieldSnippets[i] != null) {
            hits.get(i).putToSnippets(field, new LindenSnippet(fieldSnippets[i]));
          } else {
            hits.get(i).putToSnippets(field, new LindenSnippet(""));
          }
        }
      }
    }
    return hits;
  }
}
