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

package com.xiaomi.linden.lucene.query.flexiblequery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;

import com.xiaomi.linden.core.search.query.model.LindenScoreModelStrategyBuilder;

public class FlexibleWeight extends Weight {

  private FlexibleQuery query;
  private Similarity similarity;

  static public class TermStats {

    public Similarity.SimWeight stats;
    public Term term;
    public TermContext termContext;
  }

  private TermStats[][] termStatsMatrix;

  public FlexibleWeight(FlexibleQuery query, IndexSearcher searcher) throws IOException {
    this.query = query;
    this.similarity = searcher.getSimilarity();
    final IndexReaderContext context = searcher.getTopReaderContext();

    int[] maxDocFreqs = null;
    long[] maxTotalTermFreqs = null;
    Map<Term, TermContext> builtTermMap = new HashMap<>();
    if (query.enableGlobalIDF()) {
      FlexibleQuery.FlexibleTerm[][] globalTerms = query.getGlobalTerms();
      TermContext[][] globalStates = new TermContext[globalTerms.length][];
      for (int i = 0; i < globalTerms.length; ++i) {
        globalStates[i] = new TermContext[globalTerms[i].length];
        for (int j = 0; j < globalTerms[i].length; ++j) {
          Term term = globalTerms[i][j].term;
          TermContext termContext = builtTermMap.get(term);
          if (termContext != null) {
            globalStates[i][j] = termContext;
          } else {
            globalStates[i][j] = TermContext.build(context, globalTerms[i][j].term);
            builtTermMap.put(term, globalStates[i][j]);
          }
        }
      }
      maxDocFreqs = new int[globalTerms[0].length];
      maxTotalTermFreqs = new long[globalTerms[0].length];
      int fieldLength = globalTerms.length;
      int termLength = globalTerms[0].length;
      for (int i = 0; i < termLength; ++i) {
        int maxDocFreq = 0;
        long maxTotalTermFreq = 0;
        for (int j = 0; j < fieldLength; ++j) {
          maxDocFreq = Math.max(globalStates[j][i].docFreq(), maxDocFreq);
          maxTotalTermFreq = Math.max(globalStates[j][i].totalTermFreq(), maxTotalTermFreq);
        }
        maxDocFreqs[i] = maxDocFreq;
        maxTotalTermFreqs[i] = maxTotalTermFreq;
      }
    }

    FlexibleQuery.FlexibleTerm[][] terms = query.getTerms();
    TermContext[][] states = new TermContext[terms.length][];
    for (int i = 0; i < terms.length; ++i) {
      states[i] = new TermContext[terms[i].length];
      for (int j = 0; j < terms[i].length; ++j) {
        Term term = terms[i][j].term;
        TermContext termContext = builtTermMap.get(term);
        if (termContext != null) {
          states[i][j] = termContext;
        } else {
          states[i][j] = TermContext.build(context, terms[i][j].term);
          builtTermMap.put(term, states[i][j]);
        }
      }
    }
    termStatsMatrix = new TermStats[terms.length][];
    for (int i = 0; i < terms.length; ++i) {
      termStatsMatrix[i] = new TermStats[terms[i].length];
      for (int j = 0; j < terms[i].length; ++j) {
        FlexibleQuery.FlexibleTerm term = terms[i][j];
        TermContext state = states[i][j];
        TermStatistics termStats;
        if (query.enableGlobalIDF()) {
          termStats = new TermStatistics(term.term.bytes(), maxDocFreqs[j], maxTotalTermFreqs[j]);
        } else {
          termStats = searcher.termStatistics(term.term, state);
        }
        Similarity.SimWeight stats = similarity.computeWeight(term.boost,
                                                              searcher.collectionStatistics(term.term.field()),
                                                              termStats);
        TermStats termStatsInfo = new TermStats();
        termStatsInfo.stats = stats;
        termStatsInfo.term = term.term;
        termStatsInfo.termContext = state;
        termStatsMatrix[i][j] = termStatsInfo;
      }
    }
  }

  @Override
  public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
    FlexibleScorer scorer = (FlexibleScorer) scorer(context, context.reader().getLiveDocs());
    if (scorer != null) {
      int newDoc = scorer.advance(doc);
      if (newDoc == doc) {
        FlexibleScoreModelStrategy strategy = scorer.getStrategy();
        strategy.prepare(0, 0, true);
        return strategy.explain(similarity, query, doc);
      }
    }
    return new ComplexExplanation(false, 0.0f, "no matching term");
  }

  @Override
  public FlexibleQuery getQuery() {
    return query;
  }

  @Override
  public float getValueForNormalization() throws IOException {
    float sum = 0f;
    for (TermStats[] aTermStatsMatrix : termStatsMatrix) {
      for (TermStats termStats : aTermStatsMatrix) {
        sum += termStats.stats.getValueForNormalization();
      }
    }
    sum *= query.getBoost() * query.getBoost();
    return sum;
  }

  @Override
  public void normalize(float norm, float topLevelBoost) {
    topLevelBoost *= query.getBoost();
    for (TermStats[] aTermStatsMatrix : termStatsMatrix) {
      for (TermStats termStats : aTermStatsMatrix) {
        termStats.stats.normalize(norm, topLevelBoost);
      }
    }
  }

  @Override
  public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    FlexibleQuery.FlexibleTerm[][] termMatrix = query.getTerms();
    TermDocsEnum[][] matchedEnumsMatrix = new TermDocsEnum[termMatrix.length][];
    for (int i = 0; i < termMatrix.length; ++i) {
      FlexibleQuery.FlexibleTerm[] fieldTerms = termMatrix[i];
      matchedEnumsMatrix[i] = new TermDocsEnum[fieldTerms.length];
      if (fieldTerms.length == 0) {
        return null;
      }

      // Reuse single TermsEnum below:
      Terms terms = context.reader().terms(fieldTerms[0].term.field());
      if (terms == null) {
        for (int j = 0; j < fieldTerms.length; ++j) {
          matchedEnumsMatrix[i][j] = new TermDocsEnum(fieldTerms[j], 0, null, null, j);
        }
      } else {
        final TermsEnum termsEnum = terms.iterator(null);
        for (int j = 0; j < fieldTerms.length; ++j) {
          TermStats termStats = termStatsMatrix[i][j];
          Term term = termStats.term;
          final TermState state = termStats.termContext.get(context.ord);
          Similarity.SimScorer docScorer = similarity.simScorer(termStats.stats, context);
          DocsAndPositionsEnum postings = null;
          int docFreq = 0;
          if (state != null) {
            termsEnum.seekExact(term.bytes(), state);
            postings = termsEnum.docsAndPositions(acceptDocs, null, DocsAndPositionsEnum.FLAG_OFFSETS);
            docFreq = termsEnum.docFreq();
          }
          matchedEnumsMatrix[i][j] = new TermDocsEnum(fieldTerms[j], docFreq, postings, docScorer, j);
        }
      }
    }
    FlexibleScoreModelStrategy strategy;
    try {
      strategy = (FlexibleScoreModelStrategy) LindenScoreModelStrategyBuilder.buildFlexibleQueryStrategy(query);
    } catch (Exception e) {
      throw new IOException(e);
    }
    strategy.preProcess(context, query.getConfig().getSchema(), query.getModel());
    strategy.setSimilarity(similarity);
    strategy.init();
    return new FlexibleScorer(this, strategy, matchedEnumsMatrix);
  }

  @Override
  public boolean scoresDocsOutOfOrder() {
    return true;
  }
}
