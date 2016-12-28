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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TermDocsEnum {
  protected DocsAndPositionsEnum postings;
  protected FlexibleQuery.FlexibleTerm term;
  protected Similarity.SimScorer docScorer;

  protected int doc;
  private int position;
  private List<Integer> positions;
  private int freq;
  protected int termPos = -1;
  protected int docFreq = -1;

  private int matchedDoc = -1;
  private int matchedPosition = -1;
  private List<Integer> matchedPositions;
  private int matchedFreq = -1;

  public TermDocsEnum(FlexibleQuery.FlexibleTerm term, int docFreq, DocsAndPositionsEnum postings, Similarity.SimScorer docScorer, int termPos) throws IOException {
    this.term = term;
    this.postings = postings;
    this.docFreq = docFreq;
    this.docScorer = docScorer;
    this.termPos = termPos;
  }

  public FlexibleQuery.FlexibleTerm term() {
    return term;
  }

  public float score() throws IOException {
    if (isMatched())
      return docScorer.score(matchedDoc, matchedFreq);
    else
      return 0f;
  }

  public boolean next() throws IOException {
    if (postings == null) {
      doc = DocIdSetIterator.NO_MORE_DOCS;
      return false;
    }
    doc = postings.nextDoc();
    if (doc == DocIdSetIterator.NO_MORE_DOCS) {
      return false;
    }
    freq = postings.freq();
    positions = new ArrayList<>();
    for (int i = 0; i < freq; i++) {
      positions.add(postings.nextPosition());
    }
    position = positions.get(0);
    return true;
  }



  public int doc() {
    return doc;
  }

  public int freq() {
    return matchedFreq;
  }

  public int position() {
    return matchedPosition;
  }

  public List<Integer> positions() {
    return matchedPositions;
  }

  public String toString() {
    if (postings != null) {
      return "MatchedDocs(" + term.toString() +")@" +
             (doc == -1 ? "START" : (doc == Integer.MAX_VALUE) ? "END" : doc + "-" + position);
    } else {
      return term + "NotMatched";
    }
  }

  public Explanation explain(Similarity similarity, Query query) {
    if (!isMatched())
      return null;
    ComplexExplanation result = new ComplexExplanation();
    result.setDescription("weight("+query+" in "+ doc +") [" + similarity.getClass().getSimpleName() + "], result of:");
    Explanation scoreExplanation = docScorer.explain(doc, new Explanation(freq, "termFreq=" + freq));
    result.addDetail(scoreExplanation);
    result.setValue(scoreExplanation.getValue());
    result.setMatch(true);
    return result;
  }

  public boolean isMatched() {
    return matchedDoc != -1 && docScorer != null && postings != null;
  }

  public void clearMatchedInfo() {
    this.matchedDoc = -1;
    this.matchedPosition = -1;
    this.matchedFreq = -1;
    this.matchedPositions = null;
  }

  public void saveMatchedInfo() {
    this.matchedDoc = doc;
    this.matchedPosition = position;
    this.matchedFreq = freq;
    this.matchedPositions = positions;
  }
}
