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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.Similarity;

public class TermDocsEnum {

  protected DocsAndPositionsEnum postings;
  protected FlexibleQuery.FlexibleTerm term;
  protected Similarity.SimScorer docScorer;

  protected int doc;
  private int position;
  private int[] positions;
  private int freq;
  protected int termPos = -1;
  protected int docFreq = -1;

  private int matchedDoc = -1;
  private int matchedPosition = -1;
  private int[] matchedPositions;
  private int matchedFreq = -1;
  private int field;
  private int initPositionSize = 5;


  public TermDocsEnum(FlexibleQuery.FlexibleTerm term, int docFreq, DocsAndPositionsEnum postings,
                      Similarity.SimScorer docScorer, int field, int termPos) throws IOException {
    this.doc = -1;
    this.term = term;
    this.postings = postings;
    this.docFreq = docFreq;
    this.docScorer = docScorer;
    this.field = field;
    this.termPos = termPos;
    this.positions = new int[initPositionSize];
    this.matchedPositions = new int[initPositionSize];
  }

  public FlexibleQuery.FlexibleTerm term() {
    return term;
  }


  public float score(int doc) throws IOException {
    if (isMatched(doc)) {
      return docScorer.score(matchedDoc, matchedFreq);
    } else {
      return 0f;
    }
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
    if (freq > initPositionSize) {
      int newSize = 2 * freq;
      positions = new int[newSize];
      matchedPositions = new int[newSize];
      initPositionSize = newSize;
    }
    for (int i = 0; i < freq; i++) {
      positions[i] = postings.nextPosition();
    }
    position = positions[0];
    return true;
  }


  public int doc() {
    return doc;
  }

  public int freq(int doc) {
    return doc == matchedDoc ? matchedFreq : -1;
  }

  public int position(int doc) {
    return doc == matchedDoc ? matchedPosition : -1;
  }

  public int[] positions(int doc) {
    if (doc == matchedDoc) {
      int[] matchedPositionsData = new int[matchedFreq];
      System.arraycopy(matchedPositions, 0, matchedPositionsData, 0, matchedFreq);
      return matchedPositionsData;
    }
    return new int[0];
  }

  public String toString() {
    if (postings != null) {
      return "MatchedDocs(" + term.toString() + ")@" +
             (doc == -1 ? "START" : (doc == Integer.MAX_VALUE) ? "END" : doc + "-" + position);
    } else {
      return term + "NotMatched";
    }
  }

  public Explanation explain(Similarity similarity, Query query, int doc) {
    if (!isMatched(doc)) {
      return null;
    }
    ComplexExplanation result = new ComplexExplanation();
    result.setDescription(
        "weight(" + query + " in " + doc + ") [" + similarity.getClass().getSimpleName() + "], result of:");
    Explanation scoreExplanation = docScorer.explain(doc, new Explanation(matchedFreq, "termFreq=" + matchedFreq));
    result.addDetail(scoreExplanation);
    result.setValue(scoreExplanation.getValue());
    result.setMatch(true);
    return result;
  }

  public boolean isMatched(int doc) {
    return matchedDoc == doc;
  }


  public void saveMatchedInfo() {
    this.matchedDoc = doc;
    this.matchedPosition = position;
    this.matchedFreq = freq;
    for (int i = 0; i < matchedFreq; i++) {
      matchedPositions[i] = positions[i];
    }
  }

  public int getField() {
    return field;
  }

}
