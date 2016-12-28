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
import java.util.Arrays;

import org.apache.lucene.search.Scorer;

public class FlexibleScorer extends Scorer {

  private int numMatchedEnums;
  private int doc = -1;
  private TermDocsEnum[] matchedEnumsList;
  private MatchedInfoMatrix matchedInfoMatrix;
  private FlexibleScoreModelStrategy strategy;
  private boolean[] termFieldHit;
  private int minMatch;

  protected FlexibleScorer(FlexibleWeight weight, FlexibleScoreModelStrategy strategy, TermDocsEnum[][] matchedEnumsMatrix) throws IOException {
    super(weight);
    numMatchedEnums = matchedEnumsMatrix.length * matchedEnumsMatrix[0].length;
    termFieldHit = new boolean[matchedEnumsMatrix[0].length];

    this.matchedInfoMatrix = new MatchedInfoMatrix(matchedEnumsMatrix, weight.getQuery().getFieldBoosts());

    this.strategy = strategy;
    this.strategy.setMatchedMatrix(matchedInfoMatrix);
    this.strategy.prepare(0, 0, false);

    matchedEnumsList = new TermDocsEnum[numMatchedEnums];
    int counter = 0;
    for (TermDocsEnum[] fieldEnums : matchedEnumsMatrix) {
      for (TermDocsEnum docsEnum: fieldEnums) {
        matchedEnumsList[counter] = docsEnum;
        ++counter;
      }
    }

    if (weight.getQuery().isFullMatch()) {
      minMatch = matchedInfoMatrix.getTermLength();
    } else {
      double ratio = weight.getQuery().getMatchRatio();
      if (ratio > 0) {
        minMatch = Math.max(1, (int) Math.ceil(ratio * matchedInfoMatrix.getTermLength()));
      }
    }

    // first time init.
    for (TermDocsEnum termDocsEnum : matchedEnumsList) {
      termDocsEnum.next();
    }

    heapify();
  }

  /**
   * Organize matchedEnumsList into a min heap with scorers generating the earliest document on top.
   */
  protected final void heapify() {
    for (int i = (numMatchedEnums >> 1) - 1; i >= 0; i--) {
      heapAdjust(i);
    }
  }
  /**
   * The subtree of matchedEnumsList at root is a min heap except possibly for its root element.
   * Bubble the root down as required to make the subtree a heap.
   */
  protected final void heapAdjust(int root) {
    TermDocsEnum scorer = matchedEnumsList[root];
    int doc = scorer.doc();
    int i = root;
    while (i <= (numMatchedEnums >> 1) - 1) {
      int lchild = (i << 1) + 1;
      TermDocsEnum lscorer = matchedEnumsList[lchild];
      int ldoc = lscorer.doc();
      int rdoc = Integer.MAX_VALUE, rchild = (i << 1) + 2;
      TermDocsEnum rscorer = null;
      if (rchild < numMatchedEnums) {
        rscorer = matchedEnumsList[rchild];
        rdoc = rscorer.doc();
      }
      if (ldoc < doc) {
        if (rdoc < ldoc) {
          matchedEnumsList[i] = rscorer;
          matchedEnumsList[rchild] = scorer;
          i = rchild;
        } else {
          matchedEnumsList[i] = lscorer;
          matchedEnumsList[lchild] = scorer;
          i = lchild;
        }
      } else if (rdoc < doc) {
        matchedEnumsList[i] = rscorer;
        matchedEnumsList[rchild] = scorer;
        i = rchild;
      } else {
        return;
      }
    }
  }

  /**
   * Remove the root Scorer from subScorers and re-establish it as a heap
   */
  protected final void heapRemoveRoot() {
    if (numMatchedEnums == 1) {
      matchedEnumsList[0] = null;
      numMatchedEnums = 0;
    } else {
      matchedEnumsList[0] = matchedEnumsList[numMatchedEnums - 1];
      matchedEnumsList[numMatchedEnums - 1] = null;
      --numMatchedEnums;
      heapAdjust(0);
    }
  }

  @Override
  public float score() throws IOException {
    return (float) strategy.computeScore();
  }

  @Override
  public int freq() throws IOException {
    return 1;
  }

  @Override
  public int docID() {
    return doc;
  }

  private void clear() {
    for (int i = 0; i < matchedInfoMatrix.getFieldLength(); ++i) {
      for (int j = 0; j < matchedInfoMatrix.getTermLength(); ++j) {
        matchedInfoMatrix.get(i, j).clearMatchedInfo();
      }
    }
    Arrays.fill(termFieldHit, false);
  }

  @Override
  public int nextDoc() throws IOException {
    int doc;
    while (true) {
      doc = getMatchedDoc();
      if (doc == NO_MORE_DOCS) {
        return doc;
      }
      if (minMatch > 1) {
        int matchedTermNum = 0;
        for (int i = 0; i < termFieldHit.length; ++i) {
          if (termFieldHit[i]) {
            ++matchedTermNum;
          }
        }
        if (matchedTermNum >= minMatch) {
          return doc;
        }
      } else {
        return doc;
      }
    }
  }

  private int getMatchedDoc() throws IOException {
    clear();

    if (matchedEnumsList.length == 0 || matchedEnumsList[0] == null || doc == NO_MORE_DOCS) {
      return NO_MORE_DOCS;
    }

    doc = matchedEnumsList[0].doc();
    matchedInfoMatrix.setDoc(doc);

    while (matchedEnumsList[0].doc() == doc) {
      matchedEnumsList[0].saveMatchedInfo();
      termFieldHit[matchedEnumsList[0].termPos] = true;
      if (matchedEnumsList[0].next()) {
        heapAdjust(0);
      } else {
        heapRemoveRoot();
        if (numMatchedEnums == 0) {
          return doc;
        }
      }
    }
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    int doc;
    while ((doc = nextDoc()) < target) {
    }
    return doc;
  }

  @Override
  public long cost() {
    return 0;
  }

  public FlexibleScoreModelStrategy getStrategy() { return strategy; }
}
