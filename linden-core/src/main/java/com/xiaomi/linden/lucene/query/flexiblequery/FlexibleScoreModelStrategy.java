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
import java.util.List;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.Similarity;

import com.xiaomi.linden.core.search.query.model.LindenScoreModelStrategy;

// not thread safe.
abstract public class FlexibleScoreModelStrategy extends LindenScoreModelStrategy {

  private MatchedInfoMatrix matchedMatrix;
  protected Similarity similarity;
  private String explanation;
  private String[] fieldExpls;
  private String[][] termExpls;
  private Float[] fieldScores;
  private Float[][] termScores;
  private boolean isExplain = false;

  public void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
  }

  public void setMatchedMatrix(MatchedInfoMatrix matchedMatrix) {
    this.matchedMatrix = matchedMatrix;
  }

  public TermDocsEnum termMatchedInfo(int field, int term) {
    return matchedMatrix.get(field, term);
  }

  @Override
  public int doc() {
    return matchedMatrix.doc();
  }

  public int getFieldLength() {
    return matchedMatrix.getFieldLength();
  }

  public float getFieldBoost(int field) {
    return matchedMatrix.getFieldBoost(field);
  }

  public int getTermLength() {
    return matchedMatrix.getTermLength();
  }

  public boolean isMatched(int field, int term) {
    return termMatchedInfo(field, term).isMatched();
  }

  public float getScore(int field, int term) throws IOException {
    return getRawScore(field, term) * getFieldBoost(field);
  }

  public float getRawScore(int field, int term) throws IOException {
    return termMatchedInfo(field, term).score();
  }

  public Explanation explain(int field, int term, Query query) {
    return termMatchedInfo(field, term).explain(similarity, query);
  }

  public String field(int field, int term) {
    return termMatchedInfo(field, term).term().term.field();
  }

  public String text(int field, int term) {
    return termMatchedInfo(field, term).term().term.text();
  }

  public float getTermBoost(int term) {
    if (getFieldLength() != 0) {
      return termMatchedInfo(0, term).term().boost;
    }
    return 1;
  }

  public int freq(int field, int term) {
    return termMatchedInfo(field, term).freq();
  }

  public int position(int field, int term) {
    return termMatchedInfo(field, term).position();
  }

  public List<Integer> positions(int field, int term) {
    return termMatchedInfo(field, term).positions();
  }

  public Explanation explain(Similarity similarity, Query query, int doc) throws IOException {
    Explanation expl = new Explanation();
    isExplain = true;
    if (fieldExpls == null) {
      fieldExpls = new String[matchedMatrix.getFieldLength()];
      fieldScores = new Float[matchedMatrix.getFieldLength()];
    } else {
      Arrays.fill(fieldExpls, null);
      Arrays.fill(fieldScores, null);
    }
    if (termExpls == null) {
      termExpls = new String[matchedMatrix.getFieldLength()][matchedMatrix.getTermLength()];
      termScores = new Float[matchedMatrix.getFieldLength()][matchedMatrix.getTermLength()];
    } else {
      Arrays.fill(termExpls, null);
      Arrays.fill(termScores, null);
    }
    double score = computeScore();
    expl.setDescription(explanation != null ? explanation : "FlexibleWeight");
    for (int i = 0; i < getFieldLength(); ++i) {
      Explanation subExpl = new Explanation();
      int matched = 0;
      for (int j = 0; j < getTermLength(); ++j) {
        if (isMatched(i, j)) {
          Explanation result = new Explanation();
          if (termExpls[i][j] != null) {
            result.setDescription(String.format("%s", termExpls[i][j]));
            result.setValue(termScores[i][j] != null ? termScores[i][j] : getScore(i, j));
          } else {
            result.setDescription(String.format("%.2f * %.2f * %.2f -- (%s %d)",
                                                getRawScore(i, j), getFieldBoost(i), getTermBoost(j), text(i, j),
                                                position(i, j)));
            result.setValue(getScore(i, j));
          }
          subExpl.addDetail(result);
          ++matched;
        }
      }
      if (fieldExpls[i] != null) {
        subExpl.setDescription(String.format("%s        [FIELD:%s MATCHED:%d]", fieldExpls[i], field(i, 0), matched));
        subExpl.setValue(fieldScores[i] != null ? fieldScores[i] : 1);
      } else {
        subExpl.setDescription(String.format("FIELD:%s MATCHED:%d", field(i, 0), matched));
        subExpl.setValue(1);
      }
      expl.addDetail(subExpl);
    }
    expl.setValue((float) score);
    return expl;
  }

  public void writeExplanation(String format, Object... args) {
    if (isExplain) {
      explanation = String.format(format, args);
    }
  }

  public String getExpl(String format, Object... args) {
    return String.format(format, args);
  }

  public void addTermExpl(int i, int j, String expl) {
    if (isExplain) {
      termExpls[i][j] = expl;
    }
  }

  public void addTermExpl(int i, int j, float score, String expl) {
    addTermExpl(i, j, expl);
    addTermScore(i, j, score);
  }

  public void addFieldExpl(int i, String expl) {
    if (isExplain) {
      fieldExpls[i] = expl;
    }
  }

  public void addFieldExpl(int i, float score, String expl) {
    addFieldExpl(i, expl);
    addFieldScore(i, score);
  }

  public void addTermScore(int i, int j, float score) {
    if (isExplain) {
      termScores[i][j] = score;
    }
  }

  public void addFieldScore(int i, float score) {
    if (isExplain) {
      fieldScores[i] = score;
    }
  }

  public void setRootExpl(String expl) {
    if (isExplain) {
      explanation = expl;
    }
  }
}
