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

/*
public class BestMatchQuery extends FlexibleQuery {

  public BestMatchQuery(List<FlexibleQuery.FlexibleField> fields, String content, Analyzer analyzer) throws IOException {
    super();
    setMatchingStrategy(new BestMatchStrategy());
  }

  public static class BestMatchStrategy extends CustomMatchingStrategy {
    @Override
    public double score() throws IOException {
      double score = 0f;
      boolean[] termMatched = new boolean[getTermLength()];
      for (int i = 0; i < getFieldLength(); ++i) {
        int prePos = -1;
        for (int j = 0; j < getTermLength(); ++j) {
          if (isMatched(i, j)) {
            if (!termMatched[j]) {
              score += getScore(i, j);
              termMatched[i] = true;
            }
            if (prePos == -1) {
              prePos = position(i, j);
            } else {
              if (prePos + 1 == position(i, j)) {
                score += 0.2;
              }
            }
          }
        }
      }
      return score;
    }

    @Override
    public Explanation explain(Similarity similarity, Query query, int doc) {
      return null;
    }
  }
}
*/
