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

public class MatchedInfoMatrix {
  private TermDocsEnum[][] values;
  private float[] fieldBoost;
  private int doc;

  public MatchedInfoMatrix(TermDocsEnum[][] matchedInfos, float[] fieldBoost) {
    values = matchedInfos;
    this.fieldBoost = fieldBoost;
  }

  public void setDoc(int doc) {
    this.doc = doc;
  }

  public int doc() { return doc; }

  public TermDocsEnum get(int x, int y) {
    return values[x][y];
  }

  public int getFieldLength() { return values.length; }

  public int getTermLength() {
    if (values.length != 0) {
      return values[0].length;
    }
    return 0;
  }

  public float getFieldBoost(int field) { return fieldBoost[field]; }

}
