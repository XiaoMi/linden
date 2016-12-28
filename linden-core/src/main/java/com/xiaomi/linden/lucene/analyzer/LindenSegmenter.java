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

package com.xiaomi.linden.lucene.analyzer;

import java.io.Closeable;
import java.util.List;

public abstract class LindenSegmenter implements Closeable {
  static public class Term {
    public String value;
    public int startOffset = -1;
    public int endOffset = -1;

    public Term(String value, int start, int end) {
      this.value = value;
      this.startOffset = start;
      this.endOffset = end;
    }
  }

  public abstract List<Term> parse(String content) throws Exception;
}
