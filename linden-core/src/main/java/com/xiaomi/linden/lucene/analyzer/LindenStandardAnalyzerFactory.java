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

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;

import com.xiaomi.linden.plugin.LindenPluginFactory;

public class LindenStandardAnalyzerFactory implements LindenPluginFactory<StandardAnalyzer> {

  private static final String STOPWORDS_EMPTY = "stopwords.empty";

  @Override
  public StandardAnalyzer getInstance(Map<String, String> params) throws IOException {
    if (params.containsKey(STOPWORDS_EMPTY)) {
      if (Boolean.parseBoolean(params.get(STOPWORDS_EMPTY))) {
        return new StandardAnalyzer(CharArraySet.EMPTY_SET);
      }
    }
    return new StandardAnalyzer();
  }
}
