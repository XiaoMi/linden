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

import com.xiaomi.linden.plugin.LindenPluginFactory;

public class LindenMMSeg4jAnalyzerFactory implements LindenPluginFactory<LindenMMSeg4jAnalyzer> {

  private static final String MODE = "mode";
  private static final String DICT = "dict";
  private static final String STOPWORDS = "stopwords";
  private static final String CUT_LETTER_DIGIT = "cut_letter_digit";

  @Override
  public LindenMMSeg4jAnalyzer getInstance(Map<String, String> params) throws IOException {
    String mode = params.get(MODE);
    String dictPath = params.get(DICT);
    String stopWordPath = params.get(STOPWORDS);
    boolean cut = params.containsKey(CUT_LETTER_DIGIT) && params.get(CUT_LETTER_DIGIT).equalsIgnoreCase("TRUE");
    return new LindenMMSeg4jAnalyzer(mode, dictPath, stopWordPath, cut);
  }
}
