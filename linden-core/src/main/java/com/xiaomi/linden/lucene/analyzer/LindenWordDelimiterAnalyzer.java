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

import java.io.Reader;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class LindenWordDelimiterAnalyzer extends Analyzer {

  private static final String SET_STOP_WORDS = "set.stopwords";
  private static final String TO_LOWER_CASE = "lower.case";

  private boolean toLowerCase = true;
  private boolean setStopWords = true;

  private WordDelimiterFilterFactory factoryDefault;

  /**
   * generateWordParts
   * Causes parts of words to be generated:
   * <p/>
   * "PowerShot" => "Power" "Shot"
   * <p>
   * generateNumberParts
   * Causes number subwords to be generated:
   * <p/>
   * "500-42" => "500" "42"
   * <p>
   * catenateWords
   * Causes maximum runs of word parts to be catenated:
   * <p/>
   * "wi-fi" => "wifi"
   * <p>
   * catenateNumbers
   * Causes maximum runs of word parts to be catenated:
   * <p/>
   * "500-42" => "50042"
   * <p>
   * catenateAll
   * Causes all subword parts to be catenated:
   * <p/>
   * "wi-fi-4000" => "wifi4000"
   * <p>
   * preserveOriginal
   * Causes original words are preserved and added to the subword list (Defaults to false)
   * <p/>
   * "500-42" => "500" "42" "500-42"
   * <p>
   * splitOnCaseChange
   * If not set, causes case changes to be ignored (subwords will only be generated
   * given SUBWORD_DELIM tokens)
   * <p>
   * splitOnNumerics
   * If not set, causes numeric changes to be ignored (subwords will only be generated
   * given SUBWORD_DELIM tokens).
   * <p>
   * stemEnglishPossessive
   * Causes trailing "'s" to be removed for each subword
   * <p/>
   * "O'Neil's" => "O", "Neil"
   */

  public LindenWordDelimiterAnalyzer(Map<String, String> params) {
    if (params.containsKey(SET_STOP_WORDS)) {
      this.setStopWords = Boolean.parseBoolean(params.get(SET_STOP_WORDS));
      params.remove(SET_STOP_WORDS);
    }
    if (params.containsKey(TO_LOWER_CASE)) {
      this.toLowerCase = Boolean.parseBoolean(params.get(TO_LOWER_CASE));
      params.remove(TO_LOWER_CASE);
    }
    factoryDefault = new WordDelimiterFilterFactory(params);
  }


  @Override
  protected TokenStreamComponents createComponents(String s, Reader reader) {
    final Tokenizer source = new StandardTokenizer(reader);

    TokenStream ts = factoryDefault.create(source);
    if (this.toLowerCase) {
      ts = new LowerCaseFilter(ts);
    }
    if (this.setStopWords) {
      ts = new StopFilter(ts, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    }
    return new TokenStreamComponents(source, ts);
  }
}
