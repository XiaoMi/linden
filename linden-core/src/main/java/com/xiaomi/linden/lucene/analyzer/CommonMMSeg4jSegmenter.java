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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chenlb.mmseg4j.analysis.ComplexAnalyzer;
import com.chenlb.mmseg4j.analysis.CutLetterDigitFilter;
import com.chenlb.mmseg4j.analysis.MMSegAnalyzer;
import com.chenlb.mmseg4j.analysis.MaxWordAnalyzer;
import com.google.common.base.Function;

import com.xiaomi.linden.common.util.DirectoryChangeWatcher;
import com.xiaomi.linden.common.util.FileChangeWatcher;

public class CommonMMSeg4jSegmenter extends LindenSegmenter {
  private final Logger LOG = LoggerFactory.getLogger(CommonMMSeg4jSegmenter.class);
  private Analyzer analyzer = null;
  private CharArraySet stopWords = null;
  private boolean cutLetterDigit = false;
  private String mode;
  private DirectoryChangeWatcher directoryChangeWatcher;
  private FileChangeWatcher fileChangeWatcher;

  public CommonMMSeg4jSegmenter(String mode, String dictPath, String stopWordsPath, boolean cutLetterDigit) {
    this.mode = mode;
    this.cutLetterDigit = cutLetterDigit;

    initAnalyzer(dictPath);
    initStopWords(stopWordsPath);

    if (dictPath != null) {
      directoryChangeWatcher = new DirectoryChangeWatcher(dictPath, reloadDict);
      directoryChangeWatcher.start();
    }
    if (stopWordsPath != null) {
      fileChangeWatcher = new FileChangeWatcher(stopWordsPath, reloadStopWord);
      fileChangeWatcher.start();
    }
  }

  private void initAnalyzer(String dictPath) {
    if (mode != null && mode.equalsIgnoreCase("Complex")) {
      if (dictPath != null) {
        analyzer = new ComplexAnalyzer(dictPath);
      } else {
        analyzer = new ComplexAnalyzer();
      }
    } else {
      // default mode is MaxWord
      if (dictPath != null) {
        analyzer = new MaxWordAnalyzer(dictPath);
      } else {
        analyzer = new MaxWordAnalyzer();
      }
    }
  }

  private void initStopWords(String stopWordsPath) {
    if (stopWordsPath != null) {
      try {
        List<String> lines = FileUtils.readLines(new File(stopWordsPath));
        Set<String> set = new HashSet<>(lines);
        stopWords = CharArraySet.copy(set);
      } catch (IOException e) {
        throw new RuntimeException("Read stop words failed path : " + stopWordsPath);
      }
    }
  }

  final Function<String,Object> reloadDict = new Function<String,Object>() {
    @Override
    public String apply(String dictPath) {
      LOG.info("Starting reload dict {}", dictPath);
      if (((MMSegAnalyzer) analyzer).getDict().wordsFileIsChange()) {
        ((MMSegAnalyzer) analyzer).getDict().reload();
      }
      LOG.info("Finished reload dict {}", dictPath);
      return null;
    }
  };

  final Function<String,Object> reloadStopWord = new Function<String,Object>() {
    @Override
    public String apply(String stopWordsPath) {
      LOG.info("Starting reload stop words {}", stopWordsPath);
      initStopWords(stopWordsPath);
      LOG.info("Finished reload stop words {}", stopWordsPath);
      return null;
    }
  };

  @Override
  public List<Term> parse(String content) throws Exception {
    List<Term> words = new ArrayList<>();
    if (content == null || content.isEmpty()) {
      return words;
    }

    TokenStream stream = null;
    try {
      stream = analyzer.tokenStream("", content);
      stream.reset();
      if (stopWords != null) {
        if (cutLetterDigit) {
          stream = new CutLetterDigitFilter(new StopFilter(stream, stopWords));
        } else {
          stream = new StopFilter(stream, stopWords);
        }
      } else {
        if (cutLetterDigit) {
          stream = new CutLetterDigitFilter(stream);
        }
      }
      CharTermAttribute termAttr = stream.getAttribute(CharTermAttribute.class);
      OffsetAttribute offsetAttribute = stream.getAttribute(OffsetAttribute.class);
      while (stream.incrementToken()) {
        words.add(new Term(termAttr.toString(), offsetAttribute.startOffset(), offsetAttribute.endOffset()));
      }
    } catch (IOException e) {
      throw new Exception(content + " extract words from phrase failed!", e);
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
    return words;
  }

  @Override
  public void close() throws IOException {
    if (directoryChangeWatcher != null) {
      directoryChangeWatcher.close();
    }
    if (fileChangeWatcher != null) {
      fileChangeWatcher.close();
    }
  }
}
