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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Throwables;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

public class LindenMMSeg4jAnalyzer extends Analyzer {

  private LindenSegmenter segmenter;

  public LindenMMSeg4jAnalyzer(String mode, String dictPath, String stopWordsPath, boolean cutLetterDigit) {
    segmenter = new CommonMMSeg4jSegmenter(mode, dictPath, stopWordsPath, cutLetterDigit);
  }

  public class LindenMMSeg4jTokenizer extends Tokenizer {

    private List<LindenSegmenter.Term> tokens = new ArrayList<>();
    private int currentPos;
    private int inputLength;
    private CharTermAttribute termAtt;
    private OffsetAttribute offsetAtt;

    protected LindenMMSeg4jTokenizer(final Reader input) {
      super(input);
      termAtt = addAttribute(CharTermAttribute.class);
      offsetAtt = addAttribute(OffsetAttribute.class);
      currentPos = 0;
      inputLength = 0;
    }

    @Override
    public final boolean incrementToken() throws IOException {
      clearAttributes();
      if (tokens.isEmpty()) {
        try {
          currentPos = 0;
          BufferedReader br = new BufferedReader(input);
          String content = br.readLine();
          inputLength = content.length();
          tokens = segmenter.parse(content);
        } catch (Exception e) {
          throw new IOException(Throwables.getStackTraceAsString(e));
        }
      }
      if (!tokens.isEmpty()) {
        if (currentPos != tokens.size()) {
          LindenSegmenter.Term word = tokens.get(currentPos);
          if (word != null) {
            ++currentPos;
            termAtt.copyBuffer(word.value.toCharArray(), 0, word.value.length());
            offsetAtt.setOffset(word.startOffset, word.endOffset);
            return true;
          }
        } else {
          currentPos = 0;
          tokens.clear();
        }
      }
      return false;
    }

    @Override
    public final void end() throws IOException {
      super.end();
      // set final offset
      offsetAtt.setOffset(inputLength, inputLength);
    }
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
    return new TokenStreamComponents(new LindenMMSeg4jTokenizer(reader));
  }

  @Override
  public void close() {
    try {
      segmenter.close();
    } catch (IOException e) {
      // do nothing
    }
  }

}
