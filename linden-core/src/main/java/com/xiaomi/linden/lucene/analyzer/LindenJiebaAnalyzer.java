package com.xiaomi.linden.lucene.analyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Throwables;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.WordDictionary;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/**
 * Created by sls on 17-6-6.
 */
public class LindenJiebaAnalyzer extends Analyzer {

  private LindenSegmenter segmenter;

  public LindenJiebaAnalyzer(JiebaSegmenter.SegMode mode, String userDict) {
    if (userDict != null) {
      WordDictionary.getInstance().init(Paths.get(userDict));
    }
    segmenter = new CommonJiebaSegmenter(mode);
  }

  public class LindenJiebaTokenizer extends Tokenizer {

    private List<LindenSegmenter.Term> tokens = new ArrayList<>();
    private int currentPos;
    private int offset;
    private CharTermAttribute termAtt;
    private OffsetAttribute offsetAtt;

    protected LindenJiebaTokenizer(final Reader input) {
      super(input);
      termAtt = addAttribute(CharTermAttribute.class);
      offsetAtt = addAttribute(OffsetAttribute.class);
      currentPos = 0;
      offset = 0;
    }

    @Override
    public final boolean incrementToken() throws IOException {
      clearAttributes();
      if (tokens.isEmpty()) {
        try {
          currentPos = 0;
          offset = 0;
          BufferedReader br = new BufferedReader(input);
          String content = br.readLine();
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
            if (word.startOffset != -1) {
              offsetAtt.setOffset(word.startOffset, word.endOffset);
            } else {
              offsetAtt.setOffset(offset, offset + word.value.length());
              offset += word.value.length();
            }
            return true;
          }
        } else {
          currentPos = 0;
          offset = 0;
          tokens.clear();
        }
      }
      return false;
    }
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
    return new TokenStreamComponents(new LindenJiebaTokenizer(reader));
  }
}
