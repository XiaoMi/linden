package com.xiaomi.linden.lucene.analyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;

/**
 * Created by sls on 17-6-6.
 */
public class CommonJiebaSegmenter extends LindenSegmenter {

  private JiebaSegmenter.SegMode mode;
  private JiebaSegmenter segmenter;


  public CommonJiebaSegmenter(JiebaSegmenter.SegMode mode) {
    this.mode = mode;
    initAnalyzer();
  }

  private void initAnalyzer() {
    this.segmenter = new JiebaSegmenter();
  }

  private void transform(List<SegToken> source, List<Term> dest) {
    for (SegToken seg : source) {
      Term t = new Term(seg.word, seg.startOffset, seg.endOffset);
      dest.add(t);
    }
  }

  @Override
  public List<Term> parse(String content) throws Exception {
    List<SegToken> tokens = this.segmenter.process(content, this.mode);
    List<Term> result = new ArrayList<>();
    if (tokens.size() != 0) {
      transform(tokens, result);
    }
    return result;
  }

  @Override
  public void close() throws IOException {
  }
}

