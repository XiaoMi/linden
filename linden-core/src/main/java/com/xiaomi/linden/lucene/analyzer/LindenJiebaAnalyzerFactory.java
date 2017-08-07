package com.xiaomi.linden.lucene.analyzer;

import java.io.IOException;
import java.util.Map;

import com.huaban.analysis.jieba.JiebaSegmenter;

import com.xiaomi.linden.plugin.LindenPluginFactory;

/**
 * Created by sls on 17-6-6.
 */
public class LindenJiebaAnalyzerFactory implements LindenPluginFactory<LindenJiebaAnalyzer> {

  private static final String MODE = "mode";
  private static final String USER_DICT = "user.dict";

  @Override
  public LindenJiebaAnalyzer getInstance(Map<String, String> params) throws IOException {
    String mode = params.get(MODE);
    String userDict = params.get(USER_DICT);
    JiebaSegmenter.SegMode segMode;

    if (mode != null && mode.equalsIgnoreCase("index")) {
      segMode = JiebaSegmenter.SegMode.INDEX;
    } else {
      segMode = JiebaSegmenter.SegMode.SEARCH;
    }
    return new LindenJiebaAnalyzer(segMode,userDict);
  }
}
