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

package com.xiaomi.linden.core.search.query.model;

import java.io.IOException;

import com.google.common.base.Throwables;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenScoreModel;

public class LindenScoreQuery extends CustomScoreQuery {

  private LindenSchema schema;
  private LindenScoreModel scoreModel;
  private String pluginPath;

  public LindenScoreQuery(Query subQuery, LindenScoreModel scoreModel, LindenConfig config) {
    super(subQuery);
    this.scoreModel = scoreModel;
    this.schema = config.getSchema();
    this.pluginPath = config.getPluginPath();
  }

  @Override
  public String name() {
    return "LindenScoreQuery";
  }

  @Override
  protected CustomScoreProvider getCustomScoreProvider(AtomicReaderContext context) throws IOException {
    try {
      return new LindenScoreProvider(context);
    } catch (Exception e) {
      throw new IOException(Throwables.getStackTraceAsString(e));
    }
  }

  private class LindenScoreProvider extends CustomScoreProvider {

    private LindenScoreModelStrategy scoreModelStrategy;

    public LindenScoreProvider(AtomicReaderContext context) throws Exception {
      super(context);
      scoreModelStrategy = LindenScoreModelStrategyBuilder.build(pluginPath, scoreModel, schema);
      scoreModelStrategy.preProcess(context, schema, scoreModel);
      scoreModelStrategy.init();
    }

    @Override
    public float customScore(int doc, float subQueryScore, float valSrcScores[]) throws IOException {
      return customScore(doc, subQueryScore, 1);
    }

    @Override
    public float customScore(int doc, float subQueryScore, float valSrcScore) {
      scoreModelStrategy.prepare(doc, subQueryScore, false);
      try {
        return (float) scoreModelStrategy.computeScore();
      } catch (IOException e) {
        throw new RuntimeException(Throwables.getStackTraceAsString(e));
      }
    }

    @Override
    public Explanation customExplain(int doc, Explanation subQueryExpl, Explanation valSrcExpls[]) throws IOException {
      float baseScore = subQueryExpl.getValue();
      scoreModelStrategy.prepare(doc, baseScore, true);
      float finalScore = (float) scoreModelStrategy.computeScore();
      String explStr = scoreModelStrategy.getExplanation();
      if (explStr == null) {
        explStr = "SCORE MODEL";
      }
      Explanation expl = new Explanation(finalScore, explStr);
      expl.addDetail(subQueryExpl);
      return expl;
    }
  }
}
