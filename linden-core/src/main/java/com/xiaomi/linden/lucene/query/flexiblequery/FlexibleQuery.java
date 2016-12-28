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

package com.xiaomi.linden.lucene.query.flexiblequery;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.LindenScoreModel;

public class FlexibleQuery extends Query {
  private final Analyzer analyzer;
  private final LindenScoreModel model;
  private float[] fieldBoosts;
  private boolean isFullMatch = false;
  private double matchRatio = -1;
  private boolean globalIDF = false;
  private final FlexibleTermInfo termsMatrix;
  private final LindenConfig config;

  static public class FlexibleField {
    public String field;
    public double boost = 1f;

    public FlexibleField(String field, double boost) {
      this.field = field;
      this.boost = boost;
    }
  }

  static public class FlexibleTermInfo {
    public FlexibleTerm[][] terms;
    public FlexibleTerm[][] globalTerms;

    public FlexibleTermInfo(FlexibleTerm[][] terms, FlexibleTerm[][] globalTerms) {
      this.terms = terms;
      this.globalTerms = globalTerms;
    }
  }

  static public class FlexibleTerm {
    public Term term;
    public float boost = 1f;

    public FlexibleTerm(String field, String value) {
      this(field, value, 1);
    }

    public FlexibleTerm(String field, String value, float boost) {
      term = new Term(field, value);
      this.boost = boost;
    }
  }

  static class SegToken {
    public String text;
    public float boost;

    public SegToken(String text, float boost) {
      this.text = text;
      this.boost = boost;
    }
  }

  public FlexibleQuery(List<FlexibleField> fields, String query, LindenScoreModel model,
      LindenConfig config) throws IOException {
    this(fields, new ArrayList<FlexibleField>(), query, model, config);
  }

  public FlexibleQuery(List<FlexibleField> queryFields, List<FlexibleField> globalFields, String query,
      LindenScoreModel model, LindenConfig config) throws IOException {
    this.analyzer = config.getSearchAnalyzerInstance();
    this.config = config;
    this.model = model;
    termsMatrix = parseToTerms(queryFields, globalFields, query);
    fieldBoosts = new float[queryFields.size()];
    for (int i = 0; i < queryFields.size(); ++i) {
      fieldBoosts[i] = (float) queryFields.get(i).boost;
    }
  }

  public double getMatchRatio() {
    return matchRatio;
  }

  public void setMatchRatio(double matchRatio) {
    this.matchRatio = matchRatio;
  }

  public void setFullMatch(boolean fullMatch) {
    isFullMatch = fullMatch;
  }

  public boolean enableGlobalIDF() {
    return globalIDF;
  }

  public void setGlobalIDF(boolean globalIDF) {
    this.globalIDF = globalIDF;
  }

  public FlexibleTerm[][] getTerms() {
    return termsMatrix.terms;
  }

  public FlexibleTerm[][] getGlobalTerms() {
    return termsMatrix.globalTerms;
  }

  public float[] getFieldBoosts() {
    return fieldBoosts;
  }

  public LindenConfig getConfig() {
    return config;
  }

  public LindenScoreModel getModel() {
    return model;
  }

  private FlexibleTermInfo parseToTerms(List<FlexibleField> queryFields, List<FlexibleField> globalFields,
      String content) throws IOException {
    List<SegToken> segTokens = new ArrayList<>();
    try {
      segTokens.addAll(parseWeightedTokens(content));
    } catch (IllegalArgumentException e) {
      segTokens.addAll(parseToTokens(content, 1f));
    }
    FlexibleTerm[][] terms = new FlexibleTerm[queryFields.size()][];
    FlexibleTerm[][] globalTerms = new FlexibleTerm[globalFields.size()][];
    for (int i = 0; i < queryFields.size(); ++i) {
      FlexibleField field = queryFields.get(i);
      terms[i] = new FlexibleTerm[segTokens.size()];
      for (int j = 0; j < segTokens.size(); ++j) {
        terms[i][j] = new FlexibleTerm(field.field, segTokens.get(j).text, segTokens.get(j).boost);
      }
    }
    for (int i = 0; i < globalFields.size(); ++i) {
      FlexibleField field = globalFields.get(i);
      globalTerms[i] = new FlexibleTerm[segTokens.size()];
      for (int j = 0; j < segTokens.size(); ++j) {
        globalTerms[i][j] = new FlexibleTerm(field.field, segTokens.get(j).text, segTokens.get(j).boost);
      }
    }
    return new FlexibleTermInfo(terms, globalTerms);
  }

  private Collection<? extends SegToken> parseWeightedTokens(String content) throws IOException {
    List<SegToken> segTokens = new ArrayList<>();
    QueryParser queryParser = new QueryParser("", new WhitespaceAnalyzer());
    try {
      Query query = queryParser.parse(content);
      if (query instanceof BooleanQuery) {
        for (BooleanClause clause : ((BooleanQuery) query).getClauses()) {
          if (clause.getQuery() instanceof TermQuery) {
            String text = ((TermQuery) clause.getQuery()).getTerm().bytes().utf8ToString();
            float boost = clause.getQuery().getBoost();
            List<SegToken> tokens = parseToTokens(text, boost);
            segTokens.addAll(tokens);
          } else {
            throw new IllegalArgumentException("multi weighted tokens error");
          }
        }
      } else if (query instanceof TermQuery) {
        String text = ((TermQuery) query).getTerm().bytes().utf8ToString();
        float boost = query.getBoost();
        List<SegToken> tokens = parseToTokens(text, boost);
        segTokens.addAll(tokens);
      } else {
        throw new IllegalArgumentException("illegal weighted tokens");
      }
    } catch (ParseException e) {
      throw new IllegalArgumentException("illegal argument, parse exception");
    }
    return segTokens;
  }

  private List<SegToken> parseToTokens(String content, float boost) throws IOException {
    List<SegToken> tokens = new ArrayList<>();
    TokenStream stream = analyzer.tokenStream("", new StringReader(content));
    try {
      CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);
      stream.reset();
      while (stream.incrementToken()) {
        tokens.add(new SegToken(term.toString(), boost));
      }
    } finally {
      if (stream != null) stream.close();
    }
    return tokens;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("FlexibleQuery([");
    for (int i = 0; i < termsMatrix.terms.length; ++i) {
      FlexibleTerm[] fieldTerms = termsMatrix.terms[i];
      if (fieldTerms.length != 0) {
        buffer.append(fieldTerms[0].term.field());
        if (fieldBoosts[i] != 1) {
          buffer.append('^').append(fieldBoosts[i]);
        }
      }
      if (i + 1 != termsMatrix.terms.length) buffer.append(",");
    }
    buffer.append("]:[");
    if (termsMatrix.terms.length != 0) {
      FlexibleTerm[] fieldTerms = termsMatrix.terms[0];
      if (fieldTerms.length != 0) {
        for (int i = 0; i < fieldTerms.length; ++i) {
          buffer.append(fieldTerms[i].term.text());
          if (fieldTerms[i].boost != 1) {
            buffer.append('^').append(fieldTerms[i].boost);
          }
          if (i + 1 != fieldTerms.length) buffer.append(',');
        }
      }
    }
    buffer.append("]");
    if (isFullMatch) {
      buffer.append("fullMatch");
    } else if (matchRatio > 0) {
      buffer.append("matchRatio:").append(matchRatio);
      int minMatch = Math.max(1, (int) Math.ceil(matchRatio * termsMatrix.terms[0].length));
      buffer.append(" minMatch:").append(minMatch);
    }
    buffer.append(")");
    if (getBoost() != 1) {
      buffer.append("^").append(getBoost());
    }
    return buffer.toString();
  }

  public boolean isFullMatch() {
    return isFullMatch;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new FlexibleWeight(this, searcher);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    for (int i = 0; i < termsMatrix.terms.length; ++i) {
      for (int j = 0; j < termsMatrix.terms[i].length; ++j) {
        terms.add(termsMatrix.terms[i][j].term);
      }
    }
  }
}
