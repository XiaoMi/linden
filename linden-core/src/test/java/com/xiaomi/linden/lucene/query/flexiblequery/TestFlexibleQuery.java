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

import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.bql.BQLCompiler;
import com.xiaomi.linden.core.TestLindenCoreBase;
import com.xiaomi.linden.thrift.common.LindenExplanation;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenResult;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;

public class TestFlexibleQuery extends TestLindenCoreBase {
  public TestFlexibleQuery() throws Exception {
    try {
      handleRequest("{\"id\":\"0\", \"text\": \"hello world\", \"title\": \"hello lucene\"}");
      handleRequest("{\"id\":\"1\", \"text\": \"hello lucene hello world\", \"title\": \"hello world\"}");
      handleRequest("{\"id\":\"2\", \"text\": \"world hello\", \"title\": \"lucene\"}");
      handleRequest("{\"id\":\"3\", \"text\": \"hello world lucene hello\", \"title\": \"world\"}");
      lindenCore.commit();
      lindenCore.refresh();
      bqlCompiler = new BQLCompiler(lindenConfig.getSchema());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void init() throws Exception {
    LindenSchema schema = new LindenSchema().setId("id");
    schema.addToFields(new LindenFieldSchema().setName("text").setIndexed(true).setStored(true).setTokenized(true));
    schema.addToFields(new LindenFieldSchema().setName("title").setIndexed(true).setStored(true).setTokenized(true));
    lindenConfig.setSchema(schema);
  }

  static void OutputExplanation(LindenExplanation expl, String prefix) {
    System.out.println(prefix + " " + expl.getDescription());
    for (int i = 0; i < expl.getDetailsSize(); ++i) {
      OutputExplanation(expl.getDetails().get(i), prefix + "*");
    }
  }

  @Test
  public void testBasic() throws IOException {

    String bql = "SELECT * FROM LINDEN BY query is 'text:(hello world)'";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result0 = lindenCore.search(request);
    Assert.assertEquals(4, result0.getHitsSize());

    bql = "SELECT * FROM LINDEN BY flexible_query is \"hello world\" in (text) USING MODEL simplest BEGIN\n"
                 + "   float sum = 0f;\n"
                 + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
                 + "        for (int j = 0; j < getTermLength(); ++j) {\n"
                 + "            if (isMatched(i, j)) {\n"
                 + "                sum += getScore(i, j);\n"
                 + "            }\n"
                 + "        } \n"
                 + "    } \n"
                 + "    return sum;\n"
                 + "END\n"
                 + "Limit 0, 10\n";

    request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result1 = lindenCore.search(request);
    Assert.assertEquals(4, result1.getHitsSize());
    Assert.assertEquals(result0.getHits(), result1.getHits());

    // Field boost is 2
    bql = "SELECT * FROM LINDEN BY flexible_query is \"hello world\" in (text^2) USING MODEL fieldBoost1 BEGIN\n"
          + "   float sum = 0f;\n"
          + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
          + "        for (int j = 0; j < getTermLength(); ++j) {\n"
          + "            if (isMatched(i, j)) {\n"
          + "                sum += getScore(i, j);\n"
          + "            }\n"
          + "        } \n"
          + "    } \n"
          + "    return sum;\n"
          + "END\n"
          + "Limit 0, 10\n";

    request = bqlCompiler.compile(bql).getSearchRequest();
    result1 = lindenCore.search(request);
    Assert.assertEquals(4, result1.getHitsSize());
    Assert.assertEquals(result0.getHits().get(0).getScore() * 2, result1.getHits().get(0).getScore(), 0.001);
    Assert.assertEquals(result0.getHits().get(1).getScore() * 2, result1.getHits().get(1).getScore(), 0.001);
    Assert.assertEquals(result0.getHits().get(2).getScore() * 2, result1.getHits().get(2).getScore(), 0.001);
    Assert.assertEquals(result0.getHits().get(3).getScore() * 2, result1.getHits().get(3).getScore(), 0.001);


    bql = "SELECT * FROM LINDEN BY flexible_query is \"hello lucene\" in (text, title^2) USING MODEL fieldBoost2 BEGIN\n"
          + "   float sum = 0f;\n"
          + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
          + "        for (int j = 0; j < getTermLength(); ++j) {\n"
          + "            if (isMatched(i, j)) {\n"
          + "                sum += getScore(i, j);\n"
          + "            }\n"
          + "        } \n"
          + "    } \n"
          + "    return sum;\n"
          + "END\n"
          + "Limit 0, 10\n";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result1 = lindenCore.search(request);
    Assert.assertEquals(4, result1.getHitsSize());
    Assert.assertEquals("0", result1.getHits().get(0).id);
    Assert.assertEquals("2", result1.getHits().get(1).id);
    Assert.assertEquals("1", result1.getHits().get(2).id);
    Assert.assertEquals("3", result1.getHits().get(3).id);

    bql = "SELECT * FROM LINDEN BY flexible_query is \"hello lucene\" in (text, title) USING MODEL noBoost BEGIN\n"
          + "   float sum = 0f;\n"
          + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
          + "        for (int j = 0; j < getTermLength(); ++j) {\n"
          + "            if (isMatched(i, j)) {\n"
          + "                sum += getScore(i, j);\n"
          + "            }\n"
          + "        } \n"
          + "    } \n"
          + "    return sum;\n"
          + "END\n"
          + "Limit 0, 10\n";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result1 = lindenCore.search(request);
    Assert.assertEquals(4, result1.getHitsSize());
    Assert.assertEquals("0", result1.getHits().get(0).id);
    Assert.assertEquals("1", result1.getHits().get(1).id);
    Assert.assertEquals("2", result1.getHits().get(2).id);
    Assert.assertEquals("3", result1.getHits().get(3).id);


    // Term "text:world" boost is 3
    bql = "SELECT * FROM LINDEN BY query is 'text:(hello world^3)'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result0 = lindenCore.search(request);
    Assert.assertEquals(4, result0.getHitsSize());

    bql = "SELECT * FROM LINDEN BY flexible_query is \"hello world^3\" in (text) USING MODEL termBoost BEGIN\n"
          + "   float sum = 0f;\n"
          + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
          + "        for (int j = 0; j < getTermLength(); ++j) {\n"
          + "            if (isMatched(i, j)) {\n"
          + "                sum += getScore(i, j);\n"
          + "            }\n"
          + "        } \n"
          + "    } \n"
          + "    return sum;\n"
          + "END\n"
          + "Limit 0, 10\n";

    request = bqlCompiler.compile(bql).getSearchRequest();
    result1 = lindenCore.search(request);
    Assert.assertEquals(4, result1.getHitsSize());
    Assert.assertEquals(result0.getHits(), result1.getHits());

    bql = "SELECT * FROM LINDEN BY query is 'text:(hello world lucene)'";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result0 = lindenCore.search(request);
    Assert.assertEquals(4, result0.getHitsSize());
    Assert.assertEquals("1", result0.getHits().get(0).id);
    Assert.assertEquals("3", result0.getHits().get(1).id);
    Assert.assertEquals("0", result0.getHits().get(2).id);
    Assert.assertEquals("2", result0.getHits().get(3).id);

    bql = "SELECT * FROM LINDEN BY flexible_query is \"hello world lucene\" in (text) USING MODEL continuousMatchBoost BEGIN\n"
          + "   float sum = 0f;\n"
          + "   int continuousMatches = 0;\n"
          + "     for (int i = 0; i < getFieldLength(); ++i) {\n"
          + "      int lastMatechedTermIdx = Integer.MIN_VALUE;\n"
          + "      int[] lastPositions = null;\n"
          + "      int[] curPositions;\n"
          + "      for (int j = 0; j < getTermLength(); ++j) {\n"
          + "        if (isMatched(i, j)) {\n"
          + "          curPositions = positions(i, j);\n"
          + "          if (lastMatechedTermIdx + 1 == j) {\n"
          + "            for (int ii = 0; ii < lastPositions.length; ++ii)\n"
          + "              for (int jj = 0; jj < curPositions.length; ++jj) {\n"
          + "                if (lastPositions[ii] + 1 == curPositions[jj]) {\n"
          + "                  ++continuousMatches;\n"
          + "                }\n"
          + "              }\n"
          + "          }\n"
          + "          lastMatechedTermIdx = j;\n"
          + "          lastPositions = curPositions;\n"
          + "          sum += getScore(i, j);\n"
          + "        }\n"
          + "      }\n"
          + "    }\n"
          + "    sum  += continuousMatches * 0.5;\n"
          + "    return sum;\n"
          + "END\n"
          + "Limit 0, 10\n";

    request = bqlCompiler.compile(bql).getSearchRequest();
    result1 = lindenCore.search(request);
    Assert.assertEquals(4, result1.getHitsSize());
    Assert.assertEquals("3", result1.getHits().get(0).id);
    Assert.assertEquals("1", result1.getHits().get(1).id);
    Assert.assertEquals("0", result1.getHits().get(2).id);
    Assert.assertEquals("2", result1.getHits().get(3).id);

    Assert.assertEquals(1.0, result1.getHits().get(0).score - result0.getHits().get(1).score, 0.001);
    Assert.assertEquals(0.5, result1.getHits().get(1).score - result0.getHits().get(0).score, 0.001);
    Assert.assertEquals(0.5, result1.getHits().get(2).score - result0.getHits().get(2).score, 0.001);
    Assert.assertEquals(0, result1.getHits().get(3).score - result0.getHits().get(3).score, 0.001);

    bql = "SELECT * FROM LINDEN BY flexible_query is \"hello world lucene\" in (text) USING MODEL continuousMatchBoostExplained BEGIN\n"
          + "   setRootExpl(\"Explanation of continuousMatchBoostExplained model:\");\n"
          + "   float sum = 0f;\n"
          + "   for (int i = 0; i < getFieldLength(); ++i) {\n"
          + "      int continuousMatches = 0;\n"
          + "      float fieldScore = 0f;\n"
          + "      int lastMatechedTermIdx = Integer.MIN_VALUE;\n"
          + "      int[] lastPositions = null;\n"
          + "      int[] curPositions;\n"
          + "      for (int j = 0; j < getTermLength(); ++j) {\n"
          + "        if (isMatched(i, j)) {\n"
          + "          curPositions = positions(i, j);\n"
          + "          if (lastMatechedTermIdx + 1 == j) {\n"
          + "            for (int ii = 0; ii < lastPositions.length; ++ii)\n"
          + "              for (int jj = 0; jj < curPositions.length; ++jj) {\n"
          + "                if (lastPositions[ii] + 1 == curPositions[jj]) {\n"
          + "                  ++continuousMatches;\n"
          + "                }\n"
          + "              }\n"
          + "          }\n"
          + "          lastMatechedTermIdx = j;\n"
          + "          lastPositions = curPositions;\n"
          + "          float termScore = getScore(i, j);\n"
          + "          fieldScore += termScore;\n"
          + "          addTermExpl(i, j, termScore, getExpl(\"%s is matched in %s field, positions are %s\", text(i, j), field(i, j), curPositions));\n"
          + "        }\n"
          + "      }\n"
          + "      fieldScore += continuousMatches * 0.5;\n"
          + "      addFieldExpl(i, fieldScore, getExpl(\"%d continuous matches in %s field\", continuousMatches, field(i, 0)));\n"
          + "      sum += fieldScore;\n"
          + "    }\n"
          + "    return sum;\n"
          + "END\n"
          + "EXPLAIN Limit 0, 10\n";

    request = bqlCompiler.compile(bql).getSearchRequest();
    result1 = lindenCore.search(request);
    Assert.assertEquals(4, result1.getHitsSize());
    Assert.assertEquals("3", result1.getHits().get(0).id);
    Assert.assertEquals("1", result1.getHits().get(1).id);
    Assert.assertEquals("0", result1.getHits().get(2).id);
    Assert.assertEquals("2", result1.getHits().get(3).id);

    Assert.assertEquals(1.0, result1.getHits().get(0).score - result0.getHits().get(1).score, 0.001);
    Assert.assertEquals(0.5, result1.getHits().get(1).score - result0.getHits().get(0).score, 0.001);
    Assert.assertEquals(0.5, result1.getHits().get(2).score - result0.getHits().get(2).score, 0.001);
    Assert.assertEquals(0, result1.getHits().get(3).score - result0.getHits().get(3).score, 0.001);

    for (int i = 0; i < 4; ++i) {
      System.out.println("\nExplanation of Doc " +  result1.getHits().get(i).id);
      OutputExplanation(result1.getHits().get(i).getExplanation(), "*");
    }
  }

  @Test
  public void testMatchRatio() throws IOException {
    String bql =
        "SELECT * FROM LINDEN BY flexible_query is \"hello lucene\" full_match in (title) USING MODEL simplest BEGIN\n"
        + "   float sum = 0f;\n"
        + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
        + "        for (int j = 0; j < getTermLength(); ++j) {\n"
        + "            if (isMatched(i, j)) {\n"
        + "                sum += getScore(i, j);\n"
        + "            }\n"
        + "        } \n"
        + "    } \n"
        + "    return sum;\n"
        + "END\n"
        + "Limit 0, 10\n";

    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result = lindenCore.search(request);
    Assert.assertEquals(1, result.getHitsSize());
    Assert.assertEquals("0", result.getHits().get(0).id);

    bql =
        "SELECT * FROM LINDEN BY flexible_query is \"hello world lucene\" match 1 in (title) USING MODEL simplest BEGIN\n"
        + "   float sum = 0f;\n"
        + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
        + "        for (int j = 0; j < getTermLength(); ++j) {\n"
        + "            if (isMatched(i, j)) {\n"
        + "                sum += getScore(i, j);\n"
        + "            }\n"
        + "        } \n"
        + "    } \n"
        + "    return sum;\n"
        + "END\n"
        + "Limit 0, 10\n";

    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(0, result.getHitsSize());

    bql =
        "SELECT * FROM LINDEN BY flexible_query is \"hello world lucene\" match 0.5 in (title) USING MODEL simplest BEGIN\n"
        + "   float sum = 0f;\n"
        + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
        + "        for (int j = 0; j < getTermLength(); ++j) {\n"
        + "            if (isMatched(i, j)) {\n"
        + "                sum += getScore(i, j);\n"
        + "            }\n"
        + "        } \n"
        + "    } \n"
        + "    return sum;\n"
        + "END\n"
        + "Limit 0, 10\n";

    System.out.println(bql);
    request = bqlCompiler.compile(bql).getSearchRequest();
    result = lindenCore.search(request);
    Assert.assertEquals(2, result.getHitsSize());
    Assert.assertEquals("0", result.getHits().get(0).id);
    Assert.assertEquals("1", result.getHits().get(1).id);
  }

  @Test
  public void testDynamicField() throws IOException {

    String bql = "SELECT * FROM LINDEN BY query is 'text:(hello world)'";
    LindenSearchRequest request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result0 = lindenCore.search(request);
    Assert.assertEquals(4, result0.getHitsSize());

    bql = "SELECT * FROM LINDEN BY flexible_query is \"hello world\" in (text.STRING) USING MODEL simplest BEGIN\n"
          + "   float sum = 0f;\n"
          + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
          + "        for (int j = 0; j < getTermLength(); ++j) {\n"
          + "            if (isMatched(i, j)) {\n"
          + "                sum += getScore(i, j);\n"
          + "            }\n"
          + "        } \n"
          + "    } \n"
          + "    return sum;\n"
          + "END\n"
          + "Limit 0, 10\n";

    request = bqlCompiler.compile(bql).getSearchRequest();
    LindenResult result1 = lindenCore.search(request);
    Assert.assertEquals(4, result1.getHitsSize());
    Assert.assertEquals(result0.getHits(), result1.getHits());

    // Field boost is 2
    bql = "SELECT * FROM LINDEN BY flexible_query is \"hello world\" in (text.string^2) USING MODEL fieldBoost1 BEGIN\n"
          + "   float sum = 0f;\n"
          + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
          + "        for (int j = 0; j < getTermLength(); ++j) {\n"
          + "            if (isMatched(i, j)) {\n"
          + "                sum += getScore(i, j);\n"
          + "            }\n"
          + "        } \n"
          + "    } \n"
          + "    return sum;\n"
          + "END\n"
          + "Limit 0, 10\n";

    request = bqlCompiler.compile(bql).getSearchRequest();
    result1 = lindenCore.search(request);
    Assert.assertEquals(4, result1.getHitsSize());
    Assert.assertEquals(result0.getHits().get(0).getScore() * 2, result1.getHits().get(0).getScore(), 0.001);
    Assert.assertEquals(result0.getHits().get(1).getScore() * 2, result1.getHits().get(1).getScore(), 0.001);
    Assert.assertEquals(result0.getHits().get(2).getScore() * 2, result1.getHits().get(2).getScore(), 0.001);
    Assert.assertEquals(result0.getHits().get(3).getScore() * 2, result1.getHits().get(3).getScore(), 0.001);

    bql = "SELECT * FROM LINDEN BY flexible_query is \"hello lucene\" in (text.STRING, title.STRING^2) USING MODEL fieldBoost2 BEGIN\n"
        + "   float sum = 0f;\n"
        + "    for (int i = 0; i < getFieldLength(); ++i) {\n"
        + "        for (int j = 0; j < getTermLength(); ++j) {\n"
        + "            if (isMatched(i, j)) {\n"
        + "                sum += getScore(i, j);\n"
        + "            }\n"
        + "        } \n"
        + "    } \n"
        + "    return sum;\n"
        + "END\n"
        + "Limit 0, 10\n";
    request = bqlCompiler.compile(bql).getSearchRequest();
    result1 = lindenCore.search(request);
    Assert.assertEquals(4, result1.getHitsSize());
    Assert.assertEquals("0", result1.getHits().get(0).id);
    Assert.assertEquals("2", result1.getHits().get(1).id);
    Assert.assertEquals("1", result1.getHits().get(2).id);
    Assert.assertEquals("3", result1.getHits().get(3).id);
  }
}

