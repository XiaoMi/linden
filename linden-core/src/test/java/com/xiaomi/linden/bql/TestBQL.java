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

package com.xiaomi.linden.bql;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.xiaomi.linden.core.search.query.FilteredQueryConstructor;
import com.xiaomi.linden.thrift.builder.filter.LindenBooleanFilterBuilder;
import com.xiaomi.linden.thrift.builder.filter.LindenNotNullFieldFilterBuilder;
import com.xiaomi.linden.thrift.builder.filter.LindenRangeFilterBuilder;
import com.xiaomi.linden.thrift.builder.filter.LindenSpatialFilterBuilder;
import com.xiaomi.linden.thrift.builder.filter.LindenTermFilterBuilder;
import com.xiaomi.linden.thrift.builder.query.LindenBooleanQueryBuilder;
import com.xiaomi.linden.thrift.builder.query.LindenQueryBuilder;
import com.xiaomi.linden.thrift.builder.query.LindenQueryStringQueryBuilder;
import com.xiaomi.linden.thrift.builder.query.LindenRangeQueryBuilder;
import com.xiaomi.linden.thrift.common.Aggregation;
import com.xiaomi.linden.thrift.common.EarlyParam;
import com.xiaomi.linden.thrift.common.FacetDrillingType;
import com.xiaomi.linden.thrift.common.LindenBooleanClause;
import com.xiaomi.linden.thrift.common.LindenBooleanFilter;
import com.xiaomi.linden.thrift.common.LindenBooleanQuery;
import com.xiaomi.linden.thrift.common.LindenBooleanSubFilter;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenFacet;
import com.xiaomi.linden.thrift.common.LindenFacetDimAndPath;
import com.xiaomi.linden.thrift.common.LindenFacetParam;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenFilter;
import com.xiaomi.linden.thrift.common.LindenFilteredQuery;
import com.xiaomi.linden.thrift.common.LindenFlexibleQuery;
import com.xiaomi.linden.thrift.common.LindenInputParam;
import com.xiaomi.linden.thrift.common.LindenNotNullFieldFilter;
import com.xiaomi.linden.thrift.common.LindenQuery;
import com.xiaomi.linden.thrift.common.LindenQueryFilter;
import com.xiaomi.linden.thrift.common.LindenRange;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenScoreModel;
import com.xiaomi.linden.thrift.common.LindenSearchField;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenTerm;
import com.xiaomi.linden.thrift.common.LindenTermFilter;
import com.xiaomi.linden.thrift.common.LindenType;
import com.xiaomi.linden.thrift.common.LindenValue;
import com.xiaomi.linden.thrift.common.LindenWildcardQuery;
import com.xiaomi.linden.thrift.common.Operator;
import com.xiaomi.linden.thrift.common.SearchRouteParam;
import com.xiaomi.linden.thrift.common.ShardRouteParam;
import com.xiaomi.linden.thrift.common.SnippetField;
import com.xiaomi.linden.thrift.common.SnippetParam;

public class TestBQL {
  private final BQLCompiler compiler;

  public TestBQL() {
    LindenSchema lindenSchema = new LindenSchema();

    lindenSchema.addToFields(new LindenFieldSchema("id", LindenType.LONG));
    lindenSchema.addToFields(new LindenFieldSchema("title", LindenType.STRING));
    lindenSchema.addToFields(new LindenFieldSchema("rank", LindenType.DOUBLE));
    lindenSchema.addToFields(new LindenFieldSchema("content", LindenType.STRING));
    lindenSchema.addToFields(new LindenFieldSchema("color", LindenType.FACET));
    lindenSchema.addToFields(new LindenFieldSchema("shape", LindenType.FACET));
    lindenSchema.addToFields(new LindenFieldSchema("group", LindenType.FACET));
    compiler = new BQLCompiler(lindenSchema);
  }


  @Test
  public void testEqualPredicate() {
    String bql = "select * from linden where title = \"qq\" and id = 211 limit 10, 50";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getFilter().isSetBooleanFilter());
    LindenBooleanFilter booleanFilter = lindenRequest.getFilter().getBooleanFilter();
    Assert.assertEquals(2, booleanFilter.getFiltersSize());
    Assert.assertEquals(LindenBooleanClause.MUST, booleanFilter.getFilters().get(0).getClause());
    Assert.assertEquals(new LindenTerm("title", "qq"),
                        booleanFilter.getFilters().get(0).getFilter().getTermFilter().getTerm());
    Assert.assertEquals(LindenBooleanClause.MUST, booleanFilter.getFilters().get(1).getClause());
    Assert.assertEquals(new LindenRange("id", LindenType.LONG, true, true).setStartValue("211").setEndValue("211"),
                        booleanFilter.getFilters().get(1).getFilter().getRangeFilter().getRange());
    Assert.assertEquals(10, lindenRequest.getOffset());
    Assert.assertEquals(50, lindenRequest.getLength());

    bql = "delete from default where title = \"q\"\"q\" and id = 211";
    LindenDeleteRequest deleteRequest = compiler.compile(bql).getDeleteRequest();
    Assert.assertTrue(deleteRequest.getQuery().isSetFilteredQuery());
    Assert.assertTrue(deleteRequest.getQuery().getFilteredQuery().isSetLindenFilter());
    Assert.assertTrue(deleteRequest.getQuery().getFilteredQuery().getLindenFilter().isSetBooleanFilter());
    booleanFilter = deleteRequest.getQuery().getFilteredQuery().getLindenFilter().getBooleanFilter();
    Assert.assertEquals(2, booleanFilter.getFiltersSize());
    Assert.assertEquals(LindenBooleanClause.MUST, booleanFilter.getFilters().get(0).getClause());
    Assert.assertEquals(new LindenTerm("title", "q\"q"),
                        booleanFilter.getFilters().get(0).getFilter().getTermFilter().getTerm());
    Assert.assertEquals(LindenBooleanClause.MUST, booleanFilter.getFilters().get(1).getClause());
    Assert.assertEquals(new LindenRange("id", LindenType.LONG, true, true).setStartValue("211").setEndValue("211"),
                        booleanFilter.getFilters().get(1).getFilter().getRangeFilter().getRange());

    bql = "delete from default where title = $a and id = 211";
    deleteRequest = compiler.compile(bql).getDeleteRequest();
    Assert.assertTrue(deleteRequest.getQuery().isSetFilteredQuery());
    Assert.assertTrue(deleteRequest.getQuery().getFilteredQuery().isSetLindenFilter());
    Assert.assertTrue(deleteRequest.getQuery().getFilteredQuery().getLindenFilter().isSetRangeFilter());

    bql = "select * from linden where dynmaicTitle.STRING = \"qq\" and dynamicId.int = 211 limit 10, 50";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getFilter().isSetBooleanFilter());
    booleanFilter = lindenRequest.getFilter().getBooleanFilter();
    Assert.assertEquals(2, booleanFilter.getFiltersSize());
    Assert.assertEquals(LindenBooleanClause.MUST, booleanFilter.getFilters().get(0).getClause());
    Assert.assertEquals(new LindenTerm("dynmaicTitle", "qq"),
                        booleanFilter.getFilters().get(0).getFilter().getTermFilter().getTerm());
    Assert.assertEquals(LindenBooleanClause.MUST, booleanFilter.getFilters().get(1).getClause());
    Assert.assertEquals(
        new LindenRange("dynamicId", LindenType.INTEGER, true, true).setStartValue("211").setEndValue("211"),
        booleanFilter.getFilters().get(1).getFilter().getRangeFilter().getRange());
    Assert.assertEquals(10, lindenRequest.getOffset());
    Assert.assertEquals(50, lindenRequest.getLength());

    bql = "select * from linden by title = \"qq\" subBoost by 0.5 and id = 211 andDisableCoord limit 10, 50";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.isSetFilter());
    Assert.assertTrue(lindenRequest.getQuery().isSetBooleanQuery());
    LindenBooleanQuery query = lindenRequest.getQuery().getBooleanQuery();
    Assert.assertTrue(query.isDisableCoord());
    Assert.assertEquals(2, query.getQueriesSize());
    Assert.assertEquals(LindenBooleanClause.MUST, query.getQueries().get(0).clause);
    Assert.assertEquals(new LindenTerm("title", "qq"),
                        query.getQueries().get(0).getQuery().getTermQuery().getTerm());
    Assert.assertEquals(0.5,
                        query.getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(LindenBooleanClause.MUST, query.getQueries().get(1).clause);
    Assert.assertEquals(new LindenRange("id", LindenType.LONG, true, true).setStartValue("211").setEndValue("211"),
                        query.getQueries().get(1).getQuery().getRangeQuery().getRange());
    Assert.assertEquals(10, lindenRequest.getOffset());
    Assert.assertEquals(50, lindenRequest.getLength());
  }

  @Test
  public void testScoreModel() {
    String bql = "select * from linden where title = 'qq' " +
        "using score model test (Float link = 1)" +
        "   begin" +
        "     return score();" +
        "   end";

    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(new LindenTerm("title", "qq"), lindenRequest.getFilter().getTermFilter().getTerm());
    LindenScoreModel lindenScoreModel = new LindenScoreModel().setName("test").setFunc("return score();");
    lindenScoreModel.addToParams(new LindenInputParam("link").setValue(new LindenValue().setDoubleValue(1)));
    Assert.assertTrue(lindenRequest.getQuery().isSetMatchAllQuery());
    Assert.assertEquals(lindenScoreModel, lindenRequest.getQuery().getScoreModel());

    bql = "select * from linden where title = 'qq' " +
        "using score model override test (Float link = 1)" +
        "   begin" +
        "     return score();" +
        "   end";

    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(new LindenTerm("title", "qq"), lindenRequest.getFilter().getTermFilter().getTerm());
    lindenScoreModel = new LindenScoreModel().setName("test").setFunc("return score();").setOverride(true);
    lindenScoreModel.addToParams(new LindenInputParam("link").setValue(new LindenValue().setDoubleValue(1)));
    Assert.assertTrue(lindenRequest.getQuery().isSetMatchAllQuery());
    Assert.assertEquals(lindenScoreModel, lindenRequest.getQuery().getScoreModel());

    // list input params.
    bql = "select * from linden where title = 'qq' " +
        "using score model test (Float link = [1, 2], Integer pos = [3, 4], Map<String,Double> kv = {\"a\":1, \"b\":2})"
        +
        "   begin" +
        "     return score();" +
        "   end";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    lindenScoreModel = new LindenScoreModel().setName("test").setFunc("return score();");
    LindenInputParam inputParam = new LindenInputParam("link").setValue(new LindenValue());
    inputParam.getValue().addToDoubleValues(1);
    inputParam.getValue().addToDoubleValues(2);
    lindenScoreModel.addToParams(inputParam);
    LindenInputParam inputParam2 = new LindenInputParam("pos").setValue(new LindenValue());
    inputParam2.getValue().addToLongValues(3);
    inputParam2.getValue().addToLongValues(4);
    lindenScoreModel.addToParams(inputParam2);
    LindenInputParam inputParam3 = new LindenInputParam("kv").setValue(new LindenValue());
    inputParam3.getValue().putToMapValue(new LindenValue().setStringValue("a"), new LindenValue().setDoubleValue(1));
    inputParam3.getValue().putToMapValue(new LindenValue().setStringValue("b"), new LindenValue().setDoubleValue(2));
    lindenScoreModel.addToParams(inputParam3);
    Assert.assertEquals(lindenScoreModel, lindenRequest.getQuery().getScoreModel());
  }

  @Test
  public void testFlexibleQuery() {
    String bql = "select * from linden by flexible_query is 'test' match 1 in (title, name^0.9)\n" +
        "using model test (Float a = 1, Long b = 2)\n" +
        "begin\n" +
        "    return 1f;\n" +
        "end\n" +
        "where id = 231\n";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getQuery().isSetFlexQuery());
    LindenFlexibleQuery lindenFlexibleQuery = new LindenFlexibleQuery().setQuery("test")
        .setFields(Arrays.asList(new LindenSearchField("title"), new LindenSearchField("name").setBoost(0.9))).setModel(
            new LindenScoreModel().setName("test").setFunc("return 1f;").setParams(Arrays
                .asList(new LindenInputParam("a").setValue(new LindenValue().setDoubleValue(1)),
                    new LindenInputParam("b").setValue(new LindenValue().setLongValue(2))))).setMatchRatio(1);
    Assert.assertEquals(lindenFlexibleQuery, lindenRequest.getQuery().getFlexQuery());

    // test full match
    bql = "select * from linden by flexible_query is 'test' full_match in (title, name^0.9)\n" +
        "using model test (Float a = 1, Long b = 2)\n" +
        "begin\n" +
        "    return 1f;\n" +
        "end\n" +
        "where id = 231\n";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getQuery().isSetFlexQuery());
    lindenFlexibleQuery = new LindenFlexibleQuery().setQuery("test")
        .setFields(Arrays.asList(new LindenSearchField("title"), new LindenSearchField("name").setBoost(0.9)))
        .setFullMatch(true).setModel(new LindenScoreModel().setName("test").setFunc("return 1f;").setParams(Arrays
            .asList(new LindenInputParam("a").setValue(new LindenValue().setDoubleValue(1)),
                new LindenInputParam("b").setValue(new LindenValue().setLongValue(2)))));
    Assert.assertEquals(lindenFlexibleQuery, lindenRequest.getQuery().getFlexQuery());
  }

  @Test
  public void testSpatialFilter() throws InterruptedException {
    String bql = "select * from linden where distance(116.7, 35.3) in 10";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenFilter spatialFilter = LindenSpatialFilterBuilder.buildSpatialParam(35.3, 116.7, 10);

    Assert.assertEquals(spatialFilter, lindenRequest.getFilter());
  }

  @Test
  public void testMultiSelect() throws InterruptedException {
    String bql = "select * from linden where title = 'qq' and id = 211 |"
        + " select * from linden where title = 'qm' and id = 211";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getQuery().isSetDisMaxQuery());
    Assert.assertEquals(2, lindenRequest.getQuery().getDisMaxQuery().getQueriesSize());
  }

  @Test
  public void testBoostBy() {
    String bql = "select * from linden where title = 'qq' and id = 211 boost by 0.5";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(0.5, lindenRequest.getQuery().getBoost(), 0.001);

    bql = "select * from linden by title = 'test' subBoost by 0.8 and id = 211 subBoost by 2 boost by 0.5";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(0.5, lindenRequest.getQuery().getBoost(), 0.001);
    Assert.assertEquals(0.8, lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(2, lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBoost(), 0.001);

    bql = "select * from linden by title = 'test' subBoost by $b1 and id = 211 subBoost by 2 boost by $b2";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(1, lindenRequest.getQuery().getBoost(), 0.001);
    Assert.assertEquals(1, lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(2, lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBoost(), 0.001);

    bql = "select * from linden by title = 'test' subBoost by 0.8 and id in (200, 201) subBoost by 2";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(0.8, lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(2, lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBoost(), 0.001);

    bql =
        "select * from linden by title = 'test' subBoost by 0.8 and id = 211 subBoost by 2 andBoost by 3 or query is 'content:test' subBoost by 5 boost by 0.5";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(0.5, lindenRequest.getQuery().getBoost(), 0.001);
    Assert.assertEquals(3, lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(0.8, lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBooleanQuery()
        .getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(2, lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBooleanQuery()
        .getQueries().get(1).getQuery().getBoost(), 0.001);
    Assert.assertEquals(5, lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBoost(), 0.001);

    bql =
        "select * from linden by title = 'test' subBoost by 0.8 and (id = 211 subBoost by 2 or query is 'content:test' subBoost by 5 orBoost by 3)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(1, lindenRequest.getQuery().getBoost(), 0.001);
    Assert.assertEquals(0.8, lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(3, lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBoost(), 0.001);
    Assert.assertEquals(2, lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBooleanQuery()
        .getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(5, lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBooleanQuery()
        .getQueries().get(1).getQuery().getBoost(), 0.001);

    bql =
        "select * from linden by (title = 'test' subBoost by 0.8 and query is 'body:test' andBoost by 6) and (id = 211 subBoost by 2 or query is 'content:test' subBoost by 5 orBoost by 3)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(1, lindenRequest.getQuery().getBoost(), 0.001);
    Assert.assertEquals(6, lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(0.8, lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBooleanQuery()
        .getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(1, lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBooleanQuery()
        .getQueries().get(1).getQuery().getBoost(), 0.001);
    Assert.assertEquals(3, lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBoost(), 0.001);
    Assert.assertEquals(2, lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBooleanQuery()
        .getQueries().get(0).getQuery().getBoost(), 0.001);
    Assert.assertEquals(5, lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBooleanQuery()
        .getQueries().get(1).getQuery().getBoost(), 0.001);
  }

  @Test
  public void testIgnoreClause() {
    String bql = "select * from linden where title = $a or id = 211";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getFilter().isSetRangeFilter());
    bql = "select * from linden where title = $b and id = 211";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getFilter().isSetRangeFilter());

    bql = "select * from linden where title = \"test\" and id = $id";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getFilter().isSetTermFilter());

    // distance ignore
    bql = "select * from linden where title = \"ignore\" or (distance($a, $b) in 1 and id = $c)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getFilter().isSetTermFilter());

    // limit ignore
    bql = "select * from linden where title = \"lucene\" limit $from, $size";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getFilter().isSetTermFilter());

    // explain ignore
    bql = "select * from linden where title = \"lucene\" explain $e";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.isExplain());

    // source ignore
    bql = "select * from linden where title = \"lucene\" source $a";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.isSource());
  }

  @Test
  public void testMatchAll() {
    String bql = "select * from linden";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getQuery().isSetMatchAllQuery());
  }

  @Test
  public void testSource() {
    String bql = "select * from linden source explain";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.isSource());
    Assert.assertTrue(lindenRequest.isExplain());
  }

  @Test
  public void testQueryPredicate() {
    String bql = "select * from linden where query is 'title:a' and id = 233";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getFilter().isSetBooleanFilter());
    Assert.assertEquals(2, lindenRequest.getFilter().getBooleanFilter().getFilters().size());
    Assert.assertTrue(lindenRequest.getFilter().getBooleanFilter().getFilters().get(0).getFilter().isSetQueryFilter());

    bql = "select * from linden by query is 'abc' and query is 'def' where id = 1 or id = 3";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getQuery().isSetBooleanQuery());
    Assert.assertEquals(2, lindenRequest.getQuery().getBooleanQuery().getQueriesSize());
    Assert.assertTrue(lindenRequest.getFilter().isSetBooleanFilter());
    Assert.assertEquals(2, lindenRequest.getFilter().getBooleanFilter().getFilters().size());

    bql = "select * from linden by query is 'title:abc bcd' disableCoord OP(AND)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenQueryStringQueryBuilder builder = new LindenQueryStringQueryBuilder();
    builder.setQuery("title:abc bcd").setOperator(Operator.AND);
    builder.setDisableCoord(true);
    LindenQuery expected = builder.build();
    Assert.assertEquals(expected, lindenRequest.getQuery());
  }

  @Test
  public void testInPredicate() {
    String bql = "select * from linden where id in (1, 2, 3)";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenBooleanFilterBuilder filterBuilder = new LindenBooleanFilterBuilder();
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "1", "1", true, true), LindenBooleanClause.SHOULD);
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "2", "2", true, true), LindenBooleanClause.SHOULD);
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "3", "3", true, true), LindenBooleanClause.SHOULD);
    LindenFilter expected = filterBuilder.build();
    Assert.assertEquals(expected, lindenRequest.getFilter());

    bql = "select * from linden where id not in (1, 2, 3)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    filterBuilder = new LindenBooleanFilterBuilder();
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "1", "1", true, true), LindenBooleanClause.MUST_NOT);
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "2", "2", true, true), LindenBooleanClause.MUST_NOT);
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "3", "3", true, true), LindenBooleanClause.MUST_NOT);
    expected = filterBuilder.build();
    Assert.assertEquals(expected, lindenRequest.getFilter());

    bql = "select * from linden where id in (1) except (2, 3)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    filterBuilder = new LindenBooleanFilterBuilder();
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "1", "1", true, true), LindenBooleanClause.SHOULD);
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "2", "2", true, true), LindenBooleanClause.MUST_NOT);
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "3", "3", true, true), LindenBooleanClause.MUST_NOT);
    expected = filterBuilder.build();
    Assert.assertEquals(expected, lindenRequest.getFilter());

    bql = "select * from linden where dy.long in (1) except (2, 3)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    filterBuilder = new LindenBooleanFilterBuilder();
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("dy", LindenType.LONG, "1", "1", true, true), LindenBooleanClause.SHOULD);
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("dy", LindenType.LONG, "2", "2", true, true), LindenBooleanClause.MUST_NOT);
    filterBuilder.addFilter(LindenRangeFilterBuilder.buildRangeFilter("dy", LindenType.LONG, "3", "3", true, true), LindenBooleanClause.MUST_NOT);
    expected = filterBuilder.build();
    Assert.assertEquals(expected, lindenRequest.getFilter());

    bql = "select * from linden by id in (1) except (2, 3)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenBooleanQueryBuilder queryBuilder = new LindenBooleanQueryBuilder();
    queryBuilder.addQuery(LindenRangeQueryBuilder.buildRangeQuery("id", LindenType.LONG, "1", "1", true, true), LindenBooleanClause.SHOULD);
    queryBuilder.addQuery(LindenRangeQueryBuilder.buildRangeQuery("id", LindenType.LONG, "2", "2", true, true), LindenBooleanClause.MUST_NOT);
    queryBuilder.addQuery(LindenRangeQueryBuilder.buildRangeQuery("id", LindenType.LONG, "3", "3", true, true), LindenBooleanClause.MUST_NOT);
    LindenQuery query = queryBuilder.build();
    Assert.assertEquals(query, lindenRequest.getQuery());

    bql = "select * from linden by id in ($a) except (2, $b)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    queryBuilder = new LindenBooleanQueryBuilder();
    queryBuilder.addQuery(LindenRangeQueryBuilder.buildRangeQuery("id", LindenType.LONG, "2", "2", true, true), LindenBooleanClause.MUST_NOT);
    query = queryBuilder.build();
    Assert.assertEquals(query, lindenRequest.getQuery());
  }

  @Test
  public void testBetweenPred() throws Exception {
    String bql = "SELECT * FROM linden WHERE id BETWEEN 2000 AND 2001";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenFilter filter = LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "2000", "2001", true, true);
    Assert.assertEquals(filter, lindenRequest.getFilter());

    bql = "SELECT * FROM linden WHERE title BETWEEN 'black' AND \"yellow\"";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    filter = LindenRangeFilterBuilder.buildRangeFilter("title", LindenType.STRING, "black", "yellow", true, true);
    Assert.assertEquals(filter, lindenRequest.getFilter());

    bql = "SELECT * FROM linden WHERE id NOT BETWEEN 2000 AND 2002";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenFilter filter1 = LindenRangeFilterBuilder
        .buildRangeFilter("id", LindenType.LONG, null, "2000", false, false);
    LindenFilter filter2 = LindenRangeFilterBuilder
        .buildRangeFilter("id", LindenType.LONG, "2002", null, false, false);
    LindenBooleanFilter booleanFilter = new LindenBooleanFilter();
    booleanFilter.addToFilters(new LindenBooleanSubFilter().setFilter(filter1)
        .setClause(LindenBooleanClause.SHOULD));
    booleanFilter.addToFilters(new LindenBooleanSubFilter().setFilter(filter2)
        .setClause(LindenBooleanClause.SHOULD));
    LindenFilter expectedFilter = new LindenFilter().setBooleanFilter(booleanFilter);
    Assert.assertEquals(expectedFilter, lindenRequest.getFilter());

    bql = "SELECT * FROM linden WHERE title BETWEEN $a AND $b";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(null, lindenRequest.getFilter());

    bql = "SELECT * FROM linden WHERE title BETWEEN 'black' AND \"yellow\"";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    filter = LindenRangeFilterBuilder.buildRangeFilter("title", LindenType.STRING, "black", "yellow", true, true);
    Assert.assertEquals(filter, lindenRequest.getFilter());

    bql = "SELECT * FROM linden by id NOT BETWEEN 2000 AND 2002";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenQuery query1 = LindenRangeQueryBuilder
        .buildRangeQuery("id", LindenType.LONG, null, "2000", false, false);
    LindenQuery query2 = LindenRangeQueryBuilder
        .buildRangeQuery("id", LindenType.LONG, "2002", null, false, false);
    LindenBooleanQueryBuilder builder = new LindenBooleanQueryBuilder();
    builder.addQuery(query1, LindenBooleanClause.SHOULD);
    builder.addQuery(query2, LindenBooleanClause.SHOULD);
    Assert.assertEquals(builder.build(), lindenRequest.getQuery());

    bql = "SELECT * FROM linden by title BETWEEN 'black' AND \"yel\"\"low\"";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenQuery query = LindenRangeQueryBuilder.buildRangeQuery("title", LindenType.STRING, "black", "yel\"low", true, true);
    Assert.assertEquals(query, lindenRequest.getQuery());
  }

  @Test
  public void testRangePred() throws Exception {
    String bql = "SELECT * FROM linden WHERE id > 1999";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenFilter expectedFilter = LindenRangeFilterBuilder
        .buildRangeFilter("id", LindenType.LONG, "1999", null, false, false);
    Assert.assertEquals(expectedFilter, lindenRequest.getFilter());

    bql = "SELECT * FROM linden  WHERE id <= 2000";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    expectedFilter = LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, null, "2000", false, true);
    Assert.assertEquals(expectedFilter, lindenRequest.getFilter());

    bql = "SELECT * FROM linden WHERE id <= $e";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(null, lindenRequest.getFilter());

    bql = "SELECT * FROM linden by id <= 2000";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenQuery query = LindenRangeQueryBuilder.buildRangeQuery("id", LindenType.LONG, null, "2000", false, true);
    Assert.assertEquals(query, lindenRequest.getQuery());

    bql = "SELECT * FROM linden by id <= 2000 and id > 1000 subBoost by 0.5";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenQuery query1 = LindenRangeQueryBuilder.buildRangeQuery("id", LindenType.LONG, null, "2000", false, true);
    LindenQuery query2 = LindenRangeQueryBuilder.buildRangeQuery("id", LindenType.LONG, "1000", null, false, false);
    query2.setBoost(0.5);
    LindenBooleanQueryBuilder builder = new LindenBooleanQueryBuilder();
    builder.addQuery(query1, LindenBooleanClause.MUST);
    builder.addQuery(query2, LindenBooleanClause.MUST);
    Assert.assertEquals(builder.build(), lindenRequest.getQuery());

    bql = "SELECT * FROM linden WHERE color > 're''d'";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    expectedFilter = LindenRangeFilterBuilder.buildRangeFilter("color", LindenType.FACET, "re'd", null, false, false);
    Assert.assertEquals(expectedFilter, lindenRequest.getFilter());
  }

  @Test
  public void testNotEqualPred() throws Exception {
    String bql = "SELECT * FROM linden  WHERE title <> 'red'";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenBooleanFilter booleanFilter = new LindenBooleanFilter();
    LindenFilter termFilter = LindenTermFilterBuilder.buildTermFilter("title", "red");
    booleanFilter.addToFilters(
        new LindenBooleanSubFilter().setFilter(termFilter).setClause(LindenBooleanClause.MUST_NOT));
    Assert.assertEquals(booleanFilter, lindenRequest.getFilter().getBooleanFilter());

    bql = "SELECT * FROM linden  WHERE title <> 're''d'";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    booleanFilter = new LindenBooleanFilter();
    termFilter = LindenTermFilterBuilder.buildTermFilter("title", "re'd");
    booleanFilter.addToFilters(
        new LindenBooleanSubFilter().setFilter(termFilter).setClause(LindenBooleanClause.MUST_NOT));
    Assert.assertEquals(booleanFilter, lindenRequest.getFilter().getBooleanFilter());

    bql = "SELECT * FROM linden  WHERE id <> 2000";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    booleanFilter = new LindenBooleanFilter();
    LindenFilter rangeFilter = LindenRangeFilterBuilder.buildRangeFilter("id", LindenType.LONG, "2000", "2000", true,
                                                                         true);
    booleanFilter.addToFilters(
        new LindenBooleanSubFilter().setFilter(rangeFilter)
            .setClause(LindenBooleanClause.MUST_NOT));
    Assert.assertEquals(booleanFilter, lindenRequest.getFilter().getBooleanFilter());

    bql = "SELECT * FROM linden WHERE id <> $id";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(null, lindenRequest.getFilter());

    bql = "SELECT * FROM linden BY title <> 're''d'";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenBooleanQueryBuilder builder = new LindenBooleanQueryBuilder();
    builder.addQuery(LindenRangeQueryBuilder.buildMatchAllQuery(), LindenBooleanClause.MUST);
    builder.addQuery(LindenRangeQueryBuilder.buildTermQuery("title", "re'd"), LindenBooleanClause.MUST_NOT);
    Assert.assertEquals(builder.build(), lindenRequest.getQuery());

    bql = "SELECT * FROM linden BY id <> $id";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(LindenQueryBuilder.buildMatchAllQuery(), lindenRequest.getQuery());
  }

  @Test
  public void testNullPredicate() throws Exception {
    String bql = "SELECT * FROM linden  WHERE id IS NOT NULL";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenFilter filter = LindenNotNullFieldFilterBuilder.buildNotNullFieldFilterBuilder("id", false);
    Assert.assertEquals(filter, lindenRequest.getFilter());

    bql = "SELECT * FROM linden WHERE title IS NULL";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    filter = LindenNotNullFieldFilterBuilder.buildNotNullFieldFilterBuilder("title", true);
    Assert.assertEquals(filter, lindenRequest.getFilter());

    bql = "SELECT * FROM linden by title IS NULL";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    filter = LindenNotNullFieldFilterBuilder.buildNotNullFieldFilterBuilder("title", true);
    LindenQuery query = LindenQueryBuilder.buildFilteredQuery(LindenQueryBuilder.buildMatchAllQuery(), filter);
    Assert.assertEquals(query, lindenRequest.getQuery());
  }

  @Test
  public void testLikePredicate() throws Exception {
    String bql = "SELECT * FROM linden WHERE title LIKE 's?d*'";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenFilter filter = new LindenFilter()
        .setQueryFilter(new LindenQueryFilter().setQuery(new LindenQuery()
            .setWildcardQuery(new LindenWildcardQuery().setQuery("s?d*").setField("title"))));
    Assert.assertEquals(filter, lindenRequest.getFilter());

    bql = "SELECT * FROM linden WHERE title NOT LIKE 'bl*'";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenQuery query = new LindenQuery();
    query.setWildcardQuery(new LindenWildcardQuery().setQuery("bl*").setField("title"));
    filter = new LindenFilter().setQueryFilter(new LindenQueryFilter().setQuery(query));
    LindenBooleanFilter booleanFilter = new LindenBooleanFilter();
    booleanFilter.addToFilters(
        new LindenBooleanSubFilter().setFilter(filter).setClause(LindenBooleanClause.MUST_NOT));
    Assert.assertEquals(booleanFilter, lindenRequest.getFilter().getBooleanFilter());

    bql = "SELECT * FROM linden WHERE title NOT LIKE $title";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(null, lindenRequest.getFilter());

    bql = "SELECT * FROM linden WHERE title LIKE $title";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(null, lindenRequest.getFilter());

    bql = "SELECT * FROM linden BY title LIKE 'sed*'";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    query = new LindenQuery();
    query.setWildcardQuery(new LindenWildcardQuery().setQuery("sed*").setField("title"));
    Assert.assertEquals(query, lindenRequest.getQuery());

    bql = "SELECT * FROM linden BY title NOT LIKE  'sed*'";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenBooleanQueryBuilder builder = new LindenBooleanQueryBuilder();
    builder.addQuery(LindenRangeQueryBuilder.buildMatchAllQuery(), LindenBooleanClause.MUST);
    builder.addQuery(new LindenQuery().setWildcardQuery(new LindenWildcardQuery().setQuery("sed*").setField("title")),
                     LindenBooleanClause.MUST_NOT);
    Assert.assertEquals(builder.build(), lindenRequest.getQuery());

    bql = "SELECT * FROM linden BY title LIKE 'sed*' subBoost by 3";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    query = new LindenQuery();
    query.setWildcardQuery(new LindenWildcardQuery().setQuery("sed*").setField("title"));
    query.setBoost(3);
    Assert.assertEquals(query, lindenRequest.getQuery());
  }

  @Test
  public void testSnippet() {
    String bql = "SELECT * FROM linden QUERY title LIKE 'sed*' snippet title, content";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    SnippetParam expected = new SnippetParam();
    expected.addToFields(new SnippetField("title"));
    expected.addToFields(new SnippetField("content"));
    Assert.assertEquals(expected, lindenRequest.getSnippetParam());
  }

  @Test
  public void testInTop() {
    String bql = "SELECT * FROM linden where title = 'sed' in top 500";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    EarlyParam earlyParam = new EarlyParam();
    earlyParam.setMaxNum(500);
    Assert.assertEquals(earlyParam, lindenRequest.getEarlyParam());

    bql = "SELECT * FROM linden where title = 'sed' in top $num";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.isSetEarlyParam());

    bql = "SELECT * FROM linden where title = 'sed' route by (0, 1, 2) in top 500, 3, replica_key '123'";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.isSetEarlyParam());
    Assert.assertEquals(4, lindenRequest.getRouteParam().getShardParamsSize());
    Assert.assertEquals(earlyParam, lindenRequest.getRouteParam().getShardParams().get(0).getEarlyParam());
    Assert.assertEquals(0, lindenRequest.getRouteParam().getShardParams().get(0).getShardId());
    Assert.assertEquals(earlyParam, lindenRequest.getRouteParam().getShardParams().get(1).getEarlyParam());
    Assert.assertEquals(1, lindenRequest.getRouteParam().getShardParams().get(1).getShardId());
    Assert.assertEquals(earlyParam, lindenRequest.getRouteParam().getShardParams().get(2).getEarlyParam());
    Assert.assertEquals(2, lindenRequest.getRouteParam().getShardParams().get(2).getShardId());
    Assert.assertFalse(lindenRequest.getRouteParam().getShardParams().get(3).isSetEarlyParam());
    Assert.assertEquals(3, lindenRequest.getRouteParam().getShardParams().get(3).getShardId());
    Assert.assertEquals("123", lindenRequest.getRouteParam().getReplicaRouteKey());
  }

  @Test
  public void testRoute() {
    String bql = "SELECT * FROM linden where title = 'sed' route by 0, 1, 2, replica_key '12345'";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    SearchRouteParam routeParam = new SearchRouteParam();
    routeParam.addToShardParams(new ShardRouteParam(0));
    routeParam.addToShardParams(new ShardRouteParam(1));
    routeParam.addToShardParams(new ShardRouteParam(2));
    routeParam.setReplicaRouteKey("12345");
    Assert.assertEquals(routeParam, lindenRequest.getRouteParam());

    bql = "SELECT * FROM linden where title = 'sed' route by 0 in top 500, 1, 2, $a, $shard in top 500, 3 in top $max";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    routeParam = new SearchRouteParam();
    routeParam.addToShardParams(new ShardRouteParam(0).setEarlyParam(new EarlyParam(500)));
    routeParam.addToShardParams(new ShardRouteParam(1));
    routeParam.addToShardParams(new ShardRouteParam(2));
    routeParam.addToShardParams(new ShardRouteParam(3));
    Assert.assertEquals(routeParam, lindenRequest.getRouteParam());

    bql = "SELECT * FROM linden where title = 'sed' route by (0,1) in top 10000, ($shard) in top 1000, 2";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    routeParam = new SearchRouteParam();
    routeParam.addToShardParams(new ShardRouteParam(0).setEarlyParam(new EarlyParam().setMaxNum(10000)));
    routeParam.addToShardParams(new ShardRouteParam(1).setEarlyParam(new EarlyParam().setMaxNum(10000)));
    routeParam.addToShardParams(new ShardRouteParam(2));
    Assert.assertEquals(routeParam, lindenRequest.getRouteParam());
  }

  @Test
  public void testReplicaRouteKey() {
    String bql = "select * from linden where title = 'sed' route by replica_key 'nihao'";
    LindenSearchRequest request = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals("nihao", request.getRouteParam().getReplicaRouteKey());
  }

  @Test
  public void testSelections() {
    String bql = "SELECT title, rank FROM linden where title = 'sed' route by 0, 1, 2";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(2, lindenRequest.getSourceFieldsSize());
    Assert.assertEquals("title", lindenRequest.getSourceFields().get(0));
    Assert.assertEquals("rank", lindenRequest.getSourceFields().get(1));
  }

  @Test
  public void testFacetBrowsing() {
    String bql = "SELECT title, rank FROM linden browse by color";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals("title", lindenRequest.getSourceFields().get(0));
    Assert.assertEquals("rank", lindenRequest.getSourceFields().get(1));

    LindenFacet facetRequest = new LindenFacet();
    facetRequest.addToFacetParams(
        new LindenFacetParam().setFacetDimAndPath(new LindenFacetDimAndPath().setDim("color")));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());

    bql = "SELECT * FROM linden browse by color(5)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    facetRequest = new LindenFacet();
    facetRequest.addToFacetParams(
        new LindenFacetParam().setTopN(5).setFacetDimAndPath(new LindenFacetDimAndPath().setDim("color")));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());

    bql = "SELECT * FROM linden browse by color(6, \"red\")";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    facetRequest = new LindenFacet();
    facetRequest.addToFacetParams(
        new LindenFacetParam().setTopN(6).setFacetDimAndPath(
            new LindenFacetDimAndPath().setDim("color").setPath("red")));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());

    bql = "SELECT * FROM linden browse by color(6, 'light/gray/white')";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    facetRequest = new LindenFacet();
    facetRequest.addToFacetParams(
        new LindenFacetParam().setTopN(6).setFacetDimAndPath(
            new LindenFacetDimAndPath().setDim("color").setPath("light/gray/white")));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());

    bql = "SELECT * FROM linden browse by color(6, 'light/gray/white'), color(5, 'red'),  shape(10, 'rectangle')";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    facetRequest = new LindenFacet();
    facetRequest.addToFacetParams(
        new LindenFacetParam().setTopN(6).setFacetDimAndPath(
            new LindenFacetDimAndPath().setDim("color").setPath("light/gray/white")));
    facetRequest.addToFacetParams(
        new LindenFacetParam().setTopN(5).setFacetDimAndPath(
            new LindenFacetDimAndPath().setDim("color").setPath("red")));
    facetRequest.addToFacetParams(
        new LindenFacetParam().setTopN(10).setFacetDimAndPath(
            new LindenFacetDimAndPath().setDim("shape").setPath("rectangle")));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());
  }

  @Test
  public void testFacetDrilling() {
    String bql = "SELECT title, rank FROM linden drill down color";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals("title", lindenRequest.getSourceFields().get(0));
    Assert.assertEquals("rank", lindenRequest.getSourceFields().get(1));

    LindenFacet facetRequest = new LindenFacet();
    facetRequest.addToDrillDownDimAndPaths(new LindenFacetDimAndPath().setDim("color"));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());

    bql = "SELECT * FROM linden browse by color(5) drill sideways shape('rectangle')";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    facetRequest = new LindenFacet();
    facetRequest.setFacetDrillingType(FacetDrillingType.DRILLSIDEWAYS);
    facetRequest.addToFacetParams(
        new LindenFacetParam().setTopN(5).setFacetDimAndPath(new LindenFacetDimAndPath().setDim("color")));
    facetRequest.addToDrillDownDimAndPaths(new LindenFacetDimAndPath().setDim("shape").setPath("rectangle"));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());

    bql = "SELECT * FROM linden drill down color(\"red\")";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    facetRequest = new LindenFacet();
    facetRequest.addToDrillDownDimAndPaths(
        new LindenFacetDimAndPath().setDim("color").setPath("red"));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());

    bql = "SELECT * FROM linden drill sideways color('light/gray/white') ";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    facetRequest = new LindenFacet();
    facetRequest.setFacetDrillingType(FacetDrillingType.DRILLSIDEWAYS);
    facetRequest.addToDrillDownDimAndPaths(new LindenFacetDimAndPath().setDim("color").setPath("light/gray/white"));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());

    bql = "SELECT * FROM linden browse by color(6, 'light/gray/white') drill down color('red'), shape('rectangle')";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    facetRequest = new LindenFacet();
    facetRequest.addToFacetParams(
        new LindenFacetParam().setTopN(6).setFacetDimAndPath(
            new LindenFacetDimAndPath().setDim("color").setPath("light/gray/white")));
    facetRequest.addToDrillDownDimAndPaths(
            new LindenFacetDimAndPath().setDim("color").setPath("red"));
    facetRequest.addToDrillDownDimAndPaths(
        new LindenFacetDimAndPath().setDim("shape").setPath("rectangle"));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());
  }

  @Test
  public void testDelete() {
    String bql = "delete from linden where title = 'sed'";
    LindenDeleteRequest lindenRequest = compiler.compile(bql).getDeleteRequest();
    LindenTermFilter termFilter = new LindenTermFilter().setTerm(new LindenTerm().setField("title").setValue("sed"));
    Assert.assertEquals(termFilter, lindenRequest.getQuery().getFilteredQuery().getLindenFilter().getTermFilter());

    bql = "delete from linden";
    lindenRequest = compiler.compile(bql).getDeleteRequest();
    Assert.assertTrue(lindenRequest.getQuery().isSetMatchAllQuery());

    bql = "delete from tag_1,tag_2";
    lindenRequest = compiler.compile(bql).getDeleteRequest();
    Assert.assertEquals(lindenRequest.getIndexNames().size(), 2);
    Assert.assertEquals(lindenRequest.getIndexNames().get(0), "tag_1");
    Assert.assertEquals(lindenRequest.getIndexNames().get(1), "tag_2");

    bql = "delete from linden where id = '123' route by 1, 2, 3";
    lindenRequest = compiler.compile(bql).getDeleteRequest();
    Assert.assertTrue(lindenRequest.getQuery().isSetFilteredQuery());
    Assert.assertEquals(3, lindenRequest.getRouteParam().getShardParamsSize());
    Assert.assertEquals(1, lindenRequest.getRouteParam().getShardParams().get(0).getShardId());
    Assert.assertEquals(2, lindenRequest.getRouteParam().getShardParams().get(1).getShardId());
  }

  @Test
  public void testAggregation() {
    String bql = "SELECT * FROM linden aggregate by id({1, 5])";
    List<Aggregation> aggregations = compiler.compile(bql).getSearchRequest().getFacet().getAggregations();
    Assert.assertEquals(
        "[Aggregation(field:id, type:LONG, buckets:[Bucket(startValue:1, endValue:5, startClosed:false, endClosed:true)])]",
        aggregations.toString());

    bql = "SELECT * FROM linden aggregate by id({1, 5]), dynamic.double({1.0, 3.00})";
    aggregations = compiler.compile(bql).getSearchRequest().getFacet().getAggregations();
    Assert.assertEquals(
        "[Aggregation(field:id, type:LONG, buckets:[Bucket(startValue:1, endValue:5, startClosed:false, endClosed:true)]), "
            + "Aggregation(field:dynamic, type:DOUBLE, buckets:[Bucket(startValue:1.0, endValue:3.00, startClosed:false, endClosed:false)])]",
        aggregations.toString());

    bql = "SELECT * FROM linden aggregate by id({1, 5], {5, 7}, [7, 10]), dynamic.double({1.0, 3.00})";
    aggregations = compiler.compile(bql).getSearchRequest().getFacet().getAggregations();
    Assert.assertEquals(
        "[Aggregation(field:id, type:LONG, buckets:[Bucket(startValue:1, endValue:5, startClosed:false, endClosed:true), "
        + "Bucket(startValue:5, endValue:7, startClosed:false, endClosed:false), "
        + "Bucket(startValue:7, endValue:10, startClosed:true, endClosed:true)]), "
        + "Aggregation(field:dynamic, type:DOUBLE, buckets:[Bucket(startValue:1.0, endValue:3.00, startClosed:false, endClosed:false)])]",
        aggregations.toString());

    bql = "SELECT * FROM linden aggregate by dynamic.double({1.0, *})";
    aggregations = compiler.compile(bql).getSearchRequest().getFacet().getAggregations();
    Assert.assertEquals(
        "[Aggregation(field:dynamic, type:DOUBLE, buckets:[Bucket(startValue:1.0, endValue:*, startClosed:false, endClosed:false)])]",
        aggregations.toString());
  }

  @Test
  public void testEscapedColumnName() {
    String bql = "select * from linden where `select` = \"qq\" and id = 211 limit 10, 50";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getFilter().isSetBooleanFilter());
    LindenBooleanFilter booleanFilter = lindenRequest.getFilter().getBooleanFilter();
    Assert.assertEquals(2, booleanFilter.getFiltersSize());
    Assert.assertEquals(LindenBooleanClause.MUST, booleanFilter.getFilters().get(0).getClause());
    Assert.assertEquals(new LindenTerm("select", "qq"),
        booleanFilter.getFilters().get(0).getFilter().getTermFilter().getTerm());

    bql = "SELECT * FROM linden WHERE `source` <> 'red'";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    booleanFilter = new LindenBooleanFilter();
    LindenFilter termFilter = LindenTermFilterBuilder.buildTermFilter("source", "red");
    booleanFilter
        .addToFilters(new LindenBooleanSubFilter().setFilter(termFilter).setClause(LindenBooleanClause.MUST_NOT));
    Assert.assertEquals(booleanFilter, lindenRequest.getFilter().getBooleanFilter());

    bql = "select * from linden where `explain` in (1, 2, 3)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenBooleanFilterBuilder builder = new LindenBooleanFilterBuilder();
    builder.addFilter(LindenTermFilterBuilder.buildTermFilter("explain", "1"), LindenBooleanClause.SHOULD);
    builder.addFilter(LindenTermFilterBuilder.buildTermFilter("explain", "2"), LindenBooleanClause.SHOULD);
    builder.addFilter(LindenTermFilterBuilder.buildTermFilter("explain", "3"), LindenBooleanClause.SHOULD);
    LindenFilter expected = builder.build();
    Assert.assertEquals(expected, lindenRequest.getFilter());

    bql = "SELECT * FROM linden WHERE `route` BETWEEN 'black' AND \"yellow\"";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenFilter filter = LindenRangeFilterBuilder.buildRangeFilter("route", LindenType.STRING, "black", "yellow", true, true);
    Assert.assertEquals(filter, lindenRequest.getFilter());

    bql = "SELECT `delete`, `score` FROM linden where title = 'sed' route by 0, 1, 2";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertEquals(2, lindenRequest.getSourceFieldsSize());
    Assert.assertEquals("delete", lindenRequest.getSourceFields().get(0));
    Assert.assertEquals("score", lindenRequest.getSourceFields().get(1));

    bql = "SELECT * FROM linden QUERY `FROM` LIKE 'sed*'";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenQuery query = new LindenQuery();
    query.setWildcardQuery(new LindenWildcardQuery().setQuery("sed*").setField("FROM"));
    Assert.assertEquals(query, lindenRequest.getQuery());

    bql = "SELECT * FROM linden browse by `group`(5)";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    LindenFacet facetRequest = new LindenFacet();
    facetRequest.addToFacetParams(
        new LindenFacetParam().setTopN(5).setFacetDimAndPath(new LindenFacetDimAndPath().setDim("group")));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());

    bql = "SELECT title, rank FROM linden drill down `group`('big')";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    facetRequest = new LindenFacet();
    facetRequest.addToDrillDownDimAndPaths(new LindenFacetDimAndPath().setDim("group").setPath("big"));
    Assert.assertEquals(facetRequest, lindenRequest.getFacet());
  }

  @Test
  public void testDisableCoord() {

    String bql = "select * from linden by query is 'abc' disableCoord false";
    LindenSearchRequest lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.getQuery().getQueryString().disableCoord);

    bql = "select * from linden by query is 'abc' and query is 'def' disableCoord false";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.getQuery().getBooleanQuery().disableCoord);
      Assert.assertFalse(
          lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getQueryString().disableCoord);

    bql = "select * from linden by query is 'abc' and query is 'def' andDisableCoord";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getQuery().getBooleanQuery().disableCoord);

    bql = "select * from linden by query is 'abc' and query is 'def' disableCoord andDisableCoord";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getQuery().getBooleanQuery().disableCoord);
    Assert.assertTrue(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getQueryString().disableCoord);

    bql = "select * from linden by query is 'abc' disableCoord and query is 'def' disableCoord false andDisableCoord false";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.getQuery().getBooleanQuery().disableCoord);
    Assert.assertTrue(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getQueryString().disableCoord);
    Assert.assertFalse(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getQueryString().disableCoord);

    bql = "select * from linden by query is 'abc' disableCoord false and query is 'def' disableCoord true andDisableCoord false";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.getQuery().getBooleanQuery().disableCoord);
    Assert.assertFalse(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getQueryString().disableCoord);
    Assert.assertTrue(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getQueryString().disableCoord);

    bql = "select * from linden by a = 'abc' and id = 123 andDisableCoord or b = 'def' orDisableCoord false";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.getQuery().getBooleanQuery().disableCoord);
    Assert.assertTrue(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBooleanQuery().disableCoord);

    bql = "select * from linden by a = 'qm' and id = 123 andDisableCoord false or query is 'def' disableCoord orDisableCoord false";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.getQuery().getBooleanQuery().disableCoord);
    Assert.assertFalse(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBooleanQuery().disableCoord);
    Assert.assertTrue(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getQueryString().disableCoord);

    bql = "select * from linden by a = 'qm' and id = 123 or query is 'def' disableCoord orDisableCoord false";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.getQuery().getBooleanQuery().disableCoord);
    Assert.assertFalse(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(0).getQuery().getBooleanQuery().disableCoord);
    Assert.assertTrue(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getQueryString().disableCoord);

    bql = "select * from linden by a = 'qm' and (id = 123 or query is 'def' orDisableCoord) andDisableCoord false";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertFalse(lindenRequest.getQuery().getBooleanQuery().disableCoord);
    Assert.assertTrue(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(1).getQuery().getBooleanQuery().disableCoord);
    
    bql = "select * from linden by a = 'qm' or id = 123 or query is 'def' disableCoord false orDisableCoord";
    lindenRequest = compiler.compile(bql).getSearchRequest();
    Assert.assertTrue(lindenRequest.getQuery().getBooleanQuery().disableCoord);
    Assert.assertFalse(
        lindenRequest.getQuery().getBooleanQuery().getQueries().get(2).getQuery().getQueryString().disableCoord);
  }
}
