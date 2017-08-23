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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeProperty;
import org.antlr.v4.runtime.tree.TerminalNode;

import com.xiaomi.linden.core.LindenUtil;
import com.xiaomi.linden.thrift.builder.filter.LindenBooleanFilterBuilder;
import com.xiaomi.linden.thrift.builder.filter.LindenNotNullFieldFilterBuilder;
import com.xiaomi.linden.thrift.builder.filter.LindenQueryFilterBuilder;
import com.xiaomi.linden.thrift.builder.filter.LindenRangeFilterBuilder;
import com.xiaomi.linden.thrift.builder.filter.LindenSpatialFilterBuilder;
import com.xiaomi.linden.thrift.builder.filter.LindenTermFilterBuilder;
import com.xiaomi.linden.thrift.builder.query.LindenBooleanQueryBuilder;
import com.xiaomi.linden.thrift.builder.query.LindenQueryBuilder;
import com.xiaomi.linden.thrift.builder.query.LindenQueryStringQueryBuilder;
import com.xiaomi.linden.thrift.builder.query.LindenRangeQueryBuilder;
import com.xiaomi.linden.thrift.common.Aggregation;
import com.xiaomi.linden.thrift.common.Bucket;
import com.xiaomi.linden.thrift.common.EarlyParam;
import com.xiaomi.linden.thrift.common.FacetDrillingType;
import com.xiaomi.linden.thrift.common.GroupParam;
import com.xiaomi.linden.thrift.common.LindenBooleanClause;
import com.xiaomi.linden.thrift.common.LindenBooleanFilter;
import com.xiaomi.linden.thrift.common.LindenBooleanSubFilter;
import com.xiaomi.linden.thrift.common.LindenDeleteRequest;
import com.xiaomi.linden.thrift.common.LindenDisMaxQuery;
import com.xiaomi.linden.thrift.common.LindenFacet;
import com.xiaomi.linden.thrift.common.LindenFacetDimAndPath;
import com.xiaomi.linden.thrift.common.LindenFacetParam;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenFilter;
import com.xiaomi.linden.thrift.common.LindenFilteredQuery;
import com.xiaomi.linden.thrift.common.LindenFlexibleQuery;
import com.xiaomi.linden.thrift.common.LindenInputParam;
import com.xiaomi.linden.thrift.common.LindenMatchAllQuery;
import com.xiaomi.linden.thrift.common.LindenQuery;
import com.xiaomi.linden.thrift.common.LindenQueryFilter;
import com.xiaomi.linden.thrift.common.LindenRequest;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenScoreModel;
import com.xiaomi.linden.thrift.common.LindenSearchField;
import com.xiaomi.linden.thrift.common.LindenSearchRequest;
import com.xiaomi.linden.thrift.common.LindenSort;
import com.xiaomi.linden.thrift.common.LindenSortField;
import com.xiaomi.linden.thrift.common.LindenSortType;
import com.xiaomi.linden.thrift.common.LindenType;
import com.xiaomi.linden.thrift.common.LindenValue;
import com.xiaomi.linden.thrift.common.LindenWildcardQuery;
import com.xiaomi.linden.thrift.common.Operator;
import com.xiaomi.linden.thrift.common.SearchRouteParam;
import com.xiaomi.linden.thrift.common.ShardRouteParam;
import com.xiaomi.linden.thrift.common.SnippetField;
import com.xiaomi.linden.thrift.common.SnippetParam;
import com.xiaomi.linden.thrift.common.SpatialParam;


public class BQLCompilerAnalyzer extends BQLBaseListener {

  private static final int DEFAULT_REQUEST_OFFSET = 0;
  private static final int DEFAULT_REQUEST_SIZE = 10;

  private Map<String, LindenType> fieldTypeMap = new HashMap<>();
  private final ParseTreeProperty<LindenSearchRequest> lindenSearchRequestProperty = new ParseTreeProperty<>();
  private LindenDeleteRequest deleteRequest;
  private final ParseTreeProperty<LindenQuery> queryProperty = new ParseTreeProperty<>();
  private final ParseTreeProperty<LindenFilter> filterProperty = new ParseTreeProperty<>();
  private final ParseTreeProperty<Object> valProperty = new ParseTreeProperty<>();
  private final ParseTreeProperty<Integer> offsetProperty = new ParseTreeProperty<>();
  private final ParseTreeProperty<Integer> countProperty = new ParseTreeProperty<>();
  private final BQLParser parser;
  private SpatialParam spatialParam;
  private LindenFacet facetRequest = new LindenFacet();
  private Boolean inQueryWhere = false;

  public BQLCompilerAnalyzer(BQLParser parser, LindenSchema lindenSchema) {
    this.parser = parser;
    for (LindenFieldSchema fieldSchema : lindenSchema.getFields()) {
      fieldTypeMap.put(fieldSchema.getName(), fieldSchema.getType());
    }
    fieldTypeMap.put(lindenSchema.getId(), LindenType.STRING);
  }

  public LindenRequest getLindenRequest(ParseTree node) {
    LindenRequest lindenRequest = new LindenRequest();
    if (deleteRequest != null) {
      lindenRequest.setDeleteRequest(deleteRequest);
      return lindenRequest;
    }
    lindenRequest.setSearchRequest(lindenSearchRequestProperty.get(node));
    return lindenRequest;
  }

  public static boolean checkValueType(LindenType type, Object value) {
    if ((type == LindenType.STRING || type == LindenType.FACET) && value instanceof String) {
      return true;
    } else if (type == LindenType.LONG && value instanceof Long) {
      return true;
    } else if (type == LindenType.INTEGER && value instanceof Long) {
      if ((Long) value >= Integer.MIN_VALUE && (Long) value <= Integer.MAX_VALUE) {
        return true;
      }
    } else if (type == LindenType.DOUBLE && (value instanceof Long || value instanceof Double)) {
      return true;
    } else if (type == LindenType.FLOAT && value instanceof Long) {
      return true;
    } else if (type == LindenType.FLOAT && value instanceof Double) {
      if ((Double) value >= Float.MIN_VALUE && (Double) value <= Float.MAX_VALUE) {
        return true;
      }
    }
    return false;
  }

  public static boolean validateValueString(LindenType type, String value) {
    try {
      if (type == LindenType.INTEGER) {
        Integer.valueOf(value);
      } else if (type == LindenType.LONG) {
        Long.valueOf(value);
      } else if (type == LindenType.FLOAT) {
        Float.valueOf(value);
      } else if (type == LindenType.DOUBLE) {
        Double.valueOf(value);
      }
    } catch (Exception ex) {
      return false;
    }
    return true;
  }

  public static String unescapeStringLiteral(TerminalNode terminalNode) {
    Token token = terminalNode.getSymbol();
    if (token.getType() != BQLLexer.STRING_LITERAL) {
      throw new IllegalArgumentException();
    }

    String text = token.getText();
    char initialChar = text.charAt(0);
    if (text.charAt(text.length() - 1) != initialChar) {
      throw new IllegalArgumentException("malformed string literal");
    }

    text = text.substring(1, text.length() - 1);
    if (initialChar == '\'') {
      text = text.replace("''", "'");
    } else if (initialChar == '"') {
      text = text.replace("\"\"", "\"");
    } else {
      throw new UnsupportedOperationException("Not supported yet.");
    }
    return text;
  }

  @Override
  public void exitStatement(BQLParser.StatementContext ctx) {
    if (ctx.select_stmt() != null) {
      lindenSearchRequestProperty.put(ctx, lindenSearchRequestProperty.get(ctx.select_stmt()));
    } else if (ctx.multi_select() != null) {
      List<LindenSearchRequest> requests = new ArrayList<>();
      for (BQLParser.Select_stmtContext select : ctx.multi_select().select_stmt()) {
        requests.add(lindenSearchRequestProperty.get(select));
      }
      List<LindenQuery> queries = new ArrayList<>();
      for (LindenSearchRequest request : requests) {
        LindenQuery query;
        if (request.isSetQuery() && request.isSetFilter()) {
          query = LindenQueryBuilder.buildFilteredQuery(request.getQuery(), request.getFilter());
        } else if (request.isSetQuery()) {
          query = request.getQuery();
        } else if (request.isSetFilter()) {
          query = LindenQueryBuilder.buildFilteredQuery(LindenQueryBuilder.buildMatchAllQuery(),
                                                        request.getFilter());
        } else {
          continue;
        }
        queries.add(query);
      }
      LindenSearchRequest lindenRequest = requests.get(0);
      lindenRequest.setQuery(null);
      lindenRequest.setFilter(null);
      LindenDisMaxQuery disMaxQuery = new LindenDisMaxQuery().setTie(0.1);
      for (LindenQuery query : queries) {
        disMaxQuery.addToQueries(query);
      }
      lindenRequest.setQuery(new LindenQuery().setDisMaxQuery(disMaxQuery));
      lindenSearchRequestProperty.put(ctx, lindenRequest);
    }
  }

  @Override
  public void exitSelect_stmt(BQLParser.Select_stmtContext ctx) {
    if (ctx.order_by_clause().size() > 1) {
      throw new ParseCancellationException(new SemanticException(ctx.order_by_clause(1),
                                                                 "ORDER BY clause can only appear once."));
    }

    if (ctx.limit_clause().size() > 1) {
      throw new ParseCancellationException(new SemanticException(ctx.limit_clause(1),
                                                                 "LIMIT clause can only appear once."));
    }

    if (ctx.group_by_clause().size() > 1) {
      throw new ParseCancellationException(new SemanticException(ctx.group_by_clause(1),
                                                                 "GROUP BY clause can only appear once."));
    }

    if (ctx.browse_by_clause().size() > 1) {
      throw new ParseCancellationException(new SemanticException(ctx.browse_by_clause(1),
                                                                 "BROWSE BY clause can only appear once."));
    }

    if (ctx.drill_clause().size() > 1) {
      throw new ParseCancellationException(new SemanticException(ctx.drill_clause(1),
                                                                 "DRILL clause can only appear once."));
    }

    if (ctx.source_clause().size() > 1) {
      throw new ParseCancellationException(new SemanticException(ctx.source_clause(1),
                                                                 "SOURCE clause can only appear once."));
    }

    if (ctx.route_by_clause().size() > 1) {
      throw new ParseCancellationException(new SemanticException(ctx.route_by_clause(1),
                                                                 "ROUTE BY clause can only appear once."));
    }

    if (ctx.score_model_clause().size() > 1) {
      throw new ParseCancellationException(new SemanticException(ctx.score_model_clause(1),
                                                                 "USING SCORE MODEL clause can only appear once."));
    }

    LindenSearchRequest lindenRequest = new LindenSearchRequest();
    if (ctx.cols != null) {
      lindenRequest.setSourceFields((List<String>) valProperty.get(ctx.cols));
    }
    if (ctx.tables != null) {
      lindenRequest.setIndexNames((List<String>) valProperty.get(ctx.tables));
    }

    if (ctx.group_by != null) {
      GroupParam groupParam = (GroupParam) valProperty.get(ctx.group_by);
      if (groupParam != null) {
        lindenRequest.setGroupParam(groupParam);
      }
    }

    if (ctx.limit != null) {
      lindenRequest.setOffset(offsetProperty.get(ctx.limit));
      lindenRequest.setLength(countProperty.get(ctx.limit));
    }

    if (ctx.source != null && (Boolean) valProperty.get(ctx.source)) {
      lindenRequest.setSource(true);
    }

    if (ctx.explain != null && (Boolean) valProperty.get(ctx.explain)) {
      lindenRequest.setExplain(true);
    }

    LindenQuery query = null;
    if (ctx.q != null) {
      query = queryProperty.get(ctx.q);
    }
    if (query == null) {
      query = LindenQueryBuilder.buildMatchAllQuery();
    }
    lindenRequest.setQuery(query);

    if (ctx.w != null) {
      LindenFilter filter = filterProperty.get(ctx.w);
      lindenRequest.setFilter(filter);
    }
    if (ctx.scoring_model != null) {
      LindenScoreModel scoreModel = (LindenScoreModel) valProperty.get(ctx.scoring_model);
      lindenRequest.getQuery().setScoreModel(scoreModel);
    }
    if (ctx.boost_by != null && ctx.boost_by.numeric_value().PLACEHOLDER() == null) {
      lindenRequest.getQuery().setBoost(Double.valueOf(ctx.boost_by.numeric_value().getText()));
    }
    if (spatialParam != null) {
      lindenRequest.setSpatialParam(spatialParam);
    }
    if (ctx.order_by != null) {
      lindenRequest.setSort((LindenSort) valProperty.get(ctx.order_by));
    }
    if (ctx.snippet != null) {
      lindenRequest.setSnippetParam((SnippetParam) valProperty.get(ctx.snippet));
    }
    if (ctx.in_top != null) {
      lindenRequest.setEarlyParam((EarlyParam) valProperty.get(ctx.in_top));
    }
    if (ctx.route_param != null) {
      lindenRequest.setRouteParam((SearchRouteParam) valProperty.get(ctx.route_param));
    }

    if (facetRequest.isSetFacetParams() || facetRequest.isSetDrillDownDimAndPaths() ||
        facetRequest.isSetAggregations()) {
      lindenRequest.setFacet(facetRequest);
    }

    lindenSearchRequestProperty.put(ctx, lindenRequest);
  }


  @Override
  public void enterDelete_stmt(BQLParser.Delete_stmtContext ctx) {
    inQueryWhere = false;
  }

  @Override
  public void exitDelete_stmt(BQLParser.Delete_stmtContext ctx) {
    deleteRequest = new LindenDeleteRequest();
    if (ctx.dw != null) {
      LindenFilter filter = filterProperty.get(ctx.dw);
      if (filter == null) {
        throw new ParseCancellationException(new SemanticException(ctx, "Filter parse failed"));
      }
      LindenQuery query = LindenQueryBuilder.buildMatchAllQuery();
      query = LindenQueryBuilder.buildFilteredQuery(query, filter);
      deleteRequest.setQuery(query);
    } else {
      deleteRequest.setQuery(LindenQueryBuilder.buildMatchAllQuery());
    }
    if (ctx.route_param != null) {
      deleteRequest.setRouteParam((SearchRouteParam) valProperty.get(ctx.route_param));
    }
    if (ctx.indexes != null) {
      deleteRequest.setIndexNames((List<String>) valProperty.get(ctx.indexes));
    }
  }

  @Override
  public void enterQuery_where(BQLParser.Query_whereContext ctx) {
    inQueryWhere = true;
  }

  @Override
  public void exitQuery_where(BQLParser.Query_whereContext ctx) {
    inQueryWhere = false;
    queryProperty.put(ctx, queryProperty.get(ctx.search_expr()));
  }

  @Override
  public void exitWhere(BQLParser.WhereContext ctx) {
    filterProperty.put(ctx, filterProperty.get(ctx.search_expr()));
  }

  @Override
  public void exitSearch_expr(BQLParser.Search_exprContext ctx) {
    if (inQueryWhere) {
      List<LindenQuery> lindenQueries = new ArrayList<>();
      for (BQLParser.Term_exprContext f : ctx.term_expr()) {
        LindenQuery query = queryProperty.get(f);
        if (query == null) {
          continue;
        }
        lindenQueries.add(query);
      }
      if (lindenQueries.size() == 0) {
        return;
      }
      if (lindenQueries.size() == 1) {
        queryProperty.put(ctx, lindenQueries.get(0));
        return;
      }

      boolean disableCoord = ctx.disable_coord == null ? false : (boolean) valProperty.get(ctx.disable_coord);
      LindenBooleanQueryBuilder booleanQueryBuilder = new LindenBooleanQueryBuilder();
      booleanQueryBuilder.setDisableCoord(disableCoord);

      if (ctx.boost_by != null && ctx.boost_by.numeric_value().PLACEHOLDER() == null) {
        booleanQueryBuilder.setBoost(Double.valueOf(ctx.boost_by.numeric_value().getText()));
      }

      for (int i = 0; i < lindenQueries.size(); ++i) {
        // term_op indicates first clause is must
        if (ctx.term_op != null && i == 0) {
          booleanQueryBuilder.addQuery(lindenQueries.get(i), LindenBooleanClause.MUST);
        } else {
          booleanQueryBuilder.addQuery(lindenQueries.get(i), LindenBooleanClause.SHOULD);
        }
      }
      LindenQuery lindenQuery = booleanQueryBuilder.build();
      queryProperty.put(ctx, lindenQuery);
    } else {
      List<LindenFilter> lindenFilters = new ArrayList<>();
      for (BQLParser.Term_exprContext f : ctx.term_expr()) {
        LindenFilter filter = filterProperty.get(f);
        if (filter == null) {
          continue;
        }
        lindenFilters.add(filter);
      }
      if (lindenFilters.size() == 0) {
        return;
      }
      if (lindenFilters.size() == 1) {
        filterProperty.put(ctx, lindenFilters.get(0));
        return;
      }

      LindenBooleanFilterBuilder booleanFilterBuilder = new LindenBooleanFilterBuilder();
      for (int i = 0; i < lindenFilters.size(); ++i) {
        // term_op indicates first clause is must
        if (ctx.term_op != null && i == 0) {
          booleanFilterBuilder.addFilter(lindenFilters.get(i), LindenBooleanClause.MUST);
        } else {
          booleanFilterBuilder.addFilter(lindenFilters.get(i), LindenBooleanClause.SHOULD);
        }
      }
      LindenFilter lindenFilter = booleanFilterBuilder.build();
      filterProperty.put(ctx, lindenFilter);
    }
  }

  @Override
  public void exitTerm_expr(BQLParser.Term_exprContext ctx) {
    if (inQueryWhere) {
      List<LindenQuery> lindenQueries = new ArrayList<>();
      for (BQLParser.Factor_exprContext f : ctx.factor_expr()) {
        LindenQuery query = queryProperty.get(f);
        if (query == null) {
          continue;
        }
        lindenQueries.add(query);
      }
      if (lindenQueries.size() == 0) {
        return;
      }
      if (lindenQueries.size() == 1) {
        queryProperty.put(ctx, lindenQueries.get(0));
        return;
      }

      boolean disableCoord = ctx.disable_coord == null ? false : (boolean) valProperty.get(ctx.disable_coord);
      LindenBooleanQueryBuilder booleanQueryBuilder = new LindenBooleanQueryBuilder();
      booleanQueryBuilder.setDisableCoord(disableCoord);
      if (ctx.boost_by != null && ctx.boost_by.numeric_value().PLACEHOLDER() == null) {
        booleanQueryBuilder.setBoost(Double.valueOf(ctx.boost_by.numeric_value().getText()));
      }

      for (int i = 0; i < lindenQueries.size(); ++i) {
        booleanQueryBuilder.addQuery(lindenQueries.get(i), LindenBooleanClause.MUST);
      }
      LindenQuery lindenQuery = booleanQueryBuilder.build();
      queryProperty.put(ctx, lindenQuery);
    } else {
      List<LindenFilter> lindenFilters = new ArrayList<>();
      for (BQLParser.Factor_exprContext f : ctx.factor_expr()) {
        LindenFilter filter = filterProperty.get(f);
        if (filter == null) {
          continue;
        }
        lindenFilters.add(filter);
      }
      if (lindenFilters.size() == 0) {
        return;
      }
      if (lindenFilters.size() == 1) {
        filterProperty.put(ctx, lindenFilters.get(0));
        return;
      }

      LindenBooleanFilterBuilder booleanFilterBuilder = new LindenBooleanFilterBuilder();
      for (int i = 0; i < lindenFilters.size(); ++i) {
        booleanFilterBuilder.addFilter(lindenFilters.get(i), LindenBooleanClause.MUST);
      }
      LindenFilter lindenFilter = booleanFilterBuilder.build();
      filterProperty.put(ctx, lindenFilter);
    }
  }

  @Override
  public void exitFactor_expr(BQLParser.Factor_exprContext ctx) {
    if (inQueryWhere) {
      if (ctx.predicate() != null) {
        queryProperty.put(ctx, queryProperty.get(ctx.predicate()));
      } else {
        queryProperty.put(ctx, queryProperty.get(ctx.search_expr()));
      }
    } else {
      if (ctx.predicate() != null) {
        filterProperty.put(ctx, filterProperty.get(ctx.predicate()));
      } else {
        filterProperty.put(ctx, filterProperty.get(ctx.search_expr()));
      }
    }
  }

  @Override
  public void exitPredicate(BQLParser.PredicateContext ctx) {
    if (inQueryWhere) {
      LindenQuery query = queryProperty.get(ctx.getChild(0));
      if (query == null) {
        return;
      }
      if (ctx.boost_by != null && ctx.boost_by.numeric_value().PLACEHOLDER() == null) {
        query.setBoost(Double.valueOf(ctx.boost_by.numeric_value().getText()));
      }
      queryProperty.put(ctx, query);
    } else {
      filterProperty.put(ctx, filterProperty.get(ctx.getChild(0)));
    }
  }

  @Override
  public void exitEqual_predicate(BQLParser.Equal_predicateContext ctx) {
    if (ctx.value().PLACEHOLDER() != null) {
      return;
    }

    String value = valProperty.get(ctx.value()).toString();
    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    LindenType type = fieldNameAndType.getValue();
    col = fieldNameAndType.getKey();

    if (!validateValueString(type, value)) {
      throw new ParseCancellationException("Filed value: " + value + " doesn't corresponds to field type: " + type);
    }

    if (inQueryWhere) {
      LindenQuery query;
      switch (type) {
        case STRING:
        case FACET:
          query = LindenQueryBuilder.buildTermQuery(col, value);
          break;
        case INTEGER:
        case LONG:
        case DOUBLE:
        case FLOAT:
          query = LindenRangeQueryBuilder.buildRangeQuery(col, type, value, value, true, true);
          break;
        default:
          throw new ParseCancellationException("EQUAL predicate doesn't support this type " + type);
      }
      queryProperty.put(ctx, query);
    } else {
      LindenFilter filter;
      switch (type) {
        case STRING:
        case FACET:
          filter = LindenTermFilterBuilder.buildTermFilter(col, value);
          break;
        case INTEGER:
        case LONG:
        case DOUBLE:
        case FLOAT:
          filter = LindenRangeFilterBuilder.buildRangeFilter(col, type, value, value, true, true);
          break;
        default:
          throw new ParseCancellationException("EQUAL predicate doesn't support this type " + type);
      }
      filterProperty.put(ctx, filter);
    }
  }

  @Override
  public void exitNot_equal_predicate(BQLParser.Not_equal_predicateContext ctx) {
    if (ctx.value().PLACEHOLDER() != null) {
      return;
    }

    String value = valProperty.get(ctx.value()).toString();
    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    LindenType type = fieldNameAndType.getValue();
    col = fieldNameAndType.getKey();
    if (!validateValueString(type, value)) {
      throw new ParseCancellationException(
          "Filed value: " + value + " doesn't corresponds to field type: " + type);
    }

    if (inQueryWhere) {
      LindenBooleanQueryBuilder builder = new LindenBooleanQueryBuilder();
      builder.addQuery(LindenQueryBuilder.buildMatchAllQuery(),
                       LindenBooleanClause.MUST);
      switch (type) {
        case STRING:
        case FACET:
          builder.addQuery(LindenRangeQueryBuilder.buildTermQuery(col, value),
                           LindenBooleanClause.MUST_NOT);
          break;
        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
          builder.addQuery(LindenRangeQueryBuilder.buildRangeQuery(col, type, value, value, true, true),
                           LindenBooleanClause.MUST_NOT);
          break;
        default:
          throw new ParseCancellationException("NOT EQUAL predicate doesn't support this type " + type);
      }
      queryProperty.put(ctx, builder.build());
    } else {
      LindenBooleanFilterBuilder builder = new LindenBooleanFilterBuilder();
      switch (type) {
        case STRING:
        case FACET:
          builder.addFilter(LindenTermFilterBuilder.buildTermFilter(col, value),
                            LindenBooleanClause.MUST_NOT);
          break;
        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
          builder.addFilter(LindenRangeFilterBuilder.buildRangeFilter(col, type, value, value, true, true),
                            LindenBooleanClause.MUST_NOT);
          break;
        default:
          throw new ParseCancellationException("NOT EQUAL predicate doesn't support this type " + type);
      }
      filterProperty.put(ctx, builder.build());
    }
  }

  @Override
  public void exitValue(BQLParser.ValueContext ctx) {
    if (ctx.numeric() != null) {
      valProperty.put(ctx, valProperty.get(ctx.numeric()));
    } else if (ctx.STRING_LITERAL() != null) {
      valProperty.put(ctx, unescapeStringLiteral(ctx.STRING_LITERAL()));
    } else if (ctx.TRUE() != null) {
      valProperty.put(ctx, true);
    } else if (ctx.FALSE() != null) {
      valProperty.put(ctx, false);
    } else if (ctx.PLACEHOLDER() != null) {
      valProperty.put(ctx, ctx.PLACEHOLDER().getText());
    } else {
      throw new UnsupportedOperationException("Not yet implemented.");
    }
  }

  @Override
  public void exitNumeric(BQLParser.NumericContext ctx) {
    if (ctx.INTEGER() != null) {
      try {
        valProperty.put(ctx, Long.valueOf(ctx.INTEGER().getText()));
      } catch (NumberFormatException err) {
        throw new ParseCancellationException(new SemanticException(ctx.INTEGER(),
                                                                   "Hit NumberFormatException: " + err.getMessage()));
      }
    } else if (ctx.REAL() != null) {
      try {
        valProperty.put(ctx, Double.valueOf(ctx.REAL().getText()));
      } catch (NumberFormatException err) {
        throw new ParseCancellationException(new SemanticException(ctx.REAL(),
                                                                   "Hit NumberFormatException: " + err.getMessage()));
      }
    } else {
      throw new UnsupportedOperationException("Not yet implemented.");
    }
  }

  @Override
  public void exitQuery_predicate(BQLParser.Query_predicateContext ctx) {
    String orig = unescapeStringLiteral(ctx.STRING_LITERAL());

    LindenQueryStringQueryBuilder builder = new LindenQueryStringQueryBuilder().setQuery(orig);
    if (ctx.disable_coord != null) {
      builder.setDisableCoord((Boolean) valProperty.get(ctx.disable_coord));
    }
    if (ctx.AND() != null) {
      builder.setOperator(Operator.AND);
    }

    LindenQuery stringQuery = builder.build();
    if (inQueryWhere) {
      queryProperty.put(ctx, stringQuery);
    } else {
      LindenFilter filter = LindenQueryFilterBuilder.buildQueryFilter(stringQuery);
      filterProperty.put(ctx, filter);
    }
  }

  @Override
  public void exitValue_list(BQLParser.Value_listContext ctx) {
    List<Object> objects = new ArrayList<>();
    for (BQLParser.ValueContext v : ctx.value()) {
      if (v.PLACEHOLDER() == null) {
        objects.add(valProperty.get(v));
      }
    }
    valProperty.put(ctx, objects);
  }

  @Override
  public void exitExcept_clause(BQLParser.Except_clauseContext ctx) {
    valProperty.put(ctx, valProperty.get(ctx.value_list()));
  }

  @Override
  public void exitLimit_clause(BQLParser.Limit_clauseContext ctx) {
    if (ctx.n1 != null && ctx.n1.PLACEHOLDER() == null) {
      offsetProperty.put(ctx, Integer.parseInt(ctx.n1.getText()));
    } else {
      offsetProperty.put(ctx, DEFAULT_REQUEST_OFFSET);
    }
    if (ctx.n2 != null && ctx.n2.PLACEHOLDER() == null) {
      countProperty.put(ctx, Integer.parseInt(ctx.n2.getText()));
    } else {
      countProperty.put(ctx, DEFAULT_REQUEST_SIZE);
    }
  }

  @Override
  public void exitIn_predicate(BQLParser.In_predicateContext ctx) {
    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    LindenType type = fieldNameAndType.getValue();
    col = fieldNameAndType.getKey();

    List<Object> values = (List<Object>) valProperty.get(ctx.value_list());
    List<Object> excludes = (List<Object>) valProperty.get(ctx.except_clause());

    List<Map.Entry<String, LindenBooleanClause>> subClauses = new ArrayList<>();

    LindenBooleanClause clause = LindenBooleanClause.SHOULD;
    if (ctx.not != null) {
      clause = LindenBooleanClause.MUST_NOT;
    }

    for (int i = 0; values != null && i < values.size(); ++i) {
      if (values.get(i) == null) {
        continue;
      }
      subClauses.add(new AbstractMap.SimpleEntry<>(values.get(i).toString(), clause));
    }
    clause = LindenBooleanClause.MUST_NOT;
    if (ctx.not != null) {
      clause = LindenBooleanClause.SHOULD;
    }
    for (int i = 0; excludes != null && i < excludes.size(); ++i) {
      if (excludes.get(i) == null) {
        continue;
      }
      subClauses.add(new AbstractMap.SimpleEntry<>(excludes.get(i).toString(), clause));
    }
    if (subClauses.size() == 0) {
      return;
    }

    if (inQueryWhere) {
      LindenBooleanQueryBuilder builder = new LindenBooleanQueryBuilder();
      for (Map.Entry<String, LindenBooleanClause> clauseEntry : subClauses) {
        LindenQuery query;
        switch (type) {
          case STRING:
          case FACET:
            query = LindenQueryBuilder.buildTermQuery(col, clauseEntry.getKey());
            break;
          case LONG:
          case INTEGER:
          case FLOAT:
          case DOUBLE:
            query =
                LindenRangeQueryBuilder
                    .buildRangeQuery(col, type, clauseEntry.getKey(), clauseEntry.getKey(), true, true);
            break;
          default:
            throw new ParseCancellationException("IN predicate doesn't support this type " + type);
        }
        builder.addQuery(query, clauseEntry.getValue());
      }
      queryProperty.put(ctx, builder.build());
    } else {
      LindenBooleanFilterBuilder builder = new LindenBooleanFilterBuilder();
      for (Map.Entry<String, LindenBooleanClause> clauseEntry : subClauses) {
        LindenFilter filter;
        switch (type) {
          case STRING:
          case FACET:
            filter = LindenTermFilterBuilder.buildTermFilter(col, clauseEntry.getKey());
            break;
          case LONG:
          case INTEGER:
          case FLOAT:
          case DOUBLE:
            filter =
                LindenRangeFilterBuilder
                    .buildRangeFilter(col, type, clauseEntry.getKey(), clauseEntry.getKey(), true, true);
            break;
          default:
            throw new ParseCancellationException("IN predicate doesn't support this type " + type);
        }
        builder.addFilter(filter, clauseEntry.getValue());
      }
      filterProperty.put(ctx, builder.build());
    }
  }

  @Override
  public void exitScore_model_clause(BQLParser.Score_model_clauseContext ctx) {
    LindenScoreModel lindenScoreModel = new LindenScoreModel()
        .setName(ctx.IDENT().getText())
        .setFunc((String) valProperty.get(ctx.score_model()))
        .setParams((List<LindenInputParam>) valProperty.get(ctx.formal_parameters()));
    if (spatialParam != null) {
      lindenScoreModel.setCoordinate(spatialParam.getCoordinate());
    }
    if (ctx.OVERRIDE() != null) {
      lindenScoreModel.setOverride(true);
    }
    if (ctx.PLUGIN() != null) {
      lindenScoreModel.setPlugin(true);
    }
    valProperty.put(ctx, lindenScoreModel);
  }

  @Override
  public void exitScore_model(BQLParser.Score_modelContext ctx) {
    String func = parser.getInputStream().getText(ctx.model_block().getSourceInterval());
    valProperty.put(ctx, func);
  }

  @Override
  public void exitFormal_parameters(BQLParser.Formal_parametersContext ctx) {
    valProperty.put(ctx, valProperty.get(ctx.formal_parameter_decls()));
  }

  @Override
  public void exitFormal_parameter_decls(BQLParser.Formal_parameter_declsContext ctx) {
    List<LindenInputParam> inputParams = new ArrayList<>();
    for (BQLParser.Formal_parameter_declContext decl : ctx.formal_parameter_decl()) {
      inputParams.add((LindenInputParam) valProperty.get(decl));
    }
    valProperty.put(ctx, inputParams);
  }

  @Override
  public void exitFormal_parameter_decl(BQLParser.Formal_parameter_declContext ctx) {
    if (ctx.python_style_value() != null) {
      String name = ctx.variable_declarator_id().getText();
      LindenInputParam inputParam = new LindenInputParam(name);
      BQLParser.TypeContext obj = ctx.type();
      LindenType type = (LindenType) valProperty.get(obj);
      if (type == null && ctx.python_style_value().python_style_dict() == null) {
        return;
      }
      if (ctx.python_style_value().value() != null) {
        if (ctx.python_style_value().value().PLACEHOLDER() == null) {
          String value = String.valueOf(valProperty.get(ctx.python_style_value().value()));
          switch (type) {
            case STRING:
            case FACET:
              inputParam.setValue(new LindenValue().setStringValue(value));
              break;
            case LONG:
            case INTEGER:
              inputParam.setValue(new LindenValue().setLongValue(Long.valueOf(value)));
              break;
            case FLOAT:
            case DOUBLE:
              inputParam.setValue(new LindenValue().setDoubleValue(Double.valueOf(value)));
              break;
            default:
          }
        }
      } else if (ctx.python_style_value().python_style_list() != null) {
        List<String> values = (List<String>) valProperty.get(ctx.python_style_value().python_style_list());
        switch (type) {
          case STRING:
            inputParam.setValue(new LindenValue().setStringValues(values));
            break;
          case INTEGER:
          case LONG:
            if (values.size() > 0) {
              inputParam.setValue(new LindenValue());
              for (String value : values) {
                inputParam.getValue().addToLongValues(Long.valueOf(value));
              }
            }
            break;
          case FLOAT:
          case DOUBLE:
            if (values.size() > 0) {
              inputParam.setValue(new LindenValue());
              for (String value : values) {
                inputParam.getValue().addToDoubleValues(Double.valueOf(value));
              }
            }
            break;
          default:
        }
      } else {
        if (ctx.python_style_value().python_style_dict() != null) {
          LindenType leftType = (LindenType) valProperty.get(ctx.type().map_type().left);
          LindenType rightType = (LindenType) valProperty.get(ctx.type().map_type().rigth);
          Map<String, String>
              kvMap =
              (Map<String, String>) valProperty.get(ctx.python_style_value().python_style_dict());
          Map<LindenValue, LindenValue> lindenValueMap = new HashMap<>();
          for (Map.Entry<String, String> entry : kvMap.entrySet()) {
            LindenValue left = getLindenValue(leftType, entry.getKey());
            LindenValue right = getLindenValue(rightType, entry.getValue());
            lindenValueMap.put(left, right);
          }
          inputParam.setValue(new LindenValue().setMapValue(lindenValueMap));
        }
      }
      valProperty.put(ctx, inputParam);
    }
  }

  private LindenValue getLindenValue(LindenType type, String text) {
    LindenValue value = new LindenValue();
    switch (type) {
      case STRING:
      case FACET:
        value.setStringValue(text);
        break;
      case LONG:
      case INTEGER:
        value.setLongValue(Long.valueOf(text));
        break;
      case FLOAT:
      case DOUBLE:
        value.setDoubleValue(Double.valueOf(text));
        break;
    }
    return value;
  }

  @Override
  public void exitType(BQLParser.TypeContext ctx) {
    String type;
    if (ctx.primitive_type() != null) {
      type = ctx.primitive_type().getText();
    } else if (ctx.boxed_type() != null) {
      type = ctx.boxed_type().getText();
    } else if (ctx.limited_type() != null) {
      type = ctx.limited_type().getText();
    } else if (ctx.map_type() != null) {
      return;
    } else {
      throw new UnsupportedOperationException("Not implemented yet.");
    }
    try {
      if (type.equalsIgnoreCase("int")) {
        valProperty.put(ctx, LindenType.INTEGER);
      } else {
        valProperty.put(ctx, LindenType.valueOf(type.toUpperCase()));
      }
    } catch (Exception e) {
      throw new ParseCancellationException(new SemanticException(ctx, "Type " + type + " not support."));
    }
  }

  @Override
  public void exitVariable_declarator_id(BQLParser.Variable_declarator_idContext ctx) {
    valProperty.put(ctx, ctx.IDENT().getText());
  }

  @Override
  public void exitFlexible_query_predicate(BQLParser.Flexible_query_predicateContext ctx) {
    String orig = unescapeStringLiteral(ctx.STRING_LITERAL());

    LindenScoreModel lindenScoreModel = new LindenScoreModel()
        .setName(ctx.IDENT().getText())
        .setFunc((String) valProperty.get(ctx.score_model()))
        .setParams((List<LindenInputParam>) valProperty.get(ctx.formal_parameters()));
    if (ctx.OVERRIDE() != null) {
      lindenScoreModel.setOverride(true);
    }
    if (ctx.PLUGIN() != null) {
      lindenScoreModel.setPlugin(true);
    }

    LindenFlexibleQuery lindenFlexibleQuery = new LindenFlexibleQuery()
        .setQuery(orig)
        .setFields((List<LindenSearchField>) valProperty.get(ctx.flexible_fields()));
    lindenFlexibleQuery.setModel(lindenScoreModel);
    if (ctx.fm != null) {
      lindenFlexibleQuery.setFullMatch(true);
    } else if (ctx.mrt != null && ctx.mrt.PLACEHOLDER() == null) {
      double ratio = Double.valueOf(ctx.mrt.getText());
      lindenFlexibleQuery.setMatchRatio(ratio);
    }
    LindenQuery lindenQuery = new LindenQuery();
    if (inQueryWhere) {
      if (ctx.gi != null) {
        lindenFlexibleQuery.setGlobalIDF(true);
        if (ctx.gfd != null) {
          lindenFlexibleQuery.setGlobalFields((List<LindenSearchField>) valProperty.get(ctx.global_fields()));
        }
      }
      queryProperty.put(ctx, lindenQuery.setFlexQuery(lindenFlexibleQuery));
    } else {
      filterProperty.put(ctx,
                         LindenQueryFilterBuilder.buildQueryFilter(lindenQuery.setFlexQuery(lindenFlexibleQuery)));
    }
  }

  @Override
  public void exitFlexible_fields(BQLParser.Flexible_fieldsContext ctx) {
    List<LindenSearchField> fields = new ArrayList<>();
    for (BQLParser.Flexible_fieldContext field : ctx.flexible_field()) {
      fields.add((LindenSearchField) valProperty.get(field));
    }
    valProperty.put(ctx, fields);
  }

  @Override
  public void exitFlexible_field(BQLParser.Flexible_fieldContext ctx) {
    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    col = fieldNameAndType.getKey();
    LindenSearchField field = new LindenSearchField().setName(col);

    if (ctx.boost != null && ctx.boost.PLACEHOLDER() == null) {
      field.setBoost(Double.valueOf(ctx.boost.getText()));
    }
    valProperty.put(ctx, field);
  }

  @Override
  public void exitGlobal_fields(BQLParser.Global_fieldsContext ctx) {
    List<LindenSearchField> fields = new ArrayList<>();
    for (BQLParser.Flexible_fieldContext field : ctx.flexible_field()) {
      fields.add((LindenSearchField) valProperty.get(field));
    }
    valProperty.put(ctx, fields);
  }

  @Override
  public void exitDistance_predicate(BQLParser.Distance_predicateContext ctx) {
    if (ctx.range.PLACEHOLDER() != null || ctx.lat.PLACEHOLDER() != null || ctx.lon.PLACEHOLDER() != null) {
      return;
    }
    LindenFilter spatialFilter =
        LindenSpatialFilterBuilder.buildSpatialParam(
            Double.valueOf(ctx.lon.getText()),
            Double.valueOf(ctx.lat.getText()),
            Double.valueOf(ctx.range.getText()));
    spatialParam = spatialFilter.getSpatialFilter().getSpatialParam();
    filterProperty.put(ctx, spatialFilter);
  }

  @Override
  public void exitGroup_by_clause(BQLParser.Group_by_clauseContext ctx) {
    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    col = fieldNameAndType.getKey();
    GroupParam groupParam = new GroupParam(col);
    if (ctx.top != null && ctx.PLACEHOLDER() == null) {
      groupParam.setGroupInnerLimit(Integer.valueOf(ctx.top.getText()));
    }
    valProperty.put(ctx, groupParam);
  }

  @Override
  public void exitExplain_clause(BQLParser.Explain_clauseContext ctx) {
    if (ctx.e1 != null && ctx.PLACEHOLDER() != null) {
      valProperty.put(ctx, false);
    } else {
      valProperty.put(ctx, ctx.FALSE() == null);
    }
  }

  @Override
  public void exitSource_clause(BQLParser.Source_clauseContext ctx) {
    if (ctx.PLACEHOLDER() != null) {
      valProperty.put(ctx, false);
    } else {
      valProperty.put(ctx, ctx.FALSE() == null);
    }
  }

  @Override
  public void exitOrder_by_clause(BQLParser.Order_by_clauseContext ctx) {
    valProperty.put(ctx, valProperty.get(ctx.sort_specs()));
  }

  @Override
  public void exitSort_specs(BQLParser.Sort_specsContext ctx) {
    LindenSort lindenSort = new LindenSort();
    for (BQLParser.Sort_specContext sortCtx : ctx.sort_spec()) {
      lindenSort.addToFields((LindenSortField) valProperty.get(sortCtx));
    }
    valProperty.put(ctx, lindenSort);
  }

  @Override
  public void exitSort_spec(BQLParser.Sort_specContext ctx) {
    LindenSortField sortField;
    if (ctx.DISTANCE() != null) {
      sortField = new LindenSortField().setName("").setType(LindenSortType.DISTANCE);
    } else if (ctx.SCORE() != null) {
      sortField = new LindenSortField().setName("").setType(LindenSortType.SCORE);
    } else {
      String col = unescapeColumnName(ctx.column_name());
      Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
      LindenType type = fieldNameAndType.getValue();
      col = fieldNameAndType.getKey();
      LindenSortType sortType = LindenSortType.findByValue(type.getValue());
      sortField = new LindenSortField().setName(col).setType(sortType).setReverse(true);
    }
    if (ctx.ASC() != null) {
      sortField.setReverse(false);
    } else if (ctx.DESC() != null) {
      sortField.setReverse(true);
    }
    valProperty.put(ctx, sortField);
  }

  @Override
  public void exitBetween_predicate(BQLParser.Between_predicateContext ctx) {
    if (ctx.val1.PLACEHOLDER() != null || ctx.val2.PLACEHOLDER() != null) {
      return;
    }

    String val1 = valProperty.get(ctx.val1).toString();
    String val2 = valProperty.get(ctx.val2).toString();

    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    LindenType type = fieldNameAndType.getValue();
    col = fieldNameAndType.getKey();
    if (!validateValueString(type, val1)) {
      throw new ParseCancellationException("Filed value: " + val1 + " doesn't corresponds to field type: " + type);
    }
    if (!validateValueString(type, val2)) {
      throw new ParseCancellationException("Filed value: " + val2 + " doesn't corresponds to field type: " + type);
    }

    if (inQueryWhere) {
      LindenQuery query;
      if (ctx.not == null) {
        query = LindenRangeQueryBuilder.buildRangeQuery(col, type, val1, val2, true, true);
      } else {
        LindenQuery query1 = LindenRangeQueryBuilder.buildRangeQuery(col, type, null, val1, false, false);
        LindenQuery query2 = LindenRangeQueryBuilder.buildRangeQuery(col, type, val2, null, false, false);
        LindenBooleanQueryBuilder builder = new LindenBooleanQueryBuilder();
        builder.addQuery(query1, LindenBooleanClause.SHOULD);
        builder.addQuery(query2, LindenBooleanClause.SHOULD);
        query = builder.build();
      }
      queryProperty.put(ctx, query);
    } else {
      LindenFilter filter;
      if (ctx.not == null) {
        filter = LindenRangeFilterBuilder.buildRangeFilter(col, type, val1, val2, true, true);
      } else {
        LindenFilter filter1 = LindenRangeFilterBuilder.buildRangeFilter(col, type, null, val1, false, false);
        LindenFilter filter2 = LindenRangeFilterBuilder.buildRangeFilter(col, type, val2, null, false, false);
        LindenBooleanFilterBuilder builder = new LindenBooleanFilterBuilder();
        builder.addFilter(filter1, LindenBooleanClause.SHOULD);
        builder.addFilter(filter2, LindenBooleanClause.SHOULD);
        filter = builder.build();
      }
      if (filter != null) {
        filterProperty.put(ctx, filter);
      }
    }
  }

  @Override
  public void exitRange_predicate(BQLParser.Range_predicateContext ctx) {
    // ignore unassigned value
    if (ctx.val.PLACEHOLDER() != null) {
      return;
    }

    Object val = valProperty.get(ctx.val);
    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    LindenType type = fieldNameAndType.getValue();
    col = fieldNameAndType.getKey();
    if (!checkValueType(type, val)) {
      throw new ParseCancellationException(
          "Value: " + val + " in RANGE predicate doesn't correspond to field type: " + type);
    }

    String strVal1;
    String strVal2;
    boolean isStartClosed = false;
    boolean isEndClosed = false;
    if (ctx.op.getText().charAt(0) == '>') {
      strVal1 = val.toString();
      strVal2 = null;
      if (">=".equals(ctx.op.getText())) {
        isStartClosed = true;
      }
    } else {
      strVal1 = null;
      strVal2 = val.toString();
      if ("<=".equals(ctx.op.getText())) {
        isEndClosed = true;
      }
    }

    if (inQueryWhere) {
      LindenQuery
          query =
          LindenRangeQueryBuilder.buildRangeQuery(col, type, strVal1, strVal2, isStartClosed, isEndClosed);
      queryProperty.put(ctx, query);
    } else {
      LindenFilter
          filter =
          LindenRangeFilterBuilder.buildRangeFilter(col, type, strVal1, strVal2, isStartClosed, isEndClosed);
      filterProperty.put(ctx, filter);
    }
  }

  @Override
  public void exitLike_predicate(BQLParser.Like_predicateContext ctx) {
    if (ctx.PLACEHOLDER() != null) {
      return;
    }

    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    LindenType type = fieldNameAndType.getValue();
    if (type != LindenType.STRING && type != LindenType.FACET) {
      throw new ParseCancellationException(new SemanticException(ctx.column_name(),
                                                                 "Non-string type column \"" + col
                                                                 + "\" can not be used in LIKE predicates."));
    }
    col = fieldNameAndType.getKey();

    String likeString = unescapeStringLiteral(ctx.STRING_LITERAL());
    LindenWildcardQuery wildcardQuery = new LindenWildcardQuery().setField(col)
        .setQuery(likeString);
    if (inQueryWhere) {
      if (ctx.NOT() != null) {
        LindenBooleanQueryBuilder builder = new LindenBooleanQueryBuilder();
        builder.addQuery(LindenRangeQueryBuilder.buildMatchAllQuery(), LindenBooleanClause.MUST);
        builder.addQuery(new LindenQuery().setWildcardQuery(wildcardQuery), LindenBooleanClause.MUST_NOT);
        queryProperty.put(ctx, builder.build());
      } else {
        queryProperty.put(ctx, new LindenQuery().setWildcardQuery(wildcardQuery));
      }
    } else {
      LindenFilter filter = new LindenFilter().setQueryFilter(
          new LindenQueryFilter().setQuery(new LindenQuery().setWildcardQuery(wildcardQuery)));
      if (ctx.NOT() != null) {
        LindenBooleanFilter booleanFilter = new LindenBooleanFilter();
        booleanFilter.addToFilters(new LindenBooleanSubFilter().setFilter(filter)
                                       .setClause(LindenBooleanClause.MUST_NOT));
        filter = new LindenFilter().setBooleanFilter(booleanFilter);
      }
      filterProperty.put(ctx, filter);
    }
  }

  @Override
  public void exitNull_predicate(BQLParser.Null_predicateContext ctx) {
    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    col = fieldNameAndType.getKey();

    LindenFilter filter;
    if (ctx.NOT() != null) {
      filter = LindenNotNullFieldFilterBuilder.buildNotNullFieldFilterBuilder(col, false);
    } else {
      filter = LindenNotNullFieldFilterBuilder.buildNotNullFieldFilterBuilder(col, true);
    }

    if (inQueryWhere) {
      LindenQuery query = LindenQueryBuilder.buildFilteredQuery(LindenQueryBuilder.buildMatchAllQuery(), filter);
      queryProperty.put(ctx, query);
    } else {
      filterProperty.put(ctx, filter);
    }
  }

  @Override
  public void exitSnippet_clause(BQLParser.Snippet_clauseContext ctx) {
    if (ctx.selection_list() != null) {
      List<String> selections = (List<String>) valProperty.get(ctx.selection_list());
      if (selections != null && !selections.isEmpty()) {
        SnippetParam snippet = new SnippetParam();
        for (String selection : selections) {
          Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(selection);
          LindenType type = fieldNameAndType.getValue();
          String col = fieldNameAndType.getKey();
          if (type == LindenType.STRING) {
            snippet.addToFields(new SnippetField(col));
          } else {
            throw new ParseCancellationException("Snippet doesn't support this type " + type);
          }
        }
        valProperty.put(ctx, snippet);
      }
    }
  }

  @Override
  public void exitSelection_list(BQLParser.Selection_listContext ctx) {
    if (ctx.column_name() != null) {
      List<String> selections = new ArrayList<>();
      for (BQLParser.Column_nameContext colCtx : ctx.column_name()) {
        String col = unescapeColumnName(colCtx);
        selections.add(col);
      }
      if (!selections.isEmpty()) {
        valProperty.put(ctx, selections);
      }
    }
  }

  @Override
  public void exitIn_top_clause(BQLParser.In_top_clauseContext ctx) {
    if (ctx.max_num != null && ctx.max_num.PLACEHOLDER() == null) {
      EarlyParam param = new EarlyParam();
      param.setMaxNum(Integer.parseInt(ctx.max_num.getText()));
      valProperty.put(ctx, param);
    }
  }

  @Override
  public void exitRoute_by_clause(BQLParser.Route_by_clauseContext ctx) {
    SearchRouteParam routeParam = new SearchRouteParam();
    // shard route param
    if (ctx.route_shard_clause() != null) {
      routeParam.setShardParams((List<ShardRouteParam>) valProperty.get(ctx.route_shard_clause()));
    }
    // key route param
    if (ctx.route_replica_clause() != null) {
      routeParam.setReplicaRouteKey((String) valProperty.get(ctx.route_replica_clause()));
    }
    valProperty.put(ctx, routeParam);
  }

  @Override
  public void exitRoute_shard_clause(BQLParser.Route_shard_clauseContext ctx) {
    List<ShardRouteParam> shardRouteParams = new ArrayList<>();
    for (BQLParser.Route_shard_valueContext route : ctx.route_shard_value()) {
      if (route.route_single_shard_value() != null) {
        ShardRouteParam shardRouteParam = (ShardRouteParam) valProperty.get(route.route_single_shard_value());
        if (shardRouteParam != null) {
          shardRouteParams.add(shardRouteParam);
        }
      } else if (route.route_multi_shard_values() != null) {
        List<ShardRouteParam>
            multiShardRouteParams =
            (List<ShardRouteParam>) valProperty.get(route.route_multi_shard_values());
        if (multiShardRouteParams != null) {
          shardRouteParams.addAll(multiShardRouteParams);
        }
      }
    }
    valProperty.put(ctx, shardRouteParams);
  }

  @Override
  public void exitRoute_replica_clause(BQLParser.Route_replica_clauseContext ctx) {
    String key = unescapeStringLiteral(ctx.STRING_LITERAL());
    valProperty.put(ctx, key);
  }

  @Override
  public void exitRoute_single_shard_value(BQLParser.Route_single_shard_valueContext ctx) {
    // ignore
    if (ctx.numeric_value().PLACEHOLDER() != null) {
      return;
    }
    ShardRouteParam shardRouteParam = new ShardRouteParam();
    int shardId = Integer.valueOf(ctx.numeric_value().getText());
    shardRouteParam.setShardId(shardId);
    EarlyParam earlyParam = (EarlyParam) valProperty.get(ctx.in_top_clause());
    if (earlyParam != null) {
      shardRouteParam.setEarlyParam(earlyParam);
    }
    valProperty.put(ctx, shardRouteParam);
  }

  @Override
  public void exitRoute_multi_shard_values(BQLParser.Route_multi_shard_valuesContext ctx) {
    List<ShardRouteParam> params = new ArrayList<>();
    for (BQLParser.Numeric_valueContext shardCtx : ctx.numeric_value()) {
      if (shardCtx.PLACEHOLDER() != null) {
        continue;
      }
      ShardRouteParam shardRouteParam = new ShardRouteParam();
      int shardId = Integer.valueOf(shardCtx.getText());
      shardRouteParam.setShardId(shardId);
      EarlyParam earlyParam = (EarlyParam) valProperty.get(ctx.in_top_clause());
      if (earlyParam != null) {
        shardRouteParam.setEarlyParam(earlyParam);
      }
      params.add(shardRouteParam);
    }
    if (!params.isEmpty()) {
      valProperty.put(ctx, params);
    }
  }

  @Override
  public void exitPython_style_value(BQLParser.Python_style_valueContext ctx) {
    if (ctx.value() != null && ctx.value().numeric() != null) {
      valProperty.put(ctx.value(), ctx.value().getText());
    }
  }

  @Override
  public void exitPython_style_list(BQLParser.Python_style_listContext ctx) {
    List<String> values = new ArrayList<>();
    for (BQLParser.Python_style_valueContext subCtx : ctx.python_style_value()) {
      if (subCtx.value() != null) {
        values.add(subCtx.value().getText());
      } else if (subCtx.python_style_list() != null) {
        throw new ParseCancellationException(
            new SemanticException(subCtx.python_style_list(), "Nested list is not supported"));
      } else if (subCtx.python_style_dict() != null) {
        throw new ParseCancellationException(
            new SemanticException(subCtx.python_style_dict(), "Dict list is not supported"));
      }
    }
    valProperty.put(ctx, values);
  }

  @Override
  public void exitPython_style_dict(BQLParser.Python_style_dictContext ctx) {
    Map<String, String> kvMap = new HashMap<>();
    for (BQLParser.Key_value_pairContext kvCtx : ctx.key_value_pair()) {
      String key = unescapeStringLiteral(kvCtx.STRING_LITERAL());
      kvMap.put(key, kvCtx.value().getText());
    }
    valProperty.put(ctx, kvMap);
  }

  @Override
  public void exitTable_stmt(BQLParser.Table_stmtContext ctx) {
    List<String> tables = new ArrayList<>();
    if (ctx.IDENT() != null) {
      for (TerminalNode talbeCtx : ctx.IDENT()) {
        tables.add(talbeCtx.getText());
      }
    }
    if (ctx.def != null) {
      tables.add(ctx.def.getText());
    }
    if (!tables.isEmpty()) {
      valProperty.put(ctx, tables);
    }
  }

  @Override
  public void exitFacet_spec(BQLParser.Facet_specContext ctx) {
    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    LindenType type = fieldNameAndType.getValue();
    if (type != LindenType.FACET) {
      throw new ParseCancellationException(new SemanticException(ctx.column_name(),
                                                                 "Non-facet type column \"" + col
                                                                 + "\" can not be used in browse predicates."));
    }
    col = fieldNameAndType.getKey();

    LindenFacetParam facetParam = new LindenFacetParam();
    LindenFacetDimAndPath facetDimAndPath = new LindenFacetDimAndPath();
    facetDimAndPath.setDim(col);
    if (ctx.n1 != null) {
      facetParam.setTopN(Integer.parseInt(ctx.n1.getText()));
    }
    if (ctx.path != null) {
      String path = unescapeStringLiteral(ctx.STRING_LITERAL());
      facetDimAndPath.setPath(path);
    }
    facetParam.setFacetDimAndPath(facetDimAndPath);
    facetRequest.addToFacetParams(facetParam);
  }

  @Override
  public void exitBucket_spec(BQLParser.Bucket_specContext ctx) {
    Bucket bucket = new Bucket();
    if (ctx.LBRACE() != null) {
      bucket.setStartClosed(false);
    } else {
      bucket.setStartClosed(true);
    }

    if (ctx.n1 != null) {
      if (ctx.n1.PLACEHOLDER() != null) {
        return;
      }
      bucket.setStartValue(ctx.n1.getText());
    } else {
      bucket.setStartValue("*");
    }

    if (ctx.n2 != null) {
      if (ctx.n2.PLACEHOLDER() != null) {
        return;
      }
      bucket.setEndValue(ctx.n2.getText());
    } else {
      bucket.setEndValue("*");
    }

    if (ctx.RBRACE() != null) {
      bucket.setEndClosed(false);
    } else {
      bucket.setEndClosed(true);
    }
    valProperty.put(ctx, bucket);
  }

  @Override
  public void exitAggregation_spec(BQLParser.Aggregation_specContext ctx) {
    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    LindenType type = fieldNameAndType.getValue();
    if (type != LindenType.INTEGER && type != LindenType.LONG && type != LindenType.DOUBLE) {
      throw new ParseCancellationException(new SemanticException(ctx.column_name(),
                                                                 "Aggregation doesn't support the type of the field \""
                                                                 + col + "\"."));
    }
    col = fieldNameAndType.getKey();

    Aggregation aggregation = new Aggregation();
    aggregation.setField(col);
    aggregation.setType(type);
    for (BQLParser.Bucket_specContext specContext : ctx.bucket_spec()) {
      Bucket bucket = (Bucket) valProperty.get(specContext);
      if (bucket != null) {
        aggregation.addToBuckets(bucket);
      }
    }
    facetRequest.addToAggregations(aggregation);
  }

  @Override
  public void exitDrill_clause(BQLParser.Drill_clauseContext ctx) {
    if (ctx.DOWN() != null) {
      facetRequest.setFacetDrillingType(FacetDrillingType.DRILLDOWN);
    } else {
      facetRequest.setFacetDrillingType(FacetDrillingType.DRILLSIDEWAYS);
    }
  }

  @Override
  public void exitDrill_spec(BQLParser.Drill_specContext ctx) {
    LindenFacetDimAndPath facetDimAndPath = new LindenFacetDimAndPath();
    String col = unescapeColumnName(ctx.column_name());
    Map.Entry<String, LindenType> fieldNameAndType = getFieldNameAndType(col);
    LindenType type = fieldNameAndType.getValue();
    if (type != LindenType.FACET) {
      throw new ParseCancellationException(new SemanticException(ctx.column_name(),
                                                                 "Non-facet type column \"" + col
                                                                 + "\" can not be used in drill spec."));
    }
    col = fieldNameAndType.getKey();

    facetDimAndPath.setDim(col);
    if (ctx.path != null) {
      String path = unescapeStringLiteral(ctx.STRING_LITERAL());
      facetDimAndPath.setPath(path);
    }
    facetRequest.addToDrillDownDimAndPaths(facetDimAndPath);
  }

  @Override
  public void exitDisable_coord_clause(BQLParser.Disable_coord_clauseContext ctx) {
    valProperty.put(ctx, ctx.FALSE() == null);
  }

  @Override
  public void exitAnd_disable_coord_clause(BQLParser.And_disable_coord_clauseContext ctx) {
    valProperty.put(ctx, ctx.FALSE() == null);
  }

  @Override
  public void exitOr_disable_coord_clause(BQLParser.Or_disable_coord_clauseContext ctx) {
    valProperty.put(ctx, ctx.FALSE() == null);
  }


  private Map.Entry<String, LindenType> getFieldNameAndType(String col) {
    LindenType type = fieldTypeMap.get(col);
    if (type != null) {
      return new AbstractMap.SimpleEntry<>(col, type);
    }
    LindenFieldSchema fieldSchema = LindenUtil.parseDynamicFieldSchema(col);
    return new AbstractMap.SimpleEntry<>(fieldSchema.getName(), fieldSchema.getType());
  }

  private String unescapeColumnName(BQLParser.Column_nameContext ctx) {
    return ctx.getText().replace("`", "");
  }
}
