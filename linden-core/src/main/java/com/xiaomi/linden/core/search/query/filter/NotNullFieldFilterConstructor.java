package com.xiaomi.linden.core.search.query.filter;

import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Filter;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.lucene.NotNullFieldFilter;
import com.xiaomi.linden.thrift.common.LindenFilter;
import com.xiaomi.linden.thrift.common.LindenNotNullFieldFilter;

/**
 * Created by yozhao on 21/08/2017.
 */
public class NotNullFieldFilterConstructor extends FilterConstructor {

  @Override
  protected Filter construct(LindenFilter lindenFilter, LindenConfig config) throws Exception {
    LindenNotNullFieldFilter lindenNotNullFieldFilter = lindenFilter.getNotNullFieldFilter();

    NotNullFieldFilter notNullFieldFilter = new NotNullFieldFilter(lindenNotNullFieldFilter.getField());
    if (!lindenNotNullFieldFilter.isReverse()) {
      return notNullFieldFilter;
    }

    BooleanFilter booleanFilter = new BooleanFilter();
    booleanFilter.add(notNullFieldFilter, BooleanClause.Occur.MUST_NOT);
    return booleanFilter;
  }
}
