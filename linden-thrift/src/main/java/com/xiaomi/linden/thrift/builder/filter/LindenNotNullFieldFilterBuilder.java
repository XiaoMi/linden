package com.xiaomi.linden.thrift.builder.filter;

import com.xiaomi.linden.thrift.common.LindenFilter;
import com.xiaomi.linden.thrift.common.LindenNotNullFieldFilter;

/**
 * Created by yozhao on 23/08/2017.
 */
public class LindenNotNullFieldFilterBuilder {

  public static LindenFilter buildNotNullFieldFilterBuilder(String field, boolean reverse) {
    return new LindenFilter().setNotNullFieldFilter(new LindenNotNullFieldFilter(field, reverse));
  }

}
