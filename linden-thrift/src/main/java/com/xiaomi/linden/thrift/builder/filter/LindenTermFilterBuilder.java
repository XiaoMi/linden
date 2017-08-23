package com.xiaomi.linden.thrift.builder.filter;

import com.xiaomi.linden.thrift.common.LindenFilter;
import com.xiaomi.linden.thrift.common.LindenTerm;
import com.xiaomi.linden.thrift.common.LindenTermFilter;

/**
 * Created by yozhao on 23/08/2017.
 */
public class LindenTermFilterBuilder {

  public static LindenFilter buildTermFilter(String field, String value) {
    return new LindenFilter().setTermFilter(new LindenTermFilter(new LindenTerm(field, value)));
  }
}
