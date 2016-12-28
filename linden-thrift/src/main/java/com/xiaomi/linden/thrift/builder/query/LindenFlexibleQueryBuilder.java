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

package com.xiaomi.linden.thrift.builder.query;

import com.google.common.collect.Lists;
import com.xiaomi.linden.thrift.common.*;

import java.util.List;

public class LindenFlexibleQueryBuilder extends LindenQueryBuilder {

  private List<LindenSearchField> fields = Lists.newArrayList();
  private String query;
  private LindenScoreModel model = new LindenScoreModel();
  private boolean isFullMatch = false;

  public LindenFlexibleQueryBuilder setQuery(String query) {
    this.query = query;
    return this;
  }

  public LindenFlexibleQueryBuilder addField(String field) {
    return addField(field, 1f);
  }

  public LindenFlexibleQueryBuilder addField(String field, float boost) {
    fields.add(new LindenSearchField().setName(field).setBoost(boost));
    return this;
  }

  public LindenFlexibleQueryBuilder addModel(String name, String func) {
    model.setName(name).setFunc(func);
    return this;
  }

  public LindenFlexibleQueryBuilder setFullMatch(boolean fullMatch) {
    isFullMatch = fullMatch;
    return this;
  }

  @Override
  public LindenQuery build() {
    LindenFlexibleQuery flexibleQuery =
        new LindenFlexibleQuery().setQuery(query).setFields(fields).setModel(model);
    if (isFullMatch) {
      flexibleQuery.setFullMatch(true);
    }
    return new LindenQuery().setFlexQuery(flexibleQuery);
  }
}
