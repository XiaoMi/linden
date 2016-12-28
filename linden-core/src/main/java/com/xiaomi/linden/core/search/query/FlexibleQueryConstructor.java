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

package com.xiaomi.linden.core.search.query;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.lucene.search.Query;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.lucene.query.flexiblequery.FlexibleQuery;
import com.xiaomi.linden.thrift.common.LindenFlexibleQuery;
import com.xiaomi.linden.thrift.common.LindenQuery;
import com.xiaomi.linden.thrift.common.LindenSearchField;

public class FlexibleQueryConstructor extends QueryConstructor {

  @Override
  protected Query construct(LindenQuery lindenQuery, LindenConfig config) throws IOException {
    if (lindenQuery.isSetFlexQuery()) {
      LindenFlexibleQuery lindenFlexibleQuery = lindenQuery.getFlexQuery();
      List<FlexibleQuery.FlexibleField> fields = Lists.newArrayList();
      for (LindenSearchField field : lindenFlexibleQuery.getFields()) {
        fields.add(new FlexibleQuery.FlexibleField(field.getName(), field.getBoost()));
      }
      List<FlexibleQuery.FlexibleField> globalFields = initGlobalFields(lindenFlexibleQuery.isGlobalIDF(),
                                                                        lindenFlexibleQuery.getGlobalFields(),
                                                                        lindenFlexibleQuery.getFields());
      FlexibleQuery flexibleQuery = new FlexibleQuery(fields, globalFields, lindenFlexibleQuery.getQuery(),
                                                      lindenFlexibleQuery.getModel(), config);
      flexibleQuery.setFullMatch(lindenFlexibleQuery.isFullMatch());
      flexibleQuery.setGlobalIDF(lindenFlexibleQuery.isGlobalIDF());
      flexibleQuery.setMatchRatio(lindenFlexibleQuery.getMatchRatio());
      return flexibleQuery;
    }
    return null;
  }

  private List<FlexibleQuery.FlexibleField> initGlobalFields(boolean globalIDF, List<LindenSearchField> globalFields,
                                                             List<LindenSearchField> queryFields) {
    List<FlexibleQuery.FlexibleField> fields = Lists.newArrayList();
    if (globalIDF) {
      if (globalFields == null) {
        for (LindenSearchField searchField : queryFields) {
          fields.add(new FlexibleQuery.FlexibleField(searchField.name, 1));
        }
      } else {
        for (LindenSearchField field : globalFields) {
          fields.add(new FlexibleQuery.FlexibleField(field.name, 1));
        }
      }
    }
    return fields;
  }
}
