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

package com.xiaomi.linden.lucene.merge;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import com.xiaomi.linden.plugin.LindenPluginFactory;

public class SortingMergePolicyFactory implements LindenPluginFactory<MergePolicy> {

  private final static String SORT_FIELD = "sort.field";
  private final static String SORT_FIELD_TYPE = "sort.field.type";
  private final static String SORT_DESC = "sort.desc";

  @Override
  public MergePolicy getInstance(Map<String, String> params) throws IOException {
    String field = params.get(SORT_FIELD);
    SortField.Type sortFieldType = SortField.Type.DOC;
    if (params.containsKey(SORT_FIELD_TYPE)) {
      sortFieldType = SortField.Type.valueOf(params.get(SORT_FIELD_TYPE).toUpperCase());
    }

    if (sortFieldType == SortField.Type.DOC) {
      throw new IOException(
          "Relying on internal lucene DocIDs is not guaranteed to work, this is only an implementation detail.");
    }

    boolean desc = true;
    if (params.containsKey(SORT_DESC)) {
      try {
        desc = Boolean.valueOf(params.get(SORT_DESC));
      } catch (Exception e) {
        desc = true;
      }
    }
    SortField sortField = new SortField(field, sortFieldType, desc);
    Sort sort = new Sort(sortField);
    return new SortingMergePolicyDecorator(new TieredMergePolicy(), sort);
  }

}
