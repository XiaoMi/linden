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

import com.xiaomi.linden.plugin.LindenPluginFactory;

public class TieredMergePolicyFactory implements LindenPluginFactory<MergePolicy> {

  private static final String SEGMENTS_PER_TIER = "segments.per.tier";
  private static final String MAX_MERGE_AT_ONCE = "max.merge.at.once";

  @Override
  public MergePolicy getInstance(Map<String, String> config) throws IOException {
    TieredMergePolicy mergePolicy = new TieredMergePolicy();

    if (config.containsKey(SEGMENTS_PER_TIER)) {
      mergePolicy.setSegmentsPerTier(Double.valueOf(config.get(SEGMENTS_PER_TIER)));
    }
    if (config.containsKey(MAX_MERGE_AT_ONCE)) {
      mergePolicy.setMaxMergeAtOnce(Integer.valueOf(config.get(MAX_MERGE_AT_ONCE)));
    }
    return mergePolicy;
  }
}
