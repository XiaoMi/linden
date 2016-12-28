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

package com.xiaomi.linden.core.search.query.model;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

public abstract class CustomCacheWrapper implements LindenScoreModelStrategy.FieldWrapper<Object> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CustomCacheWrapper.class);

  private static final Map<Object, Map<Integer, Object>> cache = new WeakHashMap<>();

  private LindenScoreModelStrategy scoreModelStrategy;
  private AtomicReaderContext context;

  public void preProcess(AtomicReaderContext context, LindenScoreModelStrategy scoreModelStrategy) {
    this.scoreModelStrategy = scoreModelStrategy;
    this.context = context;
  }

  public LindenScoreModelStrategy.FieldValues getFieldValues(String field) throws IOException {
    return scoreModelStrategy.getFieldValues(field);
  }

  @Override
  public Object get(int doc) {
    Map<Integer, Object> innerCache = cache.get(context.reader().getCoreCacheKey());
    if (innerCache == null) {
      synchronized (cache) {
        innerCache = cache.get(context.reader().getCoreCacheKey());
        if (innerCache == null) {
          innerCache = new HashMap<>();
          for (int i = 0; i < context.reader().maxDoc(); ++i) {
            innerCache.put(i, createValue(i));
          }
          LOGGER.info("Create cache for context ord:{}, base:{}, max doc:{}.", context.ord, context.docBase, context.reader().maxDoc());
          cache.put(context.reader().getCoreCacheKey(), innerCache);
          context.reader().addCoreClosedListener(new AtomicReader.CoreClosedListener() {
            @Override
            public void onClose(Object ownerCoreCacheKey) {
              cache.remove(ownerCoreCacheKey);
            }
          });
        }
      }
    }
    return innerCache.get(doc);
  }

  public void init() throws IOException {}
  public abstract Object createValue(int doc);
}
