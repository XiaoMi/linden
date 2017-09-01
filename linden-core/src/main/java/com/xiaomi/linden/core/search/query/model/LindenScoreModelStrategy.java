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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.FieldCache;

import com.xiaomi.linden.lucene.search.LindenFieldCacheImpl;
import com.xiaomi.linden.thrift.common.Coordinate;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenInputParam;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenScoreModel;
import com.xiaomi.linden.thrift.common.LindenType;

public abstract class LindenScoreModelStrategy {
  private int doc;
  private float rawScore;
  protected AtomicReaderContext context;
  private String explanation;
  private boolean isExplain = false;
  private Map<String, LindenFieldSchema> fieldSchemaMap;
  private LindenScoreModel scoreModel;

  public FieldValues registerCustomCacheWrapper(CustomCacheWrapper cacheWrapper) throws IOException {
    cacheWrapper.preProcess(context, this);
    cacheWrapper.init();
    return new FieldValues<>(this, cacheWrapper);
  }

  public void prepare(int doc, float score, boolean isExplain) {
    this.doc = doc;
    rawScore = score;
    this.isExplain = isExplain;
  }

  public float score() {
    return rawScore;
  }

  public int doc() {
    return doc;
  }

  public void preProcess(AtomicReaderContext context, LindenSchema schema, LindenScoreModel scoreModel) {
    this.context = context;
    fieldSchemaMap = new HashMap<>();
    for (LindenFieldSchema fieldSchema : schema.getFields()) {
      fieldSchemaMap.put(fieldSchema.getName(), fieldSchema);
    }
    // add id field
    if (!fieldSchemaMap.containsKey(schema.getId())) {
      LindenFieldSchema idFieldSchema = new LindenFieldSchema();
      idFieldSchema.setName(schema.getId());
      idFieldSchema.setType(LindenType.STRING);
      idFieldSchema.setIndexed(true);
      idFieldSchema.setOmitNorms(true);
      idFieldSchema.setOmitFreqs(true);
      idFieldSchema.setStored(true);
      idFieldSchema.setTokenized(false);
      fieldSchemaMap.put(schema.getId(), idFieldSchema);
    }
    this.scoreModel = scoreModel;
  }

  public List<LindenInputParam> getParams() {
    return scoreModel.getParams();
  }

  protected boolean hasParam(int i) {
    List<LindenInputParam> scoreModelParams = scoreModel.getParams();
    return scoreModelParams.size() > i && scoreModelParams.get(i) != null && scoreModelParams.get(i).isSetValue();
  }

  public Coordinate getCoordinate() {
    return scoreModel.getCoordinate();
  }

  public synchronized FieldValues getFieldValues(String field) throws IOException {
    LindenFieldSchema fieldSchema = fieldSchemaMap.get(field);
    if (fieldSchema == null) {
      return null;
    }
    FieldValues fieldValues = null;
    if (fieldSchema.isMulti()) {
      switch (fieldSchema.getType()) {
        case INTEGER:
          fieldValues = new FieldValues<>(this, new IntListWrapper(fieldSchema.getName(), context.reader()));
          break;
        case LONG:
          fieldValues = new FieldValues<>(this, new LongListWrapper(fieldSchema.getName(), context.reader()));
          break;
        case FLOAT:
          fieldValues = new FieldValues<>(this, new FloatListWrapper(fieldSchema.getName(), context.reader()));
          break;
        case DOUBLE:
          fieldValues = new FieldValues<>(this, new DoubleListWrapper(fieldSchema.getName(), context.reader()));
          break;
        case STRING:
        case FACET:
          fieldValues = new FieldValues<>(this, new StringListWrapper(fieldSchema.getName(), context.reader()));
          break;
      }
    } else {
      switch (fieldSchema.getType()) {
        case INTEGER:
          fieldValues = new FieldValues<>(this, new IntsWrapper(fieldSchema.getName(), context.reader()));
          break;
        case LONG:
          fieldValues = new FieldValues<>(this, new LongsWrapper(fieldSchema.getName(), context.reader()));
          break;
        case FLOAT:
          fieldValues = new FieldValues<>(this, new FloatsWrapper(fieldSchema.getName(), context.reader()));
          break;
        case DOUBLE:
          fieldValues = new FieldValues<>(this, new DoublesWrapper(fieldSchema.getName(), context.reader()));
          break;
        case STRING:
        case FACET:
          if (fieldSchema.isDocValues() || (fieldSchema.isIndexed() && !fieldSchema.isTokenized())) {
            fieldValues = new FieldValues<>(this, new IndexedStringsWrapper(fieldSchema.getName(), context.reader()));
          } else if (fieldSchema.isStored()) {
            // StoredStringsWrapper may make search performance dropped
            // It is highly recommended to enable DocValues for tokenized string field
            // if you want to use the field in score model
            fieldValues = new FieldValues<>(this, new StoredStringsWrapper(fieldSchema.getName(), context.reader()));
          } else {
            throw new IOException("getFieldValues failed due to bad field type");
          }
          break;
      }
    }
    return fieldValues;
  }

  public void init() throws IOException {};
  public abstract double computeScore() throws IOException;

  public void writeExplanation(String format, Object... args) {
    if (isExplain) {
      explanation = String.format(format, args);
    }
  }

  public String getExplanation() {
    return explanation;
  }

  public static class FieldValues<T> {
    private LindenScoreModelStrategy strategy;
    private FieldWrapper<T> wrapper;
    public FieldValues(LindenScoreModelStrategy strategy, FieldWrapper<T> wrapper) {
      this.strategy = strategy;
      this.wrapper = wrapper;
    }
    public T get(int doc) {
      return wrapper.get(doc);
    }

    public T get() {
      return wrapper.get(strategy.doc());
    }
  }

  static interface FieldWrapper<T> {
    public T get(int doc);
  }

  static class IntsWrapper implements FieldWrapper<Integer> {
    private FieldCache.Ints ints;
    public IntsWrapper(String field, AtomicReader reader) throws IOException {
      ints = FieldCache.DEFAULT.getInts(reader, field, false);
    }

    @Override
    public Integer get(int doc) {
      return ints.get(doc);
    }
  }

  static class LongsWrapper implements FieldWrapper<Long> {
    private FieldCache.Longs longs;
    public LongsWrapper(String field, AtomicReader reader) throws IOException {
      longs = FieldCache.DEFAULT.getLongs(reader, field, false);
    }
    @Override
    public Long get(int doc) {
      return longs.get(doc);
    }
  }

  static class DoublesWrapper implements FieldWrapper<Double> {
    private FieldCache.Doubles doubles;
    public DoublesWrapper(String field, AtomicReader reader) throws IOException {
      doubles = FieldCache.DEFAULT.getDoubles(reader, field, false);
    }
    @Override
    public Double get(int doc) {
      return doubles.get(doc);
    }
  }

  static class FloatsWrapper implements FieldWrapper<Float> {
    private FieldCache.Floats floats;
    public FloatsWrapper(String field, AtomicReader reader) throws IOException {
      floats = FieldCache.DEFAULT.getFloats(reader, field, false);
    }
    @Override
    public Float get(int doc) {
      return floats.get(doc);
    }
  }

  static class IndexedStringsWrapper implements FieldWrapper<String> {
    private BinaryDocValues values;
    public IndexedStringsWrapper(String field, AtomicReader reader) throws IOException {
      values = FieldCache.DEFAULT.getTerms(reader, field, false);
    }
    @Override
    public String get(int doc) {
      return values.get(doc).utf8ToString();
    }
  }

  static class StoredStringsWrapper implements FieldWrapper<String> {
    private final AtomicReader reader;
    private LindenFieldCacheImpl.StoredStrings values;
    public StoredStringsWrapper(String field, AtomicReader reader) throws IOException {
      this.reader = reader;
      values = LindenFieldCacheImpl.DEFAULT.getStoredStrings(reader, field);
    }
    @Override
    public String get(int doc) {
      return values.get(reader, doc);
    }
  }

  static class IntListWrapper implements FieldWrapper<int[]> {
    private LindenFieldCacheImpl.IntList intList;
    public IntListWrapper(String field, AtomicReader reader) throws IOException {
      intList = LindenFieldCacheImpl.DEFAULT.getIntList(reader, field);
    }
    @Override
    public int[] get(int doc) {
      return intList.get(doc);
    }
  }

  static class LongListWrapper implements FieldWrapper<long[]> {
    private LindenFieldCacheImpl.LongList longList;
    public LongListWrapper(String field, AtomicReader reader) throws IOException {
      longList = LindenFieldCacheImpl.DEFAULT.getLongList(reader, field);
    }
    @Override
    public long[] get(int doc) {
      return longList.get(doc);
    }
  }

  static class FloatListWrapper implements FieldWrapper<float[]> {
    private LindenFieldCacheImpl.FloatList valueList;
    public FloatListWrapper(String field, AtomicReader reader) throws IOException {
      valueList = LindenFieldCacheImpl.DEFAULT.getFloatList(reader, field);
    }
    @Override
    public float[] get(int doc) {
      return valueList.get(doc);
    }
  }

  static class DoubleListWrapper implements FieldWrapper<double[]> {
    private LindenFieldCacheImpl.DoubleList valueList;
    public DoubleListWrapper(String field, AtomicReader reader) throws IOException {
      valueList = LindenFieldCacheImpl.DEFAULT.getDoubleList(reader, field);
    }
    @Override
    public double[] get(int doc) {
      return valueList.get(doc);
    }
  }

  static class StringListWrapper implements FieldWrapper<String[]> {
    private LindenFieldCacheImpl.StringList valueList;
    public StringListWrapper(String field, AtomicReader reader) throws IOException {
      valueList = LindenFieldCacheImpl.DEFAULT.getStringList(reader, field);
    }
    @Override
    public String[] get(int doc) {
      return valueList.get(doc);
    }
  }
}
