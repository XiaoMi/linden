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

package com.xiaomi.linden.lucene.search;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FieldCacheSanityChecker;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class LindenFieldCacheImpl implements FieldCache {

  private static Logger LOGGER = LoggerFactory.getLogger(LindenFieldCacheImpl.class);
  // standard analyzer will take '1,2' as one token.
  private static final char SPLIT_CHAR = '|';

  private Map<Class<?>, Cache> caches;

  static public class DocId {

    private int ord;
    private int doc;

    public DocId(int ord, int doc) {
      this.ord = ord;
      this.doc = doc;
    }

    public int getOrd() {
      return ord;
    }

    public int getDoc() {
      return doc;
    }

    @Override
    public String toString() {
      return "ord:" + ord + " doc:" + doc;
    }
  }

  public static LindenFieldCacheImpl DEFAULT = new LindenFieldCacheImpl();

  LindenFieldCacheImpl() {
    init();
  }

  private synchronized void init() {
    caches = new HashMap<>();
    caches.put(UIDCache.class, new UIDCache(this));
    caches.put(IntList.class, new IntListCache(this));
    caches.put(LongList.class, new LongListCache(this));
    caches.put(FloatList.class, new FloatListCache(this));
    caches.put(DoubleList.class, new DoubleListCache(this));
    caches.put(StringList.class, new StringListCache(this));
    caches.put(StoredStrings.class, new StoredStringCache(this));
    caches.put(NotNullFieldDocIdSet.class, new NotNullFieldDocIdSetCache(this));
  }

  public UIDMaps getUIDMaps(IndexReaderContext topReaderContext, String uidField) throws IOException {
    PerReaderUIDMaps[] uidMapsArray = new PerReaderUIDMaps[topReaderContext.leaves().size()];
    for (int i = 0; i < topReaderContext.leaves().size(); ++i) {
      uidMapsArray[i] = (PerReaderUIDMaps) caches.get(UIDCache.class)
          .get(topReaderContext.leaves().get(i).reader(), new CacheKey(uidField, null), false);
    }
    return new UIDMaps(uidMapsArray);
  }

  public IntList getIntList(AtomicReader reader, String field) throws IOException {
    return (IntList) caches.get(IntList.class).get(reader, new CacheKey(field, null), false);
  }

  public LongList getLongList(AtomicReader reader, String field) throws IOException {
    return (LongList) caches.get(LongList.class).get(reader, new CacheKey(field, null), false);
  }

  public FloatList getFloatList(AtomicReader reader, String field) throws IOException {
    return (FloatList) caches.get(FloatList.class).get(reader, new CacheKey(field, null), false);
  }

  public DoubleList getDoubleList(AtomicReader reader, String field) throws IOException {
    return (DoubleList) caches.get(DoubleList.class).get(reader, new CacheKey(field, null), false);
  }

  public StringList getStringList(AtomicReader reader, String field) throws IOException {
    return (StringList) caches.get(StringList.class).get(reader, new CacheKey(field, null), false);
  }

  public StoredStrings getStoredStrings(AtomicReader reader, String field) throws IOException {
    return (StoredStrings) caches.get(StoredStrings.class).get(reader, new CacheKey(field, null), false);
  }

  public NotNullFieldDocIdSet getNotNullFieldDocIdSet(AtomicReader reader, String field) throws IOException {
    return (NotNullFieldDocIdSet) caches.get(NotNullFieldDocIdSet.class).get(reader, new CacheKey(field, null), false);
  }

  final SegmentReader.CoreClosedListener purgeCore = new SegmentReader.CoreClosedListener() {
    @Override
    public void onClose(Object ownerCoreCacheKey) {
      LindenFieldCacheImpl.this.purgeByCacheKey(ownerCoreCacheKey);
    }
  };

  final IndexReader.ReaderClosedListener purgeReader = new IndexReader.ReaderClosedListener() {
    @Override
    public void onClose(IndexReader owner) {
      assert owner instanceof AtomicReader;
      LindenFieldCacheImpl.this.purgeByCacheKey(((AtomicReader) owner).getCoreCacheKey());
    }
  };

  private void initReader(AtomicReader reader) {
    if (reader instanceof SegmentReader) {
      reader.addCoreClosedListener(purgeCore);
    } else {
      // we have a slow reader of some sort, try to register a purge event
      // rather than relying on gc:
      Object key = reader.getCoreCacheKey();
      if (key instanceof AtomicReader) {
        ((AtomicReader) key).addReaderClosedListener(purgeReader);
      } else {
        // last chance
        reader.addReaderClosedListener(purgeReader);
      }
    }
  }

  @Override
  public Bits getDocsWithField(AtomicReader reader, String field) throws IOException {
    return FieldCache.DEFAULT.getDocsWithField(reader, field);
  }

  @Override
  public Bytes getBytes(AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return FieldCache.DEFAULT.getBytes(reader, field, setDocsWithField);
  }

  @Override
  public Bytes getBytes(AtomicReader reader, String field, ByteParser parser, boolean setDocsWithField)
      throws IOException {
    return FieldCache.DEFAULT.getBytes(reader, field, parser, setDocsWithField);
  }

  @Override
  public Shorts getShorts(AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return FieldCache.DEFAULT.getShorts(reader, field, setDocsWithField);
  }

  @Override
  public Shorts getShorts(AtomicReader reader, String field, ShortParser parser, boolean setDocsWithField)
      throws IOException {
    return FieldCache.DEFAULT.getShorts(reader, field, parser, setDocsWithField);
  }

  @Override
  public Ints getInts(AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return FieldCache.DEFAULT.getInts(reader, field, setDocsWithField);
  }

  @Override
  public Ints getInts(AtomicReader reader, String field, IntParser parser, boolean setDocsWithField)
      throws IOException {
    return FieldCache.DEFAULT.getInts(reader, field, parser, setDocsWithField);
  }

  @Override
  public Floats getFloats(AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return FieldCache.DEFAULT.getFloats(reader, field, setDocsWithField);
  }

  @Override
  public Floats getFloats(AtomicReader reader, String field, FloatParser parser, boolean setDocsWithField)
      throws IOException {
    return FieldCache.DEFAULT.getFloats(reader, field, parser, setDocsWithField);
  }

  @Override
  public Longs getLongs(AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return FieldCache.DEFAULT.getLongs(reader, field, setDocsWithField);
  }

  @Override
  public Longs getLongs(AtomicReader reader, String field, LongParser parser, boolean setDocsWithField)
      throws IOException {
    return FieldCache.DEFAULT.getLongs(reader, field, parser, setDocsWithField);
  }

  @Override
  public Doubles getDoubles(AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return FieldCache.DEFAULT.getDoubles(reader, field, setDocsWithField);
  }

  @Override
  public Doubles getDoubles(AtomicReader reader, String field, DoubleParser parser, boolean setDocsWithField)
      throws IOException {
    return FieldCache.DEFAULT.getDoubles(reader, field, parser, setDocsWithField);
  }

  @Override
  public BinaryDocValues getTerms(AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return FieldCache.DEFAULT.getTerms(reader, field, setDocsWithField);
  }

  @Override
  public BinaryDocValues getTerms(AtomicReader reader, String field, boolean setDocsWithField,
                                  float acceptableOverheadRatio) throws IOException {
    return FieldCache.DEFAULT.getTerms(reader, field, setDocsWithField, acceptableOverheadRatio);
  }

  @Override
  public SortedDocValues getTermsIndex(AtomicReader reader, String field) throws IOException {
    return FieldCache.DEFAULT.getTermsIndex(reader, field);
  }

  @Override
  public SortedDocValues getTermsIndex(AtomicReader reader, String field, float acceptableOverheadRatio)
      throws IOException {
    return FieldCache.DEFAULT.getTermsIndex(reader, field, acceptableOverheadRatio);
  }

  @Override
  public SortedSetDocValues getDocTermOrds(AtomicReader reader, String field) throws IOException {
    return FieldCache.DEFAULT.getDocTermOrds(reader, field);
  }

  @Override
  public CacheEntry[] getCacheEntries() {
    return new CacheEntry[0];
  }

  public void purgeAllCaches() {
    init();
  }

  public void purgeByCacheKey(Object coreCacheKey) {
    for (Cache c : caches.values()) {
      c.purgeByCacheKey(coreCacheKey);
    }
  }

  public void setInfoStream(PrintStream stream) {
  }

  public PrintStream getInfoStream() {
    return null;
  }

  abstract static class Cache {

    Cache(LindenFieldCacheImpl wrapper) {
      this.wrapper = wrapper;
    }

    final LindenFieldCacheImpl wrapper;

    final Map<Object, Map<CacheKey, Accountable>> readerCache = new WeakHashMap<>();

    protected abstract Accountable createValue(AtomicReader reader, CacheKey key, boolean setDocsWithField)
        throws IOException;

    /**
     * Remove this reader from the cache, if present.
     */
    public void purgeByCacheKey(Object coreCacheKey) {
      synchronized (readerCache) {
        readerCache.remove(coreCacheKey);
      }
    }

    /**
     * Sets the key to the value for the provided reader;
     * if the key is already set then this doesn't change it.
     */
    public void put(AtomicReader reader, CacheKey key, Accountable value) {
      final Object readerKey = reader.getCoreCacheKey();
      synchronized (readerCache) {
        Map<CacheKey, Accountable> innerCache = readerCache.get(readerKey);
        if (innerCache == null) {
          // First time this reader is using FieldCache
          innerCache = new HashMap<>();
          readerCache.put(readerKey, innerCache);
          wrapper.initReader(reader);
        }
        if (innerCache.get(key) == null) {
          innerCache.put(key, value);
        } else {
          // Another thread beat us to it; leave the current
          // value
        }
      }
    }

    public Accountable get(AtomicReader reader, CacheKey key, boolean setDocsWithField) throws IOException {
      Map<CacheKey, Accountable> innerCache;
      Accountable value;
      final Object readerKey = reader.getCoreCacheKey();
      synchronized (readerCache) {
        innerCache = readerCache.get(readerKey);
        if (innerCache == null) {
          // First time this reader is using FieldCache
          innerCache = new HashMap<>();
          readerCache.put(readerKey, innerCache);
          wrapper.initReader(reader);
          value = null;
        } else {
          value = innerCache.get(key);
        }
        if (value == null) {
          value = new CreationPlaceholder();
          innerCache.put(key, value);
        }
      }
      if (value instanceof CreationPlaceholder) {
        synchronized (value) {
          CreationPlaceholder progress = (CreationPlaceholder) value;
          if (progress.value == null) {
            long start = System.currentTimeMillis();
            progress.value = createValue(reader, key, setDocsWithField);
            long end = System.currentTimeMillis();
            LOGGER.info("createValue for {} cache in LindenFieldCacheImpl took {} ms", key.field, (end - start));
            synchronized (readerCache) {
              innerCache.put(key, progress.value);
            }

            // Only check if key.custom (the parser) is
            // non-null; else, we check twice for a single
            // call to FieldCache.getXXX
            if (key.custom != null && wrapper != null) {
              final PrintStream infoStream = wrapper.getInfoStream();
              if (infoStream != null) {
                printNewInsanity(infoStream, progress.value);
              }
            }
          }
          return progress.value;
        }
      }
      return value;
    }

    private void printNewInsanity(PrintStream infoStream, Object value) {
      final FieldCacheSanityChecker.Insanity[] insanities = FieldCacheSanityChecker.checkSanity(wrapper);
      for (int i = 0; i < insanities.length; i++) {
        final FieldCacheSanityChecker.Insanity insanity = insanities[i];
        final CacheEntry[] entries = insanity.getCacheEntries();
        for (int j = 0; j < entries.length; j++) {
          if (entries[j].getValue() == value) {
            // OK this insanity involves our entry
            infoStream.println("WARNING: new FieldCache insanity created\nDetails: " + insanity.toString());
            infoStream.println("\nStack:\n");
            new Throwable().printStackTrace(infoStream);
            break;
          }
        }
      }
    }
  }

  /**
   * Expert: Every composite-key in the internal cache is of this type.
   */
  static class CacheKey {

    final String field;        // which Field
    final Object custom;       // which custom comparator or parser

    /**
     * Creates one of these objects for a custom comparator/parser.
     */
    CacheKey(String field, Object custom) {
      this.field = field;
      this.custom = custom;
    }

    /**
     * Two of these are equal iff they reference the same field and type.
     */
    @Override
    public boolean equals(Object o) {
      if (o instanceof CacheKey) {
        CacheKey other = (CacheKey) o;
        if (other.field.equals(field)) {
          if (other.custom == null) {
            if (custom == null) {
              return true;
            }
          } else if (other.custom.equals(custom)) {
            return true;
          }
        }
      }
      return false;
    }

    /**
     * Composes a hashcode based on the field and type.
     */
    @Override
    public int hashCode() {
      return field.hashCode() ^ (custom == null ? 0 : custom.hashCode());
    }
  }

  /**
   * Placeholder indicating creation of this cache is currently in-progress.
   */
  public static final class CreationPlaceholder implements Accountable {

    Accountable value;

    @Override
    public long ramBytesUsed() {
      // don't call on the in-progress value, might make things angry.
      return RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }
  }

  private static abstract class Uninvert {

    public Bits docsWithField;

    public void uninvert(AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
      final int maxDoc = reader.maxDoc();
      Terms terms = reader.terms(field);
      if (terms != null) {
        if (setDocsWithField) {
          final int termsDocCount = terms.getDocCount();
          assert termsDocCount <= maxDoc;
          if (termsDocCount == maxDoc) {
            // Fast case: all docs have this field:
            docsWithField = new Bits.MatchAllBits(maxDoc);
            setDocsWithField = false;
          }
        }

        final TermsEnum termsEnum = termsEnum(terms);

        DocsEnum docs = null;
        FixedBitSet docsWithField = null;
        while (true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          visitTerm(term);
          docs = termsEnum.docs(null, docs, DocsEnum.FLAG_NONE);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            visitDoc(docID);
            if (setDocsWithField) {
              if (docsWithField == null) {
                // Lazy init
                this.docsWithField = docsWithField = new FixedBitSet(maxDoc);
              }
              docsWithField.set(docID);
            }
          }
        }
      }
    }

    protected abstract TermsEnum termsEnum(Terms terms) throws IOException;

    protected abstract void visitTerm(BytesRef term);

    protected abstract void visitDoc(int docID);
  }

  static final class UIDCache extends Cache {

    UIDCache(LindenFieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(final AtomicReader reader, CacheKey key, boolean setDocsWithField)
        throws IOException {
      final Map<String, Integer> uidMap = new HashMap<>();

      Uninvert u = new Uninvert() {
        private String currentValue;

        @Override
        public void visitTerm(BytesRef term) {
          currentValue = term.utf8ToString();
        }

        @Override
        public void visitDoc(int docID) {
          uidMap.put(currentValue, docID);
        }

        @Override
        protected TermsEnum termsEnum(Terms terms) throws IOException {
          return terms.iterator(null);
        }
      };
      u.uninvert(reader, key.field, setDocsWithField);
      return new PerReaderUIDMaps(reader.getContext().ord, uidMap);
    }
  }

  static public class PerReaderUIDMaps implements Accountable {

    private final Map<String, Integer> uidMaps;
    private int ord;

    public PerReaderUIDMaps(int ord, Map<String, Integer> uidMaps) {
      this.uidMaps = uidMaps;
      this.ord = ord;
    }

    public Integer get(String uid) {
      return uidMaps.get(uid);
    }

    public int getOrd() {
      return ord;
    }

    @Override
    public long ramBytesUsed() {
      //todo
      return RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_INT;
    }
  }

  static public class UIDMaps {

    private PerReaderUIDMaps[] uidMapsArray;

    public UIDMaps(PerReaderUIDMaps[] uidMapsArray) {
      this.uidMapsArray = uidMapsArray;
    }

    public DocId get(String uid) {
      for (PerReaderUIDMaps uidMaps : uidMapsArray) {
        Integer doc = uidMaps.get(uid);
        if (doc != null) {
          return new DocId(uidMaps.getOrd(), doc);
        }
      }
      return null;
    }
  }

  static public class IntListCache extends Cache {

    IntListCache(LindenFieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(final AtomicReader reader, CacheKey key, boolean setDocsWithField)
        throws IOException {
      int maxDoc = reader.maxDoc();

      final int[][] matrix = new int[maxDoc][];
      BinaryDocValues valuesIn = reader.getBinaryDocValues(key.field);
      if (valuesIn == null) {
        for (int i = 0; i < maxDoc; ++i) {
          matrix[i] = new int[0];
        }
        return new IntList(matrix);
      }
      for (int i = 0; i < maxDoc; ++i) {
        String str = valuesIn.get(i).utf8ToString();
        if (StringUtils.isEmpty(str)) {
          matrix[i] = new int[0];
          continue;
        }
        JSONArray array = JSON.parseArray(str);
        matrix[i] = new int[array.size()];
        for (int j = 0; j < array.size(); ++j) {
          matrix[i][j] = array.getInteger(j);
        }
      }
      return new IntList(matrix);
    }
  }

  static public class LongListCache extends Cache {

    LongListCache(LindenFieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(final AtomicReader reader, CacheKey key, boolean setDocsWithField)
        throws IOException {
      int maxDoc = reader.maxDoc();

      final long[][] matrix = new long[maxDoc][];
      BinaryDocValues valuesIn = reader.getBinaryDocValues(key.field);
      if (valuesIn == null) {
        for (int i = 0; i < maxDoc; ++i) {
          matrix[i] = new long[0];
        }
        return new LongList(matrix);
      }
      for (int i = 0; i < maxDoc; ++i) {
        String str = valuesIn.get(i).utf8ToString();
        if (StringUtils.isEmpty(str)) {
          matrix[i] = new long[0];
          continue;
        }
        JSONArray array = JSON.parseArray(str);
        matrix[i] = new long[array.size()];
        for (int j = 0; j < array.size(); ++j) {
          matrix[i][j] = array.getInteger(j);
        }
      }
      return new LongList(matrix);
    }
  }

  static public class FloatListCache extends Cache {

    FloatListCache(LindenFieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(final AtomicReader reader, CacheKey key, boolean setDocsWithField)
        throws IOException {
      int maxDoc = reader.maxDoc();

      final float[][] matrix = new float[maxDoc][];
      BinaryDocValues valuesIn = reader.getBinaryDocValues(key.field);
      if (valuesIn == null) {
        for (int i = 0; i < maxDoc; ++i) {
          matrix[i] = new float[0];
        }
        return new FloatList(matrix);
      }
      for (int i = 0; i < maxDoc; ++i) {
        String str = valuesIn.get(i).utf8ToString();
        if (StringUtils.isEmpty(str)) {
          matrix[i] = new float[0];
          continue;
        }
        JSONArray array = JSON.parseArray(str);
        matrix[i] = new float[array.size()];
        for (int j = 0; j < array.size(); ++j) {
          matrix[i][j] = array.getFloat(j);
        }
      }
      return new FloatList(matrix);
    }
  }

  static public class DoubleListCache extends Cache {

    DoubleListCache(LindenFieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(final AtomicReader reader, CacheKey key, boolean setDocsWithField)
        throws IOException {
      int maxDoc = reader.maxDoc();

      final double[][] matrix = new double[maxDoc][];
      BinaryDocValues valuesIn = reader.getBinaryDocValues(key.field);
      if (valuesIn == null) {
        for (int i = 0; i < maxDoc; ++i) {
          matrix[i] = new double[0];
        }
        return new DoubleList(matrix);
      }
      for (int i = 0; i < maxDoc; ++i) {
        String str = valuesIn.get(i).utf8ToString();
        if (StringUtils.isEmpty(str)) {
          matrix[i] = new double[0];
          continue;
        }
        JSONArray array = JSON.parseArray(str);
        matrix[i] = new double[array.size()];
        for (int j = 0; j < array.size(); ++j) {
          matrix[i][j] = array.getFloat(j);
        }
      }
      return new DoubleList(matrix);
    }
  }

  static public class StringListCache extends Cache {

    StringListCache(LindenFieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(final AtomicReader reader, CacheKey key, boolean setDocsWithField)
        throws IOException {
      int maxDoc = reader.maxDoc();

      final String[][] matrix = new String[maxDoc][];
      BinaryDocValues valuesIn = reader.getBinaryDocValues(key.field);
      if (valuesIn == null) {
        for (int i = 0; i < maxDoc; ++i) {
          matrix[i] = new String[0];
        }
        return new StringList(matrix);
      }
      for (int i = 0; i < maxDoc; ++i) {
        String str = valuesIn.get(i).utf8ToString();
        if (StringUtils.isEmpty(str)) {
          matrix[i] = new String[0];
          continue;
        }
        JSONArray array = JSON.parseArray(str);
        matrix[i] = new String[array.size()];
        for (int j = 0; j < array.size(); ++j) {
          matrix[i][j] = array.getString(j);
        }
      }
      return new StringList(matrix);
    }
  }

  public static class LongList implements Accountable {

    long[][] matrix;

    public LongList(long[][] matrix) {
      this.matrix = matrix;
    }

    public long[] get(int doc) {
      return matrix[doc];
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  public static class IntList implements Accountable {

    int[][] matrix;

    public IntList(int[][] matrix) {
      this.matrix = matrix;
    }

    public int[] get(int doc) {
      return matrix[doc];
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  public static class FloatList implements Accountable {

    float[][] matrix;

    public FloatList(float[][] matrix) {
      this.matrix = matrix;
    }

    public float[] get(int doc) {
      return matrix[doc];
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  public static class DoubleList implements Accountable {

    double[][] matrix;

    public DoubleList(double[][] matrix) {
      this.matrix = matrix;
    }

    public double[] get(int doc) {
      return matrix[doc];
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  public static class StringList implements Accountable {

    String[][] matrix;

    public StringList(String[][] matrix) {
      this.matrix = matrix;
    }

    public String[] get(int doc) {
      return matrix[doc];
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  static public class StoredStringCache extends Cache {

    StoredStringCache(LindenFieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(final AtomicReader reader, CacheKey key, boolean setDocsWithField)
        throws IOException {
      return new StoredStrings(reader.maxDoc(), key.field);
    }
  }

  public static class StoredStrings implements Accountable {

    final String field;
    final Set<String> fieldSet;
    final String[] array;

    public StoredStrings(int maxDoc, String fieldName) {
      field = fieldName;
      fieldSet = new HashSet<>(Collections.singletonList(field));
      array = new String[maxDoc];
    }

    public String get(AtomicReader reader, int doc) {
      if (array[doc] != null) {
        return array[doc];
      }

      String value;
      try {
        value = reader.document(doc, fieldSet).get(field);
      } catch (IOException e) {
        value = "";
      }
      if (value == null) {
        value = "";
      }
      array[doc] = value;
      return value;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  public static class NotNullFieldDocIdSet extends DocIdSet {

    private DocIdSet underlying;

    public NotNullFieldDocIdSet(DocIdSet docIdSet) {
      this.underlying = docIdSet;
    }

    public DocIdSet getDocIdSet() {
      return underlying;
    }

    @Override
    public Bits bits() throws IOException {
      return underlying.bits();
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return underlying.iterator();
    }
  }


  public static class NotNullFieldDocIdSetCache extends Cache {

    NotNullFieldDocIdSetCache(LindenFieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(final AtomicReader reader, CacheKey key, boolean setDocsWithField)
        throws IOException {
      final String field = key.field;
      final int maxDoc = reader.maxDoc();

      FixedBitSet bitSet = new FixedBitSet(maxDoc);
      NotNullFieldDocIdSet res = new NotNullFieldDocIdSet(bitSet);
      final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(key.field);
      if (fieldInfo == null) {
        // field does not exist or has no value
        return res;
      } else if (fieldInfo.hasDocValues()) {
        Bits bits = reader.getDocsWithField(key.field);
        if (bits == null) {
          return res;
        } else {
          for (int i = 0; i < maxDoc; ++i) {
            if (bits.get(i)) {
              bitSet.set(i);
            }
          }
          return res;
        }
      } else if (!fieldInfo.isIndexed()) {
        return res;
      }

      // Visit all docs that have terms for this field
      Terms terms = reader.terms(field);
      if (terms != null) {
        final int termsDocCount = terms.getDocCount();
        assert termsDocCount <= maxDoc;
        if (termsDocCount == maxDoc) {
          // Fast case: all docs have this field
          bitSet.set(0, maxDoc);
          return res;
        }

        final TermsEnum termsEnum = terms.iterator(null);
        DocsEnum docs = null;
        while (true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          docs = termsEnum.docs(null, docs, DocsEnum.FLAG_NONE);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            // set this bit
            bitSet.set(docID);
          }
        }
      }
      return res;
    }
  }
}
