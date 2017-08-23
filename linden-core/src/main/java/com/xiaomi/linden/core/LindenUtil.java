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

package com.xiaomi.linden.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;

import com.xiaomi.linden.common.schema.LindenSchemaConf;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenType;

public class LindenUtil {

  public static Float getFieldFloatValue(List<AtomicReaderContext> leaves, int docId, String fieldName)
      throws IOException {
    AtomicReaderContext atomicReaderContext = leaves.get(ReaderUtil.subIndex(docId, leaves));
    FieldCache.Floats floats = FieldCache.DEFAULT.getFloats(atomicReaderContext.reader(), fieldName, false);
    return floats.get(docId - atomicReaderContext.docBase);
  }

  public static Double getFieldDoubleValue(List<AtomicReaderContext> leaves, int docId, String fieldName)
      throws IOException {
    AtomicReaderContext atomicReaderContext = leaves.get(ReaderUtil.subIndex(docId, leaves));
    FieldCache.Doubles doubles = FieldCache.DEFAULT.getDoubles(atomicReaderContext.reader(), fieldName, false);
    return doubles.get(docId - atomicReaderContext.docBase);
  }

  public static String getFieldStringValue(List<AtomicReaderContext> leaves, int docId, String fieldName)
      throws IOException {
    AtomicReaderContext atomicReaderContext = leaves.get(ReaderUtil.subIndex(docId, leaves));
    BinaryDocValues terms = FieldCache.DEFAULT.getTerms(atomicReaderContext.reader(), fieldName, false);
    return terms.get(docId - atomicReaderContext.docBase).utf8ToString();
  }

  public static Long getFieldLongValue(List<AtomicReaderContext> leaves, int docId, String fieldName)
      throws IOException {
    AtomicReaderContext atomicReaderContext = leaves.get(ReaderUtil.subIndex(docId, leaves));
    FieldCache.Longs longs = FieldCache.DEFAULT.getLongs(atomicReaderContext.reader(), fieldName, false);
    return longs.get(docId - atomicReaderContext.docBase);
  }

  public static Integer getFieldIntValue(List<AtomicReaderContext> leaves, int docId, String fieldName)
      throws IOException {
    AtomicReaderContext atomicReaderContext = leaves.get(ReaderUtil.subIndex(docId, leaves));
    FieldCache.Ints ints = FieldCache.DEFAULT.getInts(atomicReaderContext.reader(), fieldName, false);
    return ints.get(docId - atomicReaderContext.docBase);
  }

  // We are not sure whether dynamic schema string field is tokenized or not
  public static boolean possibleTokenizedString(LindenFieldSchema fieldSchema) {
    return fieldSchema.getType() == LindenType.STRING && (fieldSchema.isTokenized() || fieldSchema.isDynamicSchema());
  }

  /**
   * Get fields by doc id.
   *
   * @param indexSearcher The IndexSearcher
   * @param docId         Doc ID.
   * @param id            Id field value
   * @param sourceFields  Specify the fields, if null get all fields values.
   * @param config        the lindenConfig for search
   * @return JSON String which contains field values.
   * @throws IOException
   */

  public static String getSource(IndexSearcher indexSearcher, int docId, String id, List<String> sourceFields,
                                 LindenConfig config) throws IOException {
    List<AtomicReaderContext> leaves = indexSearcher.getIndexReader().leaves();
    int idx = ReaderUtil.subIndex(docId, leaves);
    AtomicReaderContext atomicReaderContext = leaves.get(idx);
    AtomicReader reader = atomicReaderContext.reader();
    int locDocId = docId - atomicReaderContext.docBase;
    JSONObject src = new JSONObject();
    String idFieldName = config.getSchema().getId();
    if (id != null) {
      src.put(idFieldName, id);
    } else {
      src.put(idFieldName, FieldCache.DEFAULT.getTerms(reader, idFieldName, false).get(locDocId).utf8ToString());
    }

    List<LindenFieldSchema> fields = new ArrayList<>();
    if (sourceFields != null && !sourceFields.isEmpty()) {
      for (String sourceField : sourceFields) {
        if (sourceField.equals(idFieldName)) {
          continue;
        }
        LindenFieldSchema fieldSchema = config.getFieldSchema(sourceField);
        fields.add(fieldSchema);
      }
    } else {
      fields.addAll(config.getSchema().getFields());
    }

    Map<String, LindenFieldSchema> storedFields = new HashMap<>();
    for (LindenFieldSchema fieldSchema : fields) {
      String name = fieldSchema.getName();
      boolean fieldCache = false;
      if (fieldSchema.isMulti()) {
        /**
         * multi-field has multiple values, each value is indexed to the document according to field type
         * multi-field source value is in JSONArray format, something like "["MI4","MI Note","RedMI3"]"
         * multi-field source value is stored in BinaryDocValues
         */
        String blob = FieldCache.DEFAULT.getTerms(reader, name, false).get(locDocId).utf8ToString();
        if (StringUtils.isNotEmpty(blob)) {
          src.put(name, JSON.parseArray(blob));
        }
      } else if (fieldSchema.isDocValues()) {
        fieldCache = true;
      } else if (fieldSchema.isIndexed() && fieldSchema.isStored()) {
        // field cache doesn't support tokenized string field
        if (config.isEnableSourceFieldCache() && !possibleTokenizedString(fieldSchema)) {
          fieldCache = true;
        } else {
          storedFields.put(name, fieldSchema);
        }
      } else if (fieldSchema.isIndexed()) {
        if (!possibleTokenizedString(fieldSchema)) {
          fieldCache = true;
        }
      } else if (fieldSchema.isStored()) {
        storedFields.put(name, fieldSchema);
      }

      if (fieldCache) {
        Object val;
        switch (fieldSchema.getType()) {
          case STRING:
          case FACET:
            val = FieldCache.DEFAULT.getTerms(reader, name, false).get(locDocId).utf8ToString();
            String v = (String) val;
            fieldCache = !v.isEmpty() || actualContain(reader, name, locDocId);
            break;
          case INTEGER:
            val = FieldCache.DEFAULT.getInts(reader, name, false).get(locDocId);
            fieldCache = ((int) val) != 0 || actualContain(reader, name, locDocId);
            break;
          case LONG:
            val = FieldCache.DEFAULT.getLongs(reader, name, false).get(locDocId);
            fieldCache = ((long) val != 0) || actualContain(reader, name, locDocId);
            break;
          case FLOAT:
            val = FieldCache.DEFAULT.getFloats(reader, name, false).get(locDocId);
            fieldCache = ((float) val != 0) || actualContain(reader, name, locDocId);
            break;
          case DOUBLE:
            val = FieldCache.DEFAULT.getDoubles(reader, name, false).get(locDocId);
            fieldCache = ((double) val != 0) || actualContain(reader, name, locDocId);
            break;
          default:
            throw new IllegalStateException("Unsupported linden type");
        }
        if (fieldCache) {
          src.put(name, val);
        }
      }
    }

    if (!storedFields.isEmpty())

    {
      Document doc = indexSearcher.doc(docId, storedFields.keySet());
      for (IndexableField field : doc.getFields()) {
        String name = field.name();
        LindenFieldSchema schema = storedFields.get(name);
        Object obj = src.get(name);
        Object val = parseLindenValue(field.stringValue(), storedFields.get(name).getType());
        if (obj == null) {
          if (schema.isMulti()) {
            JSONArray array = new JSONArray();
            array.add(val);
            src.put(name, array);
          } else {
            src.put(name, val);
          }
        } else if (obj instanceof JSONArray) {
          ((JSONArray) obj).add(val);
        } else {
          JSONArray array = new JSONArray();
          array.add(obj);
          array.add(val);
          src.put(name, array);
        }
      }
    }
    return src.toJSONString();
  }

  public static Object parseLindenValue(String value, LindenType type) {
    switch (type) {
      case STRING:
      case FACET:
        return value;
      case INTEGER:
        return Integer.parseInt(value);
      case LONG:
        return Long.parseLong(value);
      case FLOAT:
        return Float.parseFloat(value);
      case DOUBLE:
        return Double.parseDouble(value);
      default:
        throw new IllegalStateException("Unsupported linden type");
    }
  }

  private static boolean actualContain(AtomicReader reader, String field, int locDocId) {
    try {
      // index really contains such field of this doc
      return FieldCache.DEFAULT.getDocsWithField(reader, field).get(locDocId);
    } catch (IOException e) {
      return false;
    }
  }

  public static LindenFieldSchema parseDynamicFieldSchema(String name) {
    LindenFieldSchema fieldSchema = new LindenFieldSchema();
    // Linden dynamic field is always indexed and stored
    fieldSchema.setStored(true);
    fieldSchema.setIndexed(true);
    fieldSchema.setDynamicSchema(true);
    int idx = name.lastIndexOf(LindenSchemaConf.DYNAMIC_TYPE_SEPARATOR);
    String tag = idx >= 0 ? name.substring(idx + 1) : "";
    switch (tag.toLowerCase()) {
      case LindenSchemaConf.INT:
        fieldSchema.setName(name.substring(0, idx));
        fieldSchema.setType(LindenType.INTEGER);
        break;
      case LindenSchemaConf.LONG:
        fieldSchema.setName(name.substring(0, idx));
        fieldSchema.setType(LindenType.LONG);
        break;
      case LindenSchemaConf.DOUBLE:
        fieldSchema.setName(name.substring(0, idx));
        fieldSchema.setType(LindenType.DOUBLE);
        break;
      case LindenSchemaConf.FLOAT:
        fieldSchema.setName(name.substring(0, idx));
        fieldSchema.setType(LindenType.FLOAT);
        break;
      case LindenSchemaConf.STRING:
        fieldSchema.setName(name.substring(0, idx));
        fieldSchema.setType(LindenType.STRING);
        break;
      case LindenSchemaConf.FACET:
        fieldSchema.setName(name.substring(0, idx));
        fieldSchema.setType(LindenType.FACET);
        break;
      default:
        // default field type is STRING
        fieldSchema.setName(name);
        fieldSchema.setType(LindenType.STRING);
        break;
    }
    return fieldSchema;
  }
}