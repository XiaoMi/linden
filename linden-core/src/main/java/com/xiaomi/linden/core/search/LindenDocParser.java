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

package com.xiaomi.linden.core.search;

import com.google.common.base.Strings;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.thrift.common.Coordinate;
import com.xiaomi.linden.thrift.common.LindenDocument;
import com.xiaomi.linden.thrift.common.LindenField;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenType;

public class LindenDocParser {

  private static FieldType STORED_ONLY = new FieldType();

  static {
    STORED_ONLY.setStored(true);
    STORED_ONLY.freeze();
  }

  public static Document parse(LindenDocument lindenDoc, LindenConfig config) {
    if (!lindenDoc.isSetFields()) {
      return null;
    }
    Document doc = new Document();
    doc.add(new StringField(config.getSchema().getId(), lindenDoc.getId(), Field.Store.YES));
    for (LindenField field : lindenDoc.getFields()) {
      LindenFieldSchema schema = field.getSchema();
      Field.Store isStored = schema.isStored() ? Field.Store.YES : Field.Store.NO;
      String name = field.getSchema().getName();
      Object value;

      if (!schema.isIndexed() && schema.isStored()) {
        doc.add(new Field(name, field.getValue(), STORED_ONLY));
      }
      switch (schema.getType()) {
        case INTEGER:
          value = Integer.valueOf(field.getValue());
          if (schema.isIndexed()) {
            doc.add(new IntField(name, (Integer) value, isStored));
          }
          if (schema.isDocValues()) {
            long docValuesBits = ((Integer) value).longValue();
            doc.add(new NumericDocValuesField(name, docValuesBits));
          }
          break;
        case LONG:
          value = Long.valueOf(field.getValue());
          if (schema.isIndexed()) {
            doc.add(new LongField(name, (Long) value, isStored));
          }
          if (schema.isDocValues()) {
            doc.add(new NumericDocValuesField(name, (long) value));
          }
          break;
        case DOUBLE:
          value = Double.valueOf(field.getValue());
          if (schema.isIndexed()) {
            doc.add(new DoubleField(name, (Double) value, isStored));
          }
          if (schema.isDocValues()) {
            long docValuesBits = Double.doubleToLongBits((Double) value);
            doc.add(new NumericDocValuesField(name, docValuesBits));
          }
          break;
        case FLOAT:
          value = Float.valueOf(field.getValue());
          if (schema.isIndexed()) {
            doc.add(new FloatField(name, (Float) value, isStored));
          }
          if (schema.isDocValues()) {
            long docValuesBits = Float.floatToIntBits((Float) value);
            doc.add(new NumericDocValuesField(name, docValuesBits));
          }
          break;
        case STRING:
          if (Strings.isNullOrEmpty(field.getValue())) {
            break;
          }
          if (schema.isIndexed()) {
            FieldType type = new FieldType();
            type.setTokenized(schema.isTokenized());
            type.setIndexed(schema.isIndexed());
            type.setStored(schema.isStored());
            type.setOmitNorms(schema.isOmitNorms());
            if (schema.isSnippet()) {
              type.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
              // snippet will use the stored info.
              type.setStored(true);
            }
            if (schema.isOmitFreqs()) {
              type.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
            }
            doc.add(new Field(name, field.getValue(), type));
          }
          if (schema.isDocValues()) {
            BytesRef bytes = new BytesRef(field.getValue());
            doc.add(new BinaryDocValuesField(name, bytes));
          }
          break;
        case FACET:
          String[] facetPath = field.getValue().split("/");
          doc.add(new FacetField(name, facetPath));
          if (schema.isIndexed()) {
            doc.add(new StringField(name, field.getValue(), isStored));
          }
          if (schema.isDocValues()) {
            doc.add(new BinaryDocValuesField(name, new BytesRef(field.getValue())));
          }
          break;
        default:
      }
    }
    if (lindenDoc.isSetCoordinate()) {
      Coordinate coord = lindenDoc.getCoordinate();
      Shape shape = SpatialContext.GEO.makePoint(coord.getLongitude(), coord.getLatitude());
      for (IndexableField field : config.getSpatialStrategy().createIndexableFields(shape)) {
        doc.add(field);
      }
    }
    return doc;
  }

  public static boolean isDocValueFields(LindenDocument lindenDoc) {
    if (lindenDoc.isSetCoordinate()) {
      return false;
    }

    if (!lindenDoc.isSetFields()) {
      return false;
    }

    for (LindenField field : lindenDoc.getFields()) {
      LindenFieldSchema schema = field.getSchema();
      if (!schema.isDocValues() || schema.isIndexed() || schema.isStored() || schema.getType() == LindenType.FACET) {
        return false;
      }
    }
    return true;
  }
}
