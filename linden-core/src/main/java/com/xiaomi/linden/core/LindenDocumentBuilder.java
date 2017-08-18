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
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.xiaomi.linden.common.schema.LindenSchemaConf;
import com.xiaomi.linden.thrift.common.Coordinate;
import com.xiaomi.linden.thrift.common.LindenDocument;
import com.xiaomi.linden.thrift.common.LindenField;
import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenType;

public class LindenDocumentBuilder {

  public static LindenDocument build(LindenSchema schema, JSONObject json) throws IOException {
    LindenDocument document = new LindenDocument();
    if (json.containsKey(schema.getId())) {
      document.setId(json.getString(schema.getId()));
    } else {
      throw new IOException(json.toJSONString() + " does not has id [" + schema.getId() + "]");
    }
    for (LindenFieldSchema fieldSchema : schema.getFields()) {
      Object value = json.get(fieldSchema.getName());
      if (value == null) {
        continue;
      }
      if (fieldSchema.isMulti()) {
        if (!(value instanceof JSONArray)) {
          throw new IOException("Multi value field " + fieldSchema.getName() + " must be in JSONArray format");
        }
        for (Object element : (JSONArray) value) {
          LindenField field = new LindenField().setSchema(fieldSchema).setValue(element.toString());
          document.addToFields(field);
        }
        LindenFieldSchema multiValueFieldSchema = new LindenFieldSchema(fieldSchema.getName(), LindenType.STRING);
        // both multi and docValues are true is forbidden in user defined schema.
        // this can only happen in multi-value source field, which is used for source data and score model
        multiValueFieldSchema.setMulti(true).setDocValues(true);
        LindenField multiValueField = new LindenField().setSchema(multiValueFieldSchema).setValue(value.toString());
        document.addToFields(multiValueField);
      } else {
        LindenField field = new LindenField().setSchema(fieldSchema).setValue(value.toString());
        document.addToFields(field);
      }
    }
    JSONArray dynamicFields = json.getJSONArray(LindenSchemaConf.DYNAMICS);
    if (dynamicFields != null) {
      for (Object element : dynamicFields) {
        JSONObject jsonElement = (JSONObject) element;
        document.addToFields(buildDynamicField(jsonElement));
      }
    }
    if (json.containsKey(LindenSchemaConf.LATITUDE) && json.containsKey(LindenSchemaConf.LONGITUDE)) {
      document.setCoordinate(new Coordinate().setLongitude(json.getDouble(LindenSchemaConf.LONGITUDE))
                                 .setLatitude(json.getDouble(LindenSchemaConf.LATITUDE)));
    }
    return document;
  }

  public static LindenField buildDynamicField(JSONObject jsonElement) throws IOException {
    LindenField field = new LindenField();
    // default field type is STRING
    // Linden dynamic field is always indexed and stored
    LindenFieldSchema fieldSchema = new LindenFieldSchema().setType(LindenType.STRING).setIndexed(true).setStored(true);
    for (Map.Entry<String, Object> entry : jsonElement.entrySet()) {
      String key = entry.getKey();
      String value = String.valueOf(entry.getValue());
      switch (key.toLowerCase()) {
        case "_tokenize":
          fieldSchema.setTokenized(value.equalsIgnoreCase("true"));
          break;
        case "_omitnorms":
          fieldSchema.setOmitNorms(value.equalsIgnoreCase("true"));
          break;
        case "_snippet":
          fieldSchema.setSnippet(value.equalsIgnoreCase("true"));
          break;
        case "_docvalues":
          fieldSchema.setDocValues(value.equalsIgnoreCase("true"));
          break;
        case "_multi":
          fieldSchema.setMulti(value.equalsIgnoreCase("true"));
          break;
        case "_omitfreqs":
          fieldSchema.setOmitFreqs(value.equalsIgnoreCase("true"));
          break;
        case "_type":
          switch (value.toLowerCase()) {
            case "int":
              fieldSchema.setType(LindenType.INTEGER);
              break;
            default:
              fieldSchema.setType(LindenType.valueOf(value.toUpperCase()));
              break;
          }
          break;
        default:
          if (fieldSchema.isSetName()) {
            throw new IOException(
                "Dynamic field name has already been set to " + fieldSchema.getName() + ", it can not be set to "
                + key);
          }
          fieldSchema.setName(key);
          field.setValue(value);
          break;
      }
    }
    LindenSchemaBuilder.verifyFieldSchema(fieldSchema);
    return field.setSchema(fieldSchema);
  }
}
