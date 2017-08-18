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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

import com.google.common.base.Preconditions;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.xiaomi.linden.thrift.common.LindenFieldSchema;
import com.xiaomi.linden.thrift.common.LindenSchema;
import com.xiaomi.linden.thrift.common.LindenType;

public class LindenSchemaBuilder {
  public static final String TABLE = "table";
  public static final String TABLE_ID = "id";
  public static final String TABLE_COLUMN = "column";
  public static final String NAME = "name";
  public static final String TYPE = "type";
  public static final String STORE = "store";
  public static final String INDEX = "index";
  public static final String TOKENIZED = "tokenize";
  public static final String OMITNORMS = "omitnorms";
  public static final String OMITFREQS = "omitfreqs";
  public static final String SNIPPET = "snippet";
  public static final String DOCVALUES = "docvalues";
  public static final String MULTI = "multi";
  public static final String TEXT = "text";
  public static final String YES = "yes";
  public static final String NO = "no";
  public static final String INT = "int";

  public static LindenSchema build(File schemaFile) throws Exception {
    Preconditions.checkNotNull(schemaFile);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document dom = db.parse(schemaFile);
    dom.getDocumentElement().normalize();

    NodeList table = dom.getElementsByTagName(TABLE);
    Node colNode = table.item(0);
    LindenSchema lindenSchema = new LindenSchema();
    lindenSchema.setId(colNode.getAttributes().getNamedItem(TABLE_ID).getNodeValue());
    NodeList colList = dom.getElementsByTagName(TABLE_COLUMN);
    for (int i = 0; i < colList.getLength(); ++i) {
      Element element = (Element) colList.item(i);
      LindenFieldSchema fieldSchema = parseFiled(element);
      verifyFieldSchema(fieldSchema);
      lindenSchema.addToFields(fieldSchema);
    }
    return lindenSchema;
  }

  public static void verifyFieldSchema(LindenFieldSchema schema) {
    if (schema.isDocValues() && schema.isMulti()) {
      throw new RuntimeException("DocValues and Multi can not be both set for field " + schema.getName());
    }
  }

  private static LindenFieldSchema parseFiled(Element element) {
    LindenFieldSchema field = new LindenFieldSchema();
    if (element.hasAttribute(NAME)) {
      field.setName(element.getAttribute(NAME));
    }

    //the linden type has string but not has text
    if (element.hasAttribute(TYPE)) {
      String type = element.getAttribute(TYPE).toUpperCase();
      if (type.toLowerCase().equals(TEXT)) {
        field.setType(LindenType.STRING);
      } else if (type.toLowerCase().equals(INT)) {
        field.setType(LindenType.INTEGER);
      } else {
        field.setType(LindenType.valueOf(type));
      }
    }
    if (element.hasAttribute(STORE)) {
      String store = element.getAttribute(STORE);
      field.setStored(store.toLowerCase().equals(YES));
    }

    //three factors composite the index
    if (element.hasAttribute(TOKENIZED)) {
      String tokenize = element.getAttribute(TOKENIZED);
      field.setTokenized(tokenize.toLowerCase().equals(YES));
    }
    if (element.hasAttribute(OMITNORMS)) {
      String omitNorms = element.getAttribute(OMITNORMS);
      field.setOmitNorms(omitNorms.toLowerCase().equals(YES));
    }
    if (element.hasAttribute(OMITFREQS)) {
      String omitNorms = element.getAttribute(OMITFREQS);
      field.setOmitFreqs(omitNorms.toLowerCase().equals(YES));
    }
    if (element.hasAttribute(SNIPPET)) {
      String snippet = element.getAttribute(SNIPPET);
      field.setSnippet(snippet.toLowerCase().equals(YES));
    }
    if (element.hasAttribute(DOCVALUES)) {
      String docValues = element.getAttribute(DOCVALUES);
      field.setDocValues(docValues.toLowerCase().equals(YES));
    }
    if (element.hasAttribute(MULTI)) {
      String multi = element.getAttribute(MULTI);
      field.setMulti(multi.toLowerCase().equals(YES));
    }

    //but if the field has the index attribute,
    //we must deal with it first
    if (element.hasAttribute(INDEX)) {
      String index = element.getAttribute(INDEX).toUpperCase();
      if (index.toLowerCase().equals(YES) || index.toLowerCase().equals(NO)) {
        field.setIndexed(index.toLowerCase().equals(YES));
      }
    }
    return field;
  }
}
