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

package com.xiaomi.linden.core.indexing;

import java.io.IOException;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xiaomi.linden.core.LindenDocumentBuilder;
import com.xiaomi.linden.thrift.common.*;

public class LindenIndexRequestParser {
  private static final String TYPE = "type";
  private static final String INDEX = "index";
  private static final String UPDATE = "update";
  private static final String REPLACE = "replace";
  private static final String CONTENT = "content";
  private static final String DELETE = "delete";
  private static final String ROUTE = "route";
  private static final String DELETE_INDEX = "delete_index";
  private static final String SWAP_INDEX = "swap_index";

  public static LindenIndexRequest parse(LindenSchema schema, String content) throws IOException{
    LindenIndexRequest request = new LindenIndexRequest();
    JSONObject json = JSONObject.parseObject(content);
    if (json.containsKey(TYPE)) {
      String type = json.getString(TYPE);
      switch (type.toLowerCase()) {
        case INDEX:
          request.setType(IndexRequestType.INDEX);
          request.setDoc(LindenDocumentBuilder.build(schema, json.getJSONObject(CONTENT)));
          request.setId(request.getDoc().getId());
          break;
        case DELETE:
          request.setType(IndexRequestType.DELETE);
          if (json.containsKey(schema.getId())) {
            request.setId(json.getString(schema.getId()));
          } else {
            throw new IOException(json.toJSONString() + " does not has id [" + schema.getId() + "]");
          }
          break;
        case UPDATE:
          request.setType(IndexRequestType.UPDATE);
          request.setDoc(LindenDocumentBuilder.build(schema, json.getJSONObject(CONTENT)));
          request.setId(request.getDoc().getId());
          break;
        case REPLACE:
          request.setType(IndexRequestType.REPLACE);
          request.setDoc(LindenDocumentBuilder.build(schema, json.getJSONObject(CONTENT)));
          request.setId(request.getDoc().getId());
          break;
        // delete one index in multi core linden mode
        case DELETE_INDEX:
          request.setType(IndexRequestType.DELETE_INDEX);
          break;
        // swap the index with current index in hot swap mode
        case SWAP_INDEX:
          request.setType(IndexRequestType.SWAP_INDEX);
          break;
        default:
          throw new IOException("Invalid index type: " + type);
      }
      if (json.containsKey(ROUTE)) {
        JSONArray shardArray = json.getJSONArray(ROUTE);
        request.setRouteParam(new IndexRouteParam());
        for (int i = 0; i < shardArray.size(); ++i) {
          request.getRouteParam().addToShardIds(shardArray.getInteger(i));
        }
      }
      if (json.containsKey(INDEX)) {
        request.setIndexName(json.getString(INDEX));
      }
    } else {
      request.setType(IndexRequestType.INDEX);
      request.setDoc(LindenDocumentBuilder.build(schema, json));
      request.setId(request.getDoc().getId());
    }
    return request;
  }
}
