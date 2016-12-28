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

package com.xiaomi.linden.hadoop.indexing.util;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;

import com.xiaomi.linden.core.LindenConfig;
import com.xiaomi.linden.core.LindenSchemaBuilder;
import com.xiaomi.linden.thrift.common.LindenSchema;

public class LindenConfigBuilder {

  private final static Logger logger = Logger.getLogger(LindenConfigBuilder.class);

  public static LindenConfig build() throws IOException {
    File lindenProperties = new File("lindenProperties");
    Preconditions.checkArgument(lindenProperties.exists(), "can not find linden properties file.");

    try {
      LindenConfig lindenConf = com.xiaomi.linden.core.LindenConfigBuilder.build(lindenProperties);
      File lindenSchema = new File("lindenSchema");
      Preconditions.checkArgument(lindenSchema.exists(), "can not find linden schema file.");
      LindenSchema schema;
      try {
        schema = LindenSchemaBuilder.build(lindenSchema);
      } catch (Exception e) {
        logger.error("Linden schema builder exception", e);
        throw new IOException(e);
      }
      lindenConf.setSchema(schema);
      lindenConf.setIndexType(LindenConfig.IndexType.RAM);
      return lindenConf;
    } catch (Exception e) {
      logger.error("Linden search config builder exception", e);
      throw new IOException(e);
    }
  }
}
