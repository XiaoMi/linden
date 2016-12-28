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

public interface LindenJobConfig {
  String NUM_SHARDS = "linden.num.shards";
  String INDEX_PATH = "linden.index.path";
  String INDEX_SHARDS = "linden.index.shards";
  String LINDEN_PROPERTIES_FILE_URL = "linden.properties.file.url";
  String SCHEMA_FILE_URL = "linden.schema.file.url";
  String INPUT_DIR = "linden.input.dir";
  String OUTPUT_DIR = "linden.output.dir";
}
