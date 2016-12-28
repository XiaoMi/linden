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

package com.xiaomi.linden.lucene.similarity;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IDFManager {
  private Map<String, Float> wordsIDF;
  private float maxIDF = -1;
  private static volatile IDFManager idfManager;

  private IDFManager(String fileName) throws IOException {
    List<String> lines = FileUtils.readLines(new File(fileName));
    wordsIDF = new HashMap<>();
    for (String line : lines) {
      String[] tokens = line.split("\t");
      if (tokens.length != 2) {
        continue;
      }
      Float idf = Float.valueOf(tokens[1]);
      if (idf > maxIDF) {
        maxIDF = idf;
      }
      wordsIDF.put(tokens[0], idf);
    }
  }

  private IDFManager(InputStream inputStream) throws IOException {
    String line;
    BufferedReader bf = new BufferedReader(new InputStreamReader(inputStream));
    wordsIDF = new HashMap<>();
    while ((line = bf.readLine()) != null) {
      String[] tokens = line.split("\t");
      if (tokens.length != 2)
        continue;
      Float idf = Float.valueOf(tokens[1]);
      if (idf > maxIDF)
        maxIDF = idf;
      wordsIDF.put(tokens[0], idf);
    }
    bf.close();
  }

  public static synchronized IDFManager createInstance(String fileName) throws IOException {
    if (idfManager == null) {
      synchronized (IDFManager.class) {
        if (idfManager == null) {
          idfManager = new IDFManager(fileName);
        }
      }
    }
    return idfManager;
  }

  public float getIDF(String word) {
    Float idf = wordsIDF.get(word);
    if (idf != null)
      return idf;
    return maxIDF;
  }
}
