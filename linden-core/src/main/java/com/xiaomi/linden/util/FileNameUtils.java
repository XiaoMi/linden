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

package com.xiaomi.linden.util;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

public class FileNameUtils {

  public static void sort(File[] files, final int seq) {
    if (files != null && files.length > 1) {
      Arrays.sort(files, new Comparator<File>() {
        @Override
        public int compare(File o1, File o2) {
          String fileName1 = o1.getName();
          String fileName2 = o2.getName();
          return seq * fileName1.compareTo(fileName2);
        }
      });
    }
  }
}
