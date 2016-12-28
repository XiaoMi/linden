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

package com.xiaomi.linden.hadoop.indexing.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.lucene.facet.FacetsConfig;

import com.xiaomi.linden.hadoop.indexing.keyvalueformat.IntermediateForm;
import com.xiaomi.linden.hadoop.indexing.keyvalueformat.Shard;
import com.xiaomi.linden.hadoop.indexing.util.LindenConfigBuilder;

/**
 * This combiner combines multiple intermediate forms into one intermediate
 * form. More specifically, the input intermediate forms are a single-document
 * ram index and/or a single delete term. An output intermediate form contains
 * a multi-document ram index and/or multiple delete terms.
 */
public class LindenCombiner extends Reducer<Shard, IntermediateForm, Shard, IntermediateForm> {

  private static final Logger logger = Logger.getLogger(LindenCombiner.class);
  private static final long maxSizeInBytes = 50L << 20;
  private static final long nearMaxSizeInBytes = maxSizeInBytes - (maxSizeInBytes >>> 3);
  private FacetsConfig facetsConfig;

  @Override
  protected void reduce(Shard key, Iterable<IntermediateForm> values, Context context)
      throws IOException, InterruptedException {
    String message = key.toString();
    IntermediateForm form = null;
    Iterator<IntermediateForm> iterator = values.iterator();
    while (iterator.hasNext()) {
      IntermediateForm singleDocForm = iterator.next();
      long formSize = form == null ? 0 : form.totalSizeInBytes();
      long singleDocFormSize = singleDocForm.totalSizeInBytes();

      if (form != null && formSize + singleDocFormSize > maxSizeInBytes) {
        closeForm(form, message);
        context.write(key, form);
        form = null;
      }

      if (form == null && singleDocFormSize >= nearMaxSizeInBytes) {
        context.write(key, singleDocForm);
      } else {
        if (form == null) {
          form = createForm(message);
        }
        form.process(singleDocForm, facetsConfig);
      }
    }

    if (form != null) {
      closeForm(form, message);
      context.write(key, form);
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    facetsConfig = LindenConfigBuilder.build().createFacetsConfig();
  }

  private IntermediateForm createForm(String message) throws IOException {
    logger.info("Construct a form writer for " + message);
    IntermediateForm form = new IntermediateForm();
    return form;
  }

  private void closeForm(IntermediateForm form, String message) throws IOException {
    form.closeWriter();
    logger.info("Closed the form writer for " + message + ", form = " + form);
  }
}
