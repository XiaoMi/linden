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

package com.xiaomi.linden.hadoop.indexing.keyvalueformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.OrdinalMappingAtomicReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.xiaomi.linden.hadoop.indexing.reduce.RAMDirectoryUtil;

/**
 * An intermediate form for one or more parsed Lucene documents and/or
 * delete terms. It actually uses Lucene file format as the format for
 * the intermediate form by using RAM dir files.
 *
 * Note: If process(*) is ever called, closeWriter() should be called.
 * Otherwise, no need to call closeWriter().
 */
public class IntermediateForm implements Writable {

  private RAMDirectory dir;
  private RAMDirectory taxoDir;
  private IndexWriter writer;
  private DirectoryTaxonomyWriter taxoWriter;
  private int numDocs;

  /**
   * Constructor
   * @throws IOException
   */
  public IntermediateForm() throws IOException {
    dir = new RAMDirectory();
    taxoDir = new RAMDirectory();
  }


  /**
   * Get the ram directory of the intermediate form.
   * @return the ram directory
   */
  public Directory getDirectory() {
    return dir;
  }

  public Directory getTaxoDirectory() {
    return taxoDir;
  }

  /**
   * This method is used by the index update mapper and process a document
   * operation into the current intermediate form.
   * @param doc  input document operation
   * @throws IOException
   */
  public void process(Term id, Document doc, Analyzer analyzer, FacetsConfig facetsConfig) throws IOException {
    if (writer == null) {
      // analyzer is null because we specify an analyzer with addDocument
      createWriter();
    }
    if (facetsConfig != null) {
      writer.updateDocument(id, facetsConfig.build(taxoWriter, doc), analyzer);
    } else {
      writer.updateDocument(id, doc, analyzer);
    }
    numDocs++;
  }


  private void createWriter() throws IOException {
    IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, null);
    config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    writer = new IndexWriter(dir, config);
    taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
  }

  /**
   * This method is used by the index update combiner and process an
   * intermediate form into the current intermediate form. More specifically,
   * the input intermediate forms are a single-document ram index and/or a
   * single delete term.
   * @param form  the input intermediate form
   * @throws IOException
   */
  public void process(IntermediateForm form, FacetsConfig facetsConfig) throws IOException {
    if (form.dir.ramBytesUsed() > 0 || form.taxoDir.ramBytesUsed() > 0) {
      if (writer == null) {
        createWriter();
      }

      if (facetsConfig != null) {
        DirectoryTaxonomyWriter.OrdinalMap map = new DirectoryTaxonomyWriter.MemoryOrdinalMap();
        // merge the taxonomies
        taxoWriter.addTaxonomy(form.taxoDir, map);
        int ordinalMap[] = map.getMap();
        DirectoryReader reader = DirectoryReader.open(form.dir);
        try {
          List<AtomicReaderContext> leaves = reader.leaves();
          int numReaders = leaves.size();
          AtomicReader wrappedLeaves[] = new AtomicReader[numReaders];
          for (int i = 0; i < numReaders; i++) {
            wrappedLeaves[i] = new OrdinalMappingAtomicReader(leaves.get(i).reader(), ordinalMap, facetsConfig);
          }
          writer.addIndexes(new MultiReader(wrappedLeaves));
        } finally {
          reader.close();
        }
      } else {
        writer.addIndexes(new Directory[] { form.dir });
      }
      numDocs++;
    }
  }

  /**
   * Close the Lucene index writer associated with the intermediate form,
   * if created. Do not close the ram directory. In fact, there is no need
   * to close a ram directory.
   * @throws IOException
   */
  public void closeWriter() throws IOException {
    if (writer != null) {
      writer.forceMerge(1);
      writer.close();
      taxoWriter.close();
      writer = null;
      taxoWriter = null;
    }
  }

  /**
   * The total size of files in the directory and ram used by the index writer.
   * It does not include memory used by the delete list.
   * @return the total size in bytes
   */
  public long totalSizeInBytes() throws IOException {
    long size = dir.ramBytesUsed();
    size += taxoDir.ramBytesUsed();
    if (writer != null) {
      size += writer.ramBytesUsed();
    }
    //omit taxoWriter ram bytes used
    return size;
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(this.getClass().getSimpleName());
    buffer.append("[numDocs=");
    buffer.append(numDocs);
    buffer.append("]");
    return buffer.toString();
  }

  private void resetForm() throws IOException {
    if (dir.ramBytesUsed() > 0) {
      // it's ok if we don't close a ram directory
      dir.close();
      // an alternative is to delete all the files and reuse the ram directory
      dir = new RAMDirectory();
      taxoDir.close();
      taxoDir = new RAMDirectory();
    }
    assert (writer == null);
    assert (taxoWriter == null);
    numDocs = 0;
  }

  // ///////////////////////////////////
  // Writable
  // ///////////////////////////////////

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    RAMDirectoryUtil.writeRAMFiles(out, dir);
    RAMDirectoryUtil.writeRAMFiles(out, taxoDir);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    resetForm();
    RAMDirectoryUtil.readRAMFiles(in, dir);
    RAMDirectoryUtil.readRAMFiles(in, taxoDir);
  }
}
