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

import com.xiaomi.linden.hadoop.indexing.keyvalueformat.IntermediateForm;
import com.xiaomi.linden.hadoop.indexing.keyvalueformat.Shard;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.log4j.Logger;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.OrdinalMappingAtomicReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * The initial version of an index is stored in the perm dir. Index files
 * created by newer versions are written to a temp dir on the local FS. After
 * successfully creating the new version in the temp dir, the shard writer
 * moves the new files to the perm dir and deletes the temp dir in close().
 */
public class ShardWriter {
  private static Logger logger = Logger.getLogger(ShardWriter.class);

  private final FileSystem fs;
  private final FileSystem localFs;
  private final Path perm;
  private final Path taxoPerm;
  private final Path temp;
  private final Path taxoTemp;
  private final IndexWriter writer;
  private final DirectoryTaxonomyWriter taxoWriter;
  private long numForms = 0;
  private final Configuration conf;

  /**
   * Constructor
   * @param fs
   * @param shard
   * @param tempDir
   * @param conf
   * @throws IOException
   */
  public ShardWriter(FileSystem fs, Shard shard, String tempDir, Configuration conf)
      throws IOException {
    logger.info("Construct a shard writer");

    this.conf = conf;
    this.fs = fs;
    localFs = FileSystem.getLocal(conf);
    perm = new Path(shard.getDirectory());
    taxoPerm = new Path(shard.getDirectory() + ".taxonomy");
    String indexDir = tempDir + "/" + "index";
    String taxoDir = tempDir + "/" + "taxo";
    temp = new Path(indexDir);
    taxoTemp = new Path(taxoDir);

    if (localFs.exists(temp)) {
      File tempFile = new File(temp.getName());
      if (tempFile.exists()) {
        LindenReducer.deleteDir(tempFile);
      }
    }

    if (!fs.exists(perm)) {
      fs.mkdirs(perm);
    } else {
      moveToTrash(conf, perm);
      fs.mkdirs(perm);
    }

    if (!fs.exists(taxoPerm)) {
      fs.mkdirs(taxoPerm);
    } else {
      moveToTrash(conf, taxoPerm);
      fs.mkdirs(taxoPerm);
    }
    IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, null);
    config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    writer = new IndexWriter(FSDirectory.open(new File(indexDir)), config);
    taxoWriter = new DirectoryTaxonomyWriter(FSDirectory.open(new File(taxoDir)));
  }

  /**
   * Process an intermediate form by carrying out, on the Lucene instance of
   * the shard, the deletes and the inserts (a ram index) in the form.
   * @param form  the intermediate form containing deletes and a ram index
   * @throws IOException
   */
  public void process(IntermediateForm form, FacetsConfig facetsConfig) throws IOException {
    if (facetsConfig != null) {
      DirectoryTaxonomyWriter.OrdinalMap map = new DirectoryTaxonomyWriter.MemoryOrdinalMap();
      // merge the taxonomies
      taxoWriter.addTaxonomy(form.getTaxoDirectory(), map);
      int ordinalMap[] = map.getMap();
      DirectoryReader reader = DirectoryReader.open(form.getDirectory());
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
      writer.addIndexes(new Directory[] { form.getDirectory() });
    }
    numForms++;
  }

  /**
   * Close the shard writer. Optimize the Lucene instance of the shard before
   * closing if necessary, and copy the files created in the temp directory
   * to the permanent directory after closing.
   * @throws IOException
   */
  public void close() throws IOException {
    logger.info("Closing the shard writer, processed " + numForms + " forms");
    try {
      writer.close();
      taxoWriter.close();
      logger.info("Closed Lucene index writer");
      moveFromTempToPerm(temp, perm);
      logger.info("Moved new index files to " + perm);
      moveFromTempToPerm(taxoTemp, taxoPerm);
      logger.info("Moved new taxo index files to " + taxoPerm);
    } finally {
      logger.info("Closed the shard writer");
    }
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.getClass().getName() + "@" + perm + "&" + temp;
  }

  private void moveFromTempToPerm(Path temp, Path perm) throws IOException {
    FileStatus[] fileStatus = localFs.listStatus(temp);
    // move the files created in temp dir except segments_N and segments.gen
    for (int i = 0; i < fileStatus.length; i++) {
      Path path = fileStatus[i].getPath();
      String name = path.getName();

      try {
        if (!fs.exists(new Path(perm, name))) {
          fs.copyFromLocalFile(path, new Path(perm, name));
        } else {
          moveToTrash(conf, perm);
          fs.copyFromLocalFile(path, new Path(perm, name));
        }
      } catch (Exception e) {
        logger.error("Exception in moveFromTempToPerm", e);
      }
    }
  }

  public void optimize() {
    try {
      writer.forceMerge(1);
    } catch (CorruptIndexException e) {
      logger.error("Corrupt Index error. ", e);
    } catch (IOException e) {
      logger.error("IOException during index optimization. ", e);
    }
  }

  public static void moveToTrash(Configuration conf, Path path) throws IOException {
    Trash t = new Trash(conf);
    boolean isMoved = t.moveToTrash(path);
    t.expunge();
    if (!isMoved) {
      logger.error("Trash is not enabled or file is already in the trash.");
    }
  }
}
