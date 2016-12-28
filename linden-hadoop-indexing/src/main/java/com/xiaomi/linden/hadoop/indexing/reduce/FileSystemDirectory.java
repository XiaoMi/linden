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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * This class implements a Lucene Directory on top of a general FileSystem.
 * Currently it does not support locking.
 */
public class FileSystemDirectory extends Directory {

  private final FileSystem fs;
  private final Path directory;
  private final int ioFileBufferSize;

  /**
   * Constructor
   * @param fs
   * @param directory
   * @param create
   * @param conf
   * @throws IOException
   */
  public FileSystemDirectory(FileSystem fs, Path directory, boolean create, Configuration conf)
      throws IOException {

    this.fs = fs;
    this.directory = directory;
    this.ioFileBufferSize = conf.getInt("io.file.buffer.size", 4096);

    if (create) {
      create();
    }

    boolean isDir = false;
    try {
      FileStatus status = fs.getFileStatus(directory);
      if (status != null) {
        isDir = status.isDirectory();
      }
    } catch (IOException e) {
      // file does not exist, isDir already set to false
    }
    if (!isDir) {
      throw new IOException(directory + " is not a directory");
    }
  }

  private void create() throws IOException {
    if (!fs.exists(directory)) {
      fs.mkdirs(directory);
    }

    boolean isDir = false;
    try {
      FileStatus status = fs.getFileStatus(directory);
      if (status != null) {
        isDir = status.isDirectory();
      }
    } catch (IOException e) {
      // file does not exist, isDir already set to false
    }
    if (!isDir) {
      throw new IOException(directory + " is not a directory");
    }

    // clear old index files
    FileStatus[] fileStatus = fs.listStatus(directory);
    for (int i = 0; i < fileStatus.length; i++) {
      if (!fs.delete(fileStatus[i].getPath(), true)) {
        throw new IOException("Cannot delete index file " + fileStatus[i].getPath());
      }
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.store.Directory#list()
   */
  @Override
  public String[] listAll() throws IOException {
    FileStatus[] fileStatus = fs.listStatus(directory);
    String[] result = new String[fileStatus.length];
    for (int i = 0; i < fileStatus.length; i++) {
      result[i] = fileStatus[i].getPath().getName();
    }
    return result;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.store.Directory#fileExists(java.lang.String)
   */
  @Override
  public boolean fileExists(String name) throws IOException {
    return fs.exists(new Path(directory, name));
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.store.Directory#fileLength(java.lang.String)
   */
  @Override
  public long fileLength(String name) throws IOException {
    return fs.getFileStatus(new Path(directory, name)).getLen();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.store.Directory#deleteFile(java.lang.String)
   */
  @Override
  public void deleteFile(String name) throws IOException {
    if (!fs.delete(new Path(directory, name), true)) {
      throw new IOException("Cannot delete index file " + name);
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.store.Directory#makeLock(java.lang.String)
   */
  @Override
  public Lock makeLock(final String name) {
    return new Lock() {
      @Override
      public boolean obtain() {
        return true;
      }

      @Override public void close() throws IOException {
      }

      @Override
      public boolean isLocked() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String toString() {
        return "Lock@" + new Path(directory, name);
      }
    };
  }

  @Override public void clearLock(String name) throws IOException {

  }

  /*
   * (non-Javadoc)
   * @see org.apache.lucene.store.Directory#close()
   */
  @Override
  public void close() throws IOException {
    // do not close the file system
  }

  @Override public void setLockFactory(LockFactory lockFactory) throws IOException {

  }

  @Override public LockFactory getLockFactory() {
    return null;
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.getClass().getName() + "@" + directory;
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
