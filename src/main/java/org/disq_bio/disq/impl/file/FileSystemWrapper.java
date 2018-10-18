/*
 * Disq
 *
 * MIT License
 *
 * Copyright (c) 2018 Disq contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.disq_bio.disq.impl.file;

import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;

/**
 * A wrapper around Hadoop and NIO filesystems so users can choose a single one to use for all
 * filesystem operations.
 */
public interface FileSystemWrapper extends Serializable {

  /** @return true if this implementation uses the {@link java.nio} API */
  boolean usesNio();

  /**
   * Returns a consistent, fully-qualified representation of the path.
   *
   * @param conf the Hadoop configuration
   * @param path the path to the file to open
   * @return the normalized (fully-qualified) path
   * @throws IOException if an IO error occurs
   */
  String normalize(Configuration conf, String path) throws IOException;

  /**
   * Open a file for reading. The caller is responsible for closing the stream that is returned.
   *
   * @param conf the Hadoop configuration
   * @param path the path to the file to open
   * @return a seekable stream to read from
   * @throws IOException if an IO error occurs
   */
  SeekableStream open(Configuration conf, String path) throws IOException;

  /**
   * Create a file for writing, overwriting an existing file. The caller is responsible for closing
   * the stream that is returned.
   *
   * @param conf the Hadoop configuration
   * @param path the path to the file to create
   * @return a stream to write to
   * @throws IOException if an IO error occurs
   */
  OutputStream create(Configuration conf, String path) throws IOException;

  /**
   * Delete a file or directory.
   *
   * @param conf the Hadoop configuration
   * @param path the path to the file or directory to delete
   * @return true if the file or directory was successfully deleted, false if the path didn't exist
   * @throws IOException if an IO error occurs
   */
  boolean delete(Configuration conf, String path) throws IOException;

  /**
   * Check if a file or directory exists.
   *
   * @param conf the Hadoop configuration
   * @param path the path to the file or directory to check for existence
   * @return true if the specified path represents a file or directory in the filesystem
   * @throws IOException if an IO error occurs
   */
  boolean exists(Configuration conf, String path) throws IOException;

  /**
   * Returns the size of a file, in bytes.
   *
   * @param conf the Hadoop configuration
   * @param path the path to the file
   * @return the file size, in bytes
   * @throws IOException if an IO error occurs
   */
  long getFileLength(Configuration conf, String path) throws IOException;

  /**
   * Check if a path is a directory.
   *
   * @param conf the Hadoop configuration
   * @param path the path to check
   * @return true if the specified path represents a directory in the filesystem
   * @throws IOException if an IO error occurs
   */
  boolean isDirectory(Configuration conf, String path) throws IOException;

  /**
   * Return the paths of files in a directory, in lexicographic order.
   *
   * @param conf the Hadoop configuration
   * @param path the path to the directory
   * @return paths in lexicographic order
   * @throws IOException if an IO error occurs
   */
  List<String> listDirectory(Configuration conf, String path) throws IOException;

  /**
   * Concatenate the contents of multiple files into a single file.
   *
   * @param conf the Hadoop configuration
   * @param parts the paths of files to concatenate
   * @param path the path of the output file
   * @throws IOException if an IO error occurs
   */
  void concat(Configuration conf, List<String> parts, String path) throws IOException;

  default String firstFileInDirectory(Configuration conf, String path) throws IOException {
    Optional<String> firstPath =
        listDirectory(conf, path).stream().filter(new HiddenFileFilter()).findFirst();
    if (!firstPath.isPresent()) {
      throw new IllegalArgumentException("No files found in " + path);
    }
    return firstPath.get();
  }
}
