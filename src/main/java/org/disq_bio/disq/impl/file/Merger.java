/*
 * Disq
 *
 * MIT License
 *
 * Copyright (c) 2018-2019 Disq contributors
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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;

public class Merger {

  private final FileSystemWrapper fileSystemWrapper;

  public Merger(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

  public void mergeParts(
      Configuration conf, List<FileSystemWrapper.FileStatus> fileStatuses, String outputFile)
      throws IOException {
    List<String> parts =
        fileStatuses.stream()
            .map(FileSystemWrapper.FileStatus::getPath)
            .collect(Collectors.toList());
    fileSystemWrapper.concat(conf, parts, outputFile);
  }

  public void mergeParts(Configuration conf, String tempPartsDirectory, String outputFile)
      throws IOException {
    List<String> parts = fileSystemWrapper.listDirectory(conf, tempPartsDirectory);
    List<String> filteredParts =
        parts.stream().filter(new HiddenFileFilter()).collect(Collectors.toList());
    fileSystemWrapper.concat(conf, filteredParts, outputFile);
  }
}
