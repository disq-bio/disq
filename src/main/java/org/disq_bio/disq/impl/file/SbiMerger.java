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

import htsjdk.samtools.SBIIndex;
import htsjdk.samtools.SBIIndexMerger;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbiMerger {

  private static final Logger logger = LoggerFactory.getLogger(SbiMerger.class);

  private final FileSystemWrapper fileSystemWrapper;

  public SbiMerger(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

  public void mergeParts(
      Configuration conf,
      String tempPartsDirectory,
      String outputFile,
      long headerLength,
      long fileLength)
      throws IOException {
    logger.info("Merging .sbi files in temp directory {} to {}", tempPartsDirectory, outputFile);
    List<String> parts = fileSystemWrapper.listDirectory(conf, tempPartsDirectory);
    List<String> filteredParts =
        parts
            .stream()
            .filter(f -> f.endsWith(SBIIndex.FILE_EXTENSION))
            .collect(Collectors.toList());
    try (OutputStream out = fileSystemWrapper.create(conf, outputFile)) {
      SBIIndexMerger sbiIndexMerger = new SBIIndexMerger(out, headerLength);
      for (String sbiPartFile : filteredParts) {
        try (InputStream in = fileSystemWrapper.open(conf, sbiPartFile)) {
          sbiIndexMerger.processIndex(SBIIndex.load(in));
        }
        fileSystemWrapper.delete(conf, sbiPartFile);
      }
      sbiIndexMerger.finish(fileLength);
    }
    logger.info("Done merging .sbi files");
  }
}
