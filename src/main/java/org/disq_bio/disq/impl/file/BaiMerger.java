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

import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.BAMIndexMerger;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Merges BAM index files for (headerless) parts of a BAM file into a single index file. */
public class BaiMerger {
  private static final Logger logger = LoggerFactory.getLogger(BaiMerger.class);

  private final FileSystemWrapper fileSystemWrapper;

  public BaiMerger(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

  public void mergeParts(
      Configuration conf,
      String tempPartsDirectory,
      String outputFile,
      SAMFileHeader header,
      List<Long> partLengths)
      throws IOException {
    logger.info("Merging .bai files in temp directory {} to {}", tempPartsDirectory, outputFile);
    List<String> parts = fileSystemWrapper.listDirectory(conf, tempPartsDirectory);
    List<String> baiParts = getBaiParts(parts);
    if (partLengths.size() - 2 != baiParts.size()) { // don't count header and terminator
      throw new IllegalArgumentException(
          "Cannot merge different number of BAM and BAI files in " + tempPartsDirectory);
    }
    try (OutputStream out = fileSystemWrapper.create(conf, outputFile)) {
      List<SeekableStream> baiStreams = new ArrayList<>();
      for (String baiPart : baiParts) {
        baiStreams.add(fileSystemWrapper.open(conf, baiPart));
      }
      BAMIndexMerger.merge(header, partLengths, baiStreams, out);

      // TODO: close all properly - consider reading BAIs into memory
      for (SeekableStream stream : baiStreams) {
        stream.close();
      }
    }
    for (String baiPart : baiParts) {
      fileSystemWrapper.delete(conf, baiPart);
    }
    logger.info("Done merging .bai files");
  }

  private List<String> getBaiParts(List<String> parts) {
    return parts
        .stream()
        .filter(f -> f.endsWith(BAMIndex.BAMIndexSuffix))
        .collect(Collectors.toList());
  }
}
