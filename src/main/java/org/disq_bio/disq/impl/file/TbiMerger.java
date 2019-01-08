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
import htsjdk.tribble.index.tabix.TabixIndexMerger;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.disq_bio.disq.TabixIndexWriteOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Merges tabix index files for (headerless) parts of a VCF file into a single index file. */
public class TbiMerger {
  private static final Logger logger = LoggerFactory.getLogger(TbiMerger.class);

  private final FileSystemWrapper fileSystemWrapper;

  public TbiMerger(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

  public void mergeParts(
      Configuration conf, String tempPartsDirectory, String outputFile, List<Long> partLengths)
      throws IOException {
    logger.info("Merging .tbi files in temp directory {} to {}", tempPartsDirectory, outputFile);
    List<String> parts = fileSystemWrapper.listDirectory(conf, tempPartsDirectory);
    List<String> tbiParts = getTbiParts(parts);
    if (partLengths.size() - 2 != tbiParts.size()) { // don't count header and terminator
      throw new IllegalArgumentException(
          "Cannot merge different number of VCF and TBI files in " + tempPartsDirectory);
    }
    List<SeekableStream> tbiStreams = new ArrayList<>();
    try (OutputStream out = fileSystemWrapper.create(conf, outputFile)) {
      for (String tbiPart : tbiParts) {
        tbiStreams.add(fileSystemWrapper.open(conf, tbiPart));
      }
      TabixIndexMerger.merge(partLengths, tbiStreams, out);
    } finally {
      for (SeekableStream stream : tbiStreams) {
        stream.close();
      }
    }
    for (String tbiPart : tbiParts) {
      fileSystemWrapper.delete(conf, tbiPart);
    }
    logger.info("Done merging .tbi files");
  }

  private List<String> getTbiParts(List<String> parts) {
    return parts
        .stream()
        .filter(f -> f.endsWith(TabixIndexWriteOption.getIndexExtension()))
        .collect(Collectors.toList());
  }
}
