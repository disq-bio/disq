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

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.tribble.index.tabix.TabixIndexMerger;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
      Configuration conf,
      String tempPartsDirectory,
      String outputFile,
      List<Long> partLengths,
      long fileLength)
      throws IOException {
    logger.info("Merging .tbi files in temp directory {} to {}", tempPartsDirectory, outputFile);
    List<String> parts = fileSystemWrapper.listDirectory(conf, tempPartsDirectory);
    List<String> filteredParts =
        parts
            .stream()
            .filter(f -> f.endsWith(TabixIndexWriteOption.getIndexExtension()))
            .collect(Collectors.toList());
    if (partLengths.size() - 2 != filteredParts.size()) { // don't count header and terminator
      throw new IllegalArgumentException(
          "Cannot merge different number of VCF and TBI files in " + tempPartsDirectory);
    }
    int i = 0;
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    try (OutputStream out = fileSystemWrapper.create(conf, outputFile)) {
      TabixIndexMerger indexMerger = new TabixIndexMerger(out, partLengths.get(i++));
      List<Callable<TabixIndex>> callables =
          filteredParts
              .stream()
              .map(part -> (Callable<TabixIndex>) () -> readIndex(conf, part))
              .collect(Collectors.toList());
      for (Future<TabixIndex> futureIndex : executorService.invokeAll(callables)) {
        indexMerger.processIndex(futureIndex.get(), partLengths.get(i++));
      }
      indexMerger.finish(fileLength);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      executorService.shutdown();
    }
    logger.info("Done merging .tbi files");
  }

  private TabixIndex readIndex(Configuration conf, String file) throws IOException {
    TabixIndex index;
    try (SeekableStream in = fileSystemWrapper.open(conf, file)) {
      index = new TabixIndex(new BlockCompressedInputStream(in));
    }
    fileSystemWrapper.delete(conf, file);
    return index;
  }
}
