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

import htsjdk.samtools.IndexMerger;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merges index files for (headerless) parts of a data file into a single index file.
 *
 * @param <I> the index type
 * @param <H> the header type
 */
public abstract class IndexFileMerger<I, H> {
  private static final Logger logger = LoggerFactory.getLogger(IndexFileMerger.class);

  protected final FileSystemWrapper fileSystemWrapper;

  protected IndexFileMerger(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

  protected abstract String getIndexExtension();

  protected abstract IndexMerger<I> newIndexMerger(final OutputStream out, final long headerLength)
      throws IOException;

  public void mergeParts(
      Configuration conf,
      String tempPartsDirectory,
      String outputFile,
      H header,
      List<Long> partLengths,
      long fileLength)
      throws IOException {
    logger.info(
        "Merging {} files in temp directory {} to {}",
        getIndexExtension(),
        tempPartsDirectory,
        outputFile);
    List<String> parts = fileSystemWrapper.listDirectory(conf, tempPartsDirectory);
    List<String> filteredParts =
        parts.stream().filter(f -> f.endsWith(getIndexExtension())).collect(Collectors.toList());
    if (partLengths.size() - 2 != filteredParts.size()) { // don't count header and terminator
      throw new IllegalArgumentException(
          "Cannot merge different number of BAM and BAI files in " + tempPartsDirectory);
    }
    int i = 0;
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    try (OutputStream out = fileSystemWrapper.create(conf, outputFile)) {
      IndexMerger<I> indexMerger = newIndexMerger(out, partLengths.get(i++));
      List<Callable<I>> callables =
          filteredParts.stream()
              .map(part -> (Callable<I>) () -> readIndex(conf, part, header))
              .collect(Collectors.toList());
      for (Future<I> futureIndex : executorService.invokeAll(callables)) {
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
    logger.info("Done merging {} files", getIndexExtension());
  }

  protected abstract I readIndex(Configuration conf, String part, H header) throws IOException;
}
