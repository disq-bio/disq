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
package org.disq_bio.disq.impl.formats.sam;

import htsjdk.samtools.*;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.HtsjdkReadsTraversalParameters;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.formats.cram.CramReferenceSourceBuilder;

public abstract class AbstractSamSource implements Serializable {

  protected final FileSystemWrapper fileSystemWrapper;

  protected AbstractSamSource(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

  public abstract SamFormat getSamFormat();

  public SAMFileHeader getFileHeader(
      JavaSparkContext jsc,
      String path,
      ValidationStringency stringency,
      String referenceSourcePath)
      throws IOException {
    Configuration conf = jsc.hadoopConfiguration();
    String firstSamPath;
    if (fileSystemWrapper.isDirectory(conf, path)) {
      firstSamPath = fileSystemWrapper.firstFileInDirectory(conf, path);
    } else {
      firstSamPath = path;
    }
    try (SamReader samReader =
        createSamReader(conf, firstSamPath, stringency, referenceSourcePath)) {
      return samReader.getFileHeader();
    }
  }

  public abstract <T extends Locatable> JavaRDD<SAMRecord> getReads(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      ValidationStringency validationStringency,
      String referenceSourcePath)
      throws IOException;

  protected SamReader createSamReader(
      Configuration conf, String path, ValidationStringency stringency, String referenceSourcePath)
      throws IOException {
    SeekableStream in = fileSystemWrapper.open(conf, path);
    SeekableStream indexStream = findIndex(conf, path);
    SamReaderFactory readerFactory =
        SamReaderFactory.makeDefault()
            .setOption(SamReaderFactory.Option.CACHE_FILE_BASED_INDEXES, true)
            .setOption(SamReaderFactory.Option.EAGERLY_DECODE, false)
            .setUseAsyncIo(false);
    if (stringency != null) {
      readerFactory.validationStringency(stringency);
    }
    if (referenceSourcePath != null) {
      readerFactory.referenceSource(
          CramReferenceSourceBuilder.build(fileSystemWrapper, conf, referenceSourcePath));
    }
    SamInputResource resource = SamInputResource.of(in);
    if (indexStream != null) {
      resource.index(indexStream);
    }
    SamReader samReader = readerFactory.open(resource);
    ensureIndexWillBeClosed(samReader);
    return samReader;
  }

  protected SeekableStream findIndex(Configuration conf, String path) throws IOException {
    SamFormat samFormat = getSamFormat();
    if (samFormat.getIndexExtension() == null) {
      return null; // doesn't support indexes
    }
    String index = path + samFormat.getIndexExtension();
    if (fileSystemWrapper.exists(conf, index)) {
      return fileSystemWrapper.open(conf, index);
    }
    index =
        path.replaceFirst(
            Pattern.quote(samFormat.getExtension()) + "$", samFormat.getIndexExtension());
    if (fileSystemWrapper.exists(conf, index)) {
      return fileSystemWrapper.open(conf, index);
    }
    return null;
  }

  private SamReader ensureIndexWillBeClosed(SamReader samReader) {
    SamReader.PrimitiveSamReader underlyingReader =
        ((SamReader.PrimitiveSamReaderToSamReaderAdapter) samReader).underlyingReader();
    if (underlyingReader.hasIndex()) {
      // force BAMFileReader#mIndex to be populated so the index stream is properly
      // closed by the close() method
      underlyingReader.getIndex();
    }
    return samReader;
  }

  protected static <T> Stream<T> stream(final Iterator<T> iterator) {
    return StreamSupport.stream(((Iterable<T>) () -> iterator).spliterator(), false);
  }
}
