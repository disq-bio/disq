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

import htsjdk.samtools.AbstractBAMFileIndex;
import htsjdk.samtools.BAMIndexMerger;
import htsjdk.samtools.IndexMerger;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.cram.io.InputStreamUtils;
import htsjdk.samtools.seekablestream.ByteArraySeekableStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.disq_bio.disq.BaiWriteOption;

/** Merges BAM index files for (headerless) parts of a BAM file into a single index file. */
public class BaiMerger extends IndexFileMerger<AbstractBAMFileIndex, SAMFileHeader> {
  public BaiMerger(FileSystemWrapper fileSystemWrapper) {
    super(fileSystemWrapper);
  }

  @Override
  protected String getIndexExtension() {
    return BaiWriteOption.getIndexExtension();
  }

  @Override
  protected IndexMerger<AbstractBAMFileIndex> newIndexMerger(OutputStream out, long headerLength) {
    return new BAMIndexMerger(out, headerLength);
  }

  @Override
  protected AbstractBAMFileIndex readIndex(Configuration conf, String file, SAMFileHeader header)
      throws IOException {
    AbstractBAMFileIndex index;
    try (SeekableStream in = fileSystemWrapper.open(conf, file)) {
      // read all bytes into memory since BAMIndexMerger.openIndex is lazy
      byte[] bytes = InputStreamUtils.readFully(in);
      SeekableStream allIn = new ByteArraySeekableStream(bytes);
      index = BAMIndexMerger.openIndex(allIn, header.getSequenceDictionary());
    }
    fileSystemWrapper.delete(conf, file);
    return index;
  }
}
