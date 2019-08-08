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

import htsjdk.samtools.CRAMCRAIIndexer;
import htsjdk.samtools.IndexMerger;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.cram.CRAIIndex;
import htsjdk.samtools.cram.CRAIIndexMerger;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.disq_bio.disq.CraiWriteOption;

/** Merges CRAM index files for (headerless) parts of a CRAM file into a single index file. */
public class CraiMerger extends IndexFileMerger<CRAIIndex, SAMFileHeader> {
  public CraiMerger(FileSystemWrapper fileSystemWrapper) {
    super(fileSystemWrapper);
  }

  @Override
  protected String getIndexExtension() {
    return CraiWriteOption.getIndexExtension();
  }

  @Override
  protected IndexMerger<CRAIIndex> newIndexMerger(OutputStream out, long headerLength)
      throws IOException {
    return new CRAIIndexMerger(out, headerLength);
  }

  @Override
  protected CRAIIndex readIndex(Configuration conf, String file, SAMFileHeader header)
      throws IOException {
    CRAIIndex index;
    try (SeekableStream in = fileSystemWrapper.open(conf, file)) {
      index = CRAMCRAIIndexer.readIndex(in);
    }
    fileSystemWrapper.delete(conf, file);
    return index;
  }
}
