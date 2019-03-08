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
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SBIIndex;
import htsjdk.samtools.SBIIndexMerger;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;

public class SbiMerger extends IndexFileMerger<SBIIndex, SAMFileHeader> {

  public SbiMerger(FileSystemWrapper fileSystemWrapper) {
    super(fileSystemWrapper);
  }

  @Override
  protected String getIndexExtension() {
    return SBIIndex.FILE_EXTENSION;
  }

  @Override
  protected IndexMerger<SBIIndex> newIndexMerger(OutputStream out, long headerLength) {
    return new SBIIndexMerger(out, headerLength);
  }

  @Override
  protected SBIIndex readIndex(Configuration conf, String part, SAMFileHeader header)
      throws IOException {
    SBIIndex index;
    try (SeekableStream in = fileSystemWrapper.open(conf, part)) {
      index = SBIIndex.load(in);
    }
    fileSystemWrapper.delete(conf, part);
    return index;
  }
}
