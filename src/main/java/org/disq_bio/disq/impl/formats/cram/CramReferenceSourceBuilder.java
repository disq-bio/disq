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
package org.disq_bio.disq.impl.formats.cram;

import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.reference.BlockCompressedIndexedFastaSequenceFile;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.FileExtensions;
import htsjdk.samtools.util.GZIIndex;
import htsjdk.samtools.util.IOUtil;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.disq_bio.disq.impl.file.FileSystemWrapper;

/** A utility class for creating a {@link CRAMReferenceSource}. */
public class CramReferenceSourceBuilder {
  public static CRAMReferenceSource build(
      FileSystemWrapper fileSystemWrapper, Configuration conf, String referenceSourcePath)
      throws IOException {
    SeekableStream refIn = fileSystemWrapper.open(conf, referenceSourcePath);
    String indexPath = referenceSourcePath + FileExtensions.FASTA_INDEX;
    ReferenceSequenceFile refSeqFile;
    if (IOUtil.hasBlockCompressedExtension(referenceSourcePath)) {
      String gziIndexPath = referenceSourcePath + FileExtensions.GZI;
      try (SeekableStream indexIn = fileSystemWrapper.open(conf, indexPath);
          SeekableStream gziIndexIn = fileSystemWrapper.open(conf, gziIndexPath)) {
        FastaSequenceIndex index = new FastaSequenceIndex(indexIn);
        GZIIndex gziIndex = GZIIndex.loadIndex(gziIndexPath, gziIndexIn);
        refSeqFile =
            new BlockCompressedIndexedFastaSequenceFile(
                referenceSourcePath, refIn, index, null, gziIndex);
      }
    } else {
      try (SeekableStream indexIn = fileSystemWrapper.open(conf, indexPath)) {
        FastaSequenceIndex index = new FastaSequenceIndex(indexIn);
        refSeqFile =
            ReferenceSequenceFileFactory.getReferenceSequenceFile(
                referenceSourcePath, refIn, index);
      }
    }
    return new ReferenceSource(refSeqFile);
  }
}
