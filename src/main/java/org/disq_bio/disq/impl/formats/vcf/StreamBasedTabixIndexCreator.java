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
package org.disq_bio.disq.impl.formats.vcf;

import htsjdk.samtools.BinningIndexContent;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.tribble.index.Index;
import htsjdk.tribble.index.tabix.TabixFormat;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.tribble.index.tabix.TabixIndexCreator;
import htsjdk.tribble.index.tabix.TabixIndexMerger;
import htsjdk.tribble.util.LittleEndianOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;

// TODO: enhance htsjdk so that it's possible to write a tabix index to a stream (not just a path)
// see IndexingVariantContextWriter, which calls writeBasedOnFeaturePath
public class StreamBasedTabixIndexCreator extends TabixIndexCreator {

  static class StreamBasedTabixIndex extends TabixIndex {
    private final OutputStream out;

    StreamBasedTabixIndex(
        TabixFormat formatSpec,
        List<String> sequenceNames,
        BinningIndexContent[] indices,
        OutputStream out) {
      super(formatSpec, sequenceNames, indices);
      this.out = out;
    }

    @Override
    public void writeBasedOnFeaturePath(final Path featurePath) throws IOException {
      try (final LittleEndianOutputStream los =
          new LittleEndianOutputStream(new BlockCompressedOutputStream(out, (Path) null))) {
        write(los);
      }
    }
  }

  private final OutputStream out;

  public StreamBasedTabixIndexCreator(
      SAMSequenceDictionary sequenceDictionary, TabixFormat formatSpec, OutputStream out) {
    super(sequenceDictionary, formatSpec);
    this.out = out;
  }

  @Override
  public Index finalizeIndex(long finalFilePosition) {
    Index index = super.finalizeIndex(finalFilePosition);
    TabixIndex tabixIndex = (TabixIndex) index;
    return new StreamBasedTabixIndex(
        tabixIndex.getFormatSpec(),
        tabixIndex.getSequenceNames(),
        TabixIndexMerger.getBinningIndexContents(tabixIndex),
        out);
  }
}
