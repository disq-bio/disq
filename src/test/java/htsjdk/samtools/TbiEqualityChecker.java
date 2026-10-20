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
package htsjdk.samtools;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import htsjdk.index.BinningIndex;
import htsjdk.index.ReferenceBins;
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixIndex;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.junit.Assert;

public class TbiEqualityChecker {

  private File vcfFile;
  private File tbiFile1;
  private File tbiFile2;

  private BlockCompressedInputStream blockStream;

  public static void assertEquals(File vcfFile, File tbiFile1, File tbiFile2, boolean identical)
      throws IOException {
    new TbiEqualityChecker(vcfFile, tbiFile1, tbiFile2).assertEquals(identical);
  }

  public TbiEqualityChecker(File vcfFile, File tbiFile1, File tbiFile2) throws IOException {
    this.vcfFile = vcfFile;
    this.tbiFile1 = tbiFile1;
    this.tbiFile2 = tbiFile2;

    this.blockStream = new BlockCompressedInputStream(vcfFile);
  }

  private void assertEquals(boolean identical) throws IOException {
    TabixIndex tbi1 = (TabixIndex) IndexFactory.loadIndex(tbiFile1.getPath());
    TabixIndex tbi2 = (TabixIndex) IndexFactory.loadIndex(tbiFile2.getPath());

    Assert.assertEquals("Sequences", tbi1.getSequenceNames(), tbi2.getSequenceNames());

    BinningIndex index1 = tbi1.getBinningIndex();
    BinningIndex index2 = tbi2.getBinningIndex();
    for (int i = 0; i < index1.getReferenceCount(); i++) {
      assertEquals(index1.getReference(i), index2.getReference(i), identical);
    }
  }

  private void assertEquals(ReferenceBins reference1, ReferenceBins reference2, boolean identical)
      throws IOException {
    Assert.assertEquals("Number of bins", reference1.getBinCount(), reference2.getBinCount());
    for (int b = 0; b < reference1.getBinCount(); b++) {
      Assert.assertEquals("Bin number", reference1.getBinNumber(b), reference2.getBinNumber(b));
      List<Chunk> chunks1 = reference1.getChunks(b);
      List<Chunk> chunks2 = reference2.getChunks(b);
      Assert.assertEquals("Chunk list size", chunks1.size(), chunks2.size());
      for (int i = 0; i < chunks1.size(); i++) {
        assertEquals(chunks1.get(i), chunks2.get(i), identical);
      }
    }
    assertArrayEquals(
        "Linear index entries", reference1.getLinearIndex(), reference2.getLinearIndex());
  }

  private void assertEquals(Chunk chunk1, Chunk chunk2, boolean identical) {
    if (identical) {
      Assert.assertEquals("Chunk start", chunk1.getChunkStart(), chunk2.getChunkStart());
      Assert.assertEquals("Chunk end", chunk1.getChunkEnd(), chunk2.getChunkEnd());
    } else {
      assertEquivalent("Chunk start", chunk1.getChunkStart(), chunk2.getChunkStart());
      assertEquivalent("Chunk end", chunk1.getChunkEnd(), chunk2.getChunkEnd());
    }
  }

  private void assertEquivalent(
      String message, long virtualFilePointer1, long virtualFilePointer2) {
    // seek to the given positions check they are actually equivalent
    long norm1 = normalizeVirtualFilePointer(virtualFilePointer1);
    long norm2 = normalizeVirtualFilePointer(virtualFilePointer2);
    Assert.assertEquals(message, norm1, norm2);
  }

  private long normalizeVirtualFilePointer(long virtualFilePointer) {
    try {
      blockStream.seek(virtualFilePointer);
      return blockStream.getFilePointer();
    } catch (IOException e) {
      fail("Failed to seek to " + BlockCompressedFilePointerUtil.asString(virtualFilePointer));
      return -1; // never reached
    }
  }
}
