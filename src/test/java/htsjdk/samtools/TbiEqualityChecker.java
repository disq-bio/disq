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
package htsjdk.samtools;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.tribble.index.tabix.TabixIndexMerger;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
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

    int numReferences = tbi1.getSequenceNames().size();
    for (int i = 0; i < numReferences; i++) {
      BinningIndexContent binningIndexContent1 = TabixIndexMerger.getBinningIndexContent(tbi1, i);
      BinningIndexContent binningIndexContent2 = TabixIndexMerger.getBinningIndexContent(tbi2, i);
      assertEquals(binningIndexContent1, binningIndexContent2, identical);
    }
  }

  private void assertEquals(
      BinningIndexContent binningIndexContent1,
      BinningIndexContent binningIndexContent2,
      boolean identical)
      throws IOException {

    assertEquals(binningIndexContent1.getBins(), binningIndexContent2.getBins(), identical);
    assertEquals(binningIndexContent1.getLinearIndex(), binningIndexContent2.getLinearIndex());
  }

  private void assertEquals(
      BinningIndexContent.BinList bins1, BinningIndexContent.BinList bins2, boolean identical)
      throws IOException {
    Assert.assertEquals("Max bin number", bins1.maxBinNumber, bins2.maxBinNumber);
    Assert.assertEquals(
        "Number of non-null bins", bins1.getNumberOfNonNullBins(), bins2.getNumberOfNonNullBins());
    for (int i = 0; i <= bins1.maxBinNumber; i++) {
      assertEquals(bins1.getBin(i), bins2.getBin(i), identical || i != bins1.maxBinNumber);
    }
  }

  private void assertEquals(Bin bin1, Bin bin2, boolean identical) throws IOException {
    if (bin1 == null || bin2 == null) {
      Assert.assertEquals(bin1, bin2);
      return;
    }
    Assert.assertEquals("Bin number", bin1.getBinNumber(), bin2.getBinNumber());
    Assert.assertEquals("Chunk list size", bin1.getChunkList().size(), bin2.getChunkList().size());
    for (int i = 0; i < bin1.getChunkList().size(); i++) {
      assertEquals(bin1.getChunkList().get(i), bin2.getChunkList().get(i), identical);
    }
  }

  private void assertEquals(LinearIndex linearIndex1, LinearIndex linearIndex2) {
    Assert.assertEquals(
        "Linear index ref",
        linearIndex1.getReferenceSequence(),
        linearIndex2.getReferenceSequence());
    Assert.assertEquals("Linear index size", linearIndex1.size(), linearIndex2.size());
    Assert.assertEquals(
        "Linear index start", linearIndex1.getIndexStart(), linearIndex2.getIndexStart());
    assertArrayEquals(
        "Linear index entries", linearIndex1.getIndexEntries(), linearIndex2.getIndexEntries());
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

  private void dump(BinningIndexContent binningIndexContent) {
    PrintWriter pw = new PrintWriter(System.out);
    writeReference(pw, binningIndexContent);
  }

  public static void dump(PrintWriter pw, TabixIndex tbi) {
    BinningIndexContent[] binningIndexContents = TabixIndexMerger.getBinningIndexContents(tbi);
    for (BinningIndexContent content : binningIndexContents) {
      writeReference(pw, content);
    }
  }

  public static void writeReference(PrintWriter pw, final BinningIndexContent content) {

    final int reference = content.getReferenceSequence();

    final BAMIndexContent.BinList bins = content.getBins();
    final int size = bins == null ? 0 : content.getNumberOfNonNullBins();

    if (size == 0) {
      pw.println("Reference " + reference + " has n_bin=0");
      pw.println("Reference " + reference + " has n_intv=0");
      return;
    }

    //    pw.println("Reference " + reference + " has n_bin= " + Integer.toString(size + (metaData
    // != null? 1 : 0)));

    // chunks
    for (final Bin bin : bins) { // note, bins will always be sorted
      if (bin.getBinNumber() == GenomicIndexUtil.MAX_BINS) break;
      if (bin.getChunkList() == null) {
        pw.println(
            "  Ref " + reference + " bin " + bin.getBinNumber() + " has no binArray"); // remove?
        continue;
      }
      final List<Chunk> chunkList = bin.getChunkList();
      if (chunkList == null) {
        pw.println("  Ref " + reference + " bin " + bin.getBinNumber() + " has no chunkList");
        continue;
      }
      pw.print(
          "  Ref "
              + reference
              + " bin "
              + bin.getBinNumber()
              + " has n_chunk= "
              + chunkList.size());
      if (chunkList.isEmpty()) {
        pw.println();
      }
      for (final Chunk c : chunkList) {
        pw.println(
            "     Chunk: "
                + c.toString()
                + " start: "
                + Long.toString(c.getChunkStart(), 16)
                + " end: "
                + Long.toString(c.getChunkEnd(), 16));
      }
    }

    // linear index
    final LinearIndex linearIndex = content.getLinearIndex();
    if (linearIndex == null || linearIndex.getIndexEntries() == null) {
      pw.println("Reference " + reference + " has n_intv= 0");
      return;
    }
    final long[] entries = linearIndex.getIndexEntries();
    final int indexStart = linearIndex.getIndexStart();
    // System.out.println("index start is " + indexStart);
    final int n_intv = entries.length + indexStart;
    pw.println("Reference " + reference + " has n_intv= " + n_intv);
    for (int k = 0; k < entries.length; k++) {
      if (entries[k] != 0) {
        pw.println(
            "  Ref "
                + reference
                + " ioffset for "
                + (k + indexStart)
                + " is "
                + Long.toString(entries[k]));
      }
    }
    pw.flush(); // write each reference to disk as it's being created
  }
}
