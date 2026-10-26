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

import htsjdk.index.BinningIndex;
import htsjdk.index.ReferenceBins;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.index.tabix.TabixIndex;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/** Produces readable text versions of tabix files for debugging. */
public class TabixPlainText {

  public static void main(String[] args) throws IOException {
    textIndexTbi(new File(args[0]));
  }

  // create a human-readable tbi
  public static File textIndexTbi(File tbi) throws IOException {
    File textTbi = new File(tbi.toString() + ".txt");
    try (PrintWriter pw = new PrintWriter(textTbi)) {
      dump(pw, new TabixIndex(new BlockCompressedInputStream(new FileInputStream(tbi))));
    }
    return textTbi;
  }

  public static void dump(PrintWriter pw, TabixIndex tbi) {
    BinningIndex index = tbi.getBinningIndex();
    for (int reference = 0; reference < index.getReferenceCount(); reference++) {
      writeReference(pw, reference, index.getReference(reference));
    }
  }

  private static void writeReference(PrintWriter pw, int reference, ReferenceBins bins) {
    pw.println("Reference " + reference + " has n_bin=" + bins.getBinCount());
    for (int b = 0; b < bins.getBinCount(); b++) {
      final List<Chunk> chunkList = bins.getChunks(b);
      pw.println(
          "  Ref "
              + reference
              + " bin "
              + bins.getBinNumber(b)
              + " has n_chunk= "
              + chunkList.size());
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

    final long[] entries = bins.getLinearIndex();
    pw.println("Reference " + reference + " has n_intv= " + entries.length);
    for (int k = 0; k < entries.length; k++) {
      if (entries[k] != 0) {
        pw.println("  Ref " + reference + " ioffset for " + k + " is " + Long.toString(entries[k]));
      }
    }
    pw.flush(); // write each reference to disk as it's being created
  }
}
