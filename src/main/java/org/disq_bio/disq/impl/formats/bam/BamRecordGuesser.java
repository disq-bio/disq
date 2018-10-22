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
package org.disq_bio.disq.impl.formats.bam;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.hadoop.io.IOUtils;

class BamRecordGuesser implements Closeable {

  private static final int READS_TO_CHECK = 10;

  private final BlockCompressedInputStream uncompressedBytes;
  private final int referenceSequenceCount;
  private final SAMFileHeader header;

  private final ByteBuffer buf = ByteBuffer.allocate(36).order(ByteOrder.LITTLE_ENDIAN);

  private final ByteBuffer readNameBuffer = ByteBuffer.allocate(255).order(ByteOrder.LITTLE_ENDIAN);

  private final ByteBuffer cigarOpBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

  public BamRecordGuesser(SeekableStream ss, int referenceSequenceCount, SAMFileHeader header) {
    this.uncompressedBytes = new BlockCompressedInputStream(ss);
    this.referenceSequenceCount = referenceSequenceCount;
    this.header = header;
  }

  public boolean checkRecordStart(long vPos) {
    return checkRecordStart(vPos, 0);
  }

  private boolean checkRecordStart(long vPos, int successfulReads) {
    if (successfulReads == READS_TO_CHECK) {
      return true;
    }
    try {
      Result result = checkRecordStartInternal(vPos);
      // recursive call stack is up to `readsToCheck` calls deep, which is OK
      return result.isStart() && checkRecordStart(result.getNextVPos(), successfulReads + 1);
    } catch (EOFException e) {
      // EOF is OK if at least one read has been successfully checked (e.g. for last read in file)
      return successfulReads > 0;
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    uncompressedBytes.close();
  }

  static class Result {
    private final boolean start;
    private final long nextVPos;

    public Result(boolean start, long nextVPos) {
      this.start = start;
      this.nextVPos = nextVPos;
    }

    public boolean isStart() {
      return start;
    }

    public long getNextVPos() {
      return nextVPos;
    }
  }

  private static final Result NO_START = new Result(false, -1);

  private Result checkRecordStartInternal(long vPos) throws IOException {
    // The fields in a BAM record, are as follows, plus auxiliary data at the end (ignored here)
    //
    // Field      Length (bytes)   Cumulative offset (bytes)
    // ---------- ---------------- ---------------------------------------------
    // block_size 4                0
    // refID      4                4
    // pos        4                8
    // bin_mq_nl  4                12
    // flag_nc    4                16
    // l_seq      4                20
    // next_refID 4                24
    // next_pos   4                28
    // t_len      4                32
    // read_name  l_read_name      36 + l_read_name
    // cigar      n_cigar_op       36 + l_read_name
    // seq        (l_seq + 1)/2    36 + l_read_name + n_cigar_op
    // qual       l_seq            36 + l_read_name + n_cigar_op + (l_seq + 1)/2

    seek(uncompressedBytes, vPos);
    readFully(uncompressedBytes, buf.array(), 0, 36);

    final int remainingBytes = buf.getInt(0);

    // If the first two checks fail we have what looks like a valid
    // reference sequence ID. Assume we're at offset [4] or [24], i.e.
    // the ID of either this read or its mate, respectively. So check
    // the next integer ([8] or [28]) to make sure it's a 0-based
    // leftmost coordinate.
    final int id = buf.getInt(4);
    final int pos = buf.getInt(8);
    if (id < -1 || id >= referenceSequenceCount || pos < -1) {
      return NO_START;
    }

    if (id >= 0 && pos > header.getSequenceDictionary().getSequence(id).getSequenceLength()) {
      return NO_START; // Locus too large
    }

    // Okay, we could be at [4] or [24]. Assuming we're at [4], check
    // that [24] is valid. Assume [4] because we should hit it first:
    // the only time we expect to hit [24] is at the beginning of the
    // split, as part of the first read we should skip.

    final int nid = buf.getInt(24);
    final int npos = buf.getInt(28);
    if (nid < -1 || nid >= referenceSequenceCount || npos < -1) {
      return NO_START;
    }

    if (nid >= 0 && npos > header.getSequenceDictionary().getSequence(nid).getSequenceLength()) {
      return NO_START; // Locus too large
    }

    // So far so good: [4] and [24] seem okay. Now do something a bit
    // more involved: make sure that [36 + [12]&0xff - 1] == 0: that
    // is, the name of the read should be null terminated.

    final int nameLength = buf.getInt(12) & 0xff;
    if (nameLength < 2) {
      // Names are null-terminated so length must be greater than one
      return NO_START;
    }

    int flags = buf.getInt(16) >>> 16;
    int numCigarOps = buf.getInt(16) & 0xffff;
    int cigarOpsLength = numCigarOps * 4;
    int seqLength = buf.getInt(20) + (buf.getInt(20) + 1) / 2;

    if ((flags & 4) == 0 && (seqLength == 0 || numCigarOps == 0)) {
      return NO_START; // Non-empty cigar/seq in mapped reads
    }

    // Pos 36 + nameLength
    readNameBuffer.position(0);
    readNameBuffer.limit(nameLength);
    readFully(uncompressedBytes, readNameBuffer.array(), 0, nameLength);

    if (readNameBuffer.get(nameLength - 1) != 0) {
      return NO_START; // Read-name ends with `\0`
    }

    for (int i = 0; i < nameLength - 1; i++) {
      if (!isValidReadNameCharacter(readNameBuffer.get(i))) {
        return NO_START; // Invalid read-name chars
      }
    }

    for (int i = 0; i < numCigarOps; i++) {
      readFully(uncompressedBytes, cigarOpBuffer.array(), 0, 4);
      int read = cigarOpBuffer.getInt(0);
      if (read == -1) {
        throw new EOFException();
      }
      if (!isValidCigarOp(read)) {
        return NO_START; // Cigar ops valid
      }
    }

    // All of [4], [24], and [36 + [12]&0xff] look good. If [0] is also
    // sensible, that's good enough for us. "Sensible" to us means the
    // following:
    //
    // [0] >= 4*([16]&0xffff) + [20] + ([20]+1)/2 + 4*8 + ([12]&0xff)

    // Note that [0] is "length of the _remainder_ of the alignment
    // record", which is why this uses 4*8 instead of 4*9.
    int zeroMin = 4 * 8 + nameLength + cigarOpsLength + seqLength;

    if (remainingBytes >= zeroMin) {
      seek(uncompressedBytes, vPos);
      IOUtils.skipFully(uncompressedBytes, 4 + remainingBytes);
      return new Result(true, uncompressedBytes.getPosition());
    }
    return NO_START;
  }

  private static boolean isValidReadNameCharacter(byte b) {
    return ((byte) '!' <= b && b <= (byte) '?') || ((byte) 'A' <= b && b <= (byte) '~');
  }

  private boolean isValidCigarOp(int read) {
    return (read & 0xf) <= 8;
  }

  // Modifies BlockCompressedInputStream#seek to throw EOFException when an attempting to seek past
  // EOF
  private static void seek(BlockCompressedInputStream blockCompressedInputStream, long pos)
      throws IOException {
    try {
      blockCompressedInputStream.seek(pos);
    } catch (IOException e) {
      if (e.getMessage().startsWith("Invalid file pointer")) {
        throw new EOFException(e.getMessage());
      }
      throw e;
    }
  }

  // Duplicate of method in Hadoop IOUtils except it throws EOFException rather than IOException for
  // EOF
  private static void readFully(InputStream in, byte buf[], int off, int len) throws IOException {
    int toRead = len;
    while (toRead > 0) {
      int ret = in.read(buf, off, toRead);
      if (ret < 0) {
        throw new EOFException("Premature EOF from inputStream");
      }
      toRead -= ret;
      off += ret;
    }
  }
}
