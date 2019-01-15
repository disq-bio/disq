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
package org.disq_bio.disq.impl.formats.bgzf;

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;

/**
 * A Hadoop {@link CompressionCodec} for the <a
 * href="https://samtools.github.io/hts-specs/SAMv1.pdf">BGZF compression format</a>, which reads
 * and writes files with a <code>.gz</code> suffix.
 *
 * <p>BGZF is a splittable extension of gzip, which means that all BGZF files are standard gzip
 * files, however the reverse is not necessarily the case. BGZF files often have the standard <code>
 * .gz</code> suffix (such as those produced by the <code>bcftools</code> command), which causes a
 * difficulty since it is not immediately apparent from the filename alone whether a file is a BGZF
 * file, or merely a regular gzip file. BGZFEnhancedGzipCodec will read the start of the file to
 * look for BGZF headers to detect the type of compression.
 *
 * <p>BGZFEnhancedGzipCodec will read BGZF or gzip files, but currently always writes regular gzip
 * files.
 *
 * <p>To use BGZFEnhancedGzipCodec, set it on the configuration object as follows. This will
 * override the built-in GzipCodec that is mapped to the <code>.gz</code> suffix. {@code
 * conf.set("io.compression.codecs", BGZFEnhancedGzipCodec.class.getCanonicalName()) }
 *
 * @see BGZFCodec
 */
public class BGZFEnhancedGzipCodec extends GzipCodec implements SplittableCompressionCodec {

  @Override
  public SplitCompressionInputStream createInputStream(
      InputStream seekableIn, Decompressor decompressor, long start, long end, READ_MODE readMode)
      throws IOException {
    if (!(seekableIn instanceof Seekable)) {
      throw new IOException("seekableIn must be an instance of " + Seekable.class.getName());
    }
    if (!BlockCompressedInputStream.isValidFile(new BufferedInputStream(seekableIn))) {
      // data is regular gzip, not BGZF
      ((Seekable) seekableIn).seek(0);
      final CompressionInputStream compressionInputStream =
          createInputStream(seekableIn, decompressor);
      return new SplitCompressionInputStream(compressionInputStream, start, end) {
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          return compressionInputStream.read(b, off, len);
        }

        @Override
        public void resetState() throws IOException {
          compressionInputStream.resetState();
        }

        @Override
        public int read() throws IOException {
          return compressionInputStream.read();
        }
      };
    }
    String source = this.toString();
    SeekableStream ss = new HadoopFileSystemWrapper.SeekableHadoopStream(seekableIn, end, source);
    BgzfBlockGuesser splitGuesser = new BgzfBlockGuesser(ss, null);
    BgzfBlockGuesser.BgzfBlock bgzfBlock = splitGuesser.guessNextBGZFPos(start, end);
    long adjustedStart = bgzfBlock != null ? bgzfBlock.pos : end;
    ((Seekable) seekableIn).seek(adjustedStart);
    return new BGZFSplitCompressionInputStream(seekableIn, adjustedStart, end);
  }
}
