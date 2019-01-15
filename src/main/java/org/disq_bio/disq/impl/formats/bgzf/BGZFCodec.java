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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;

/**
 * A Hadoop {@link CompressionCodec} for the <a
 * href="https://samtools.github.io/hts-specs/SAMv1.pdf">BGZF compression format</a>, which reads
 * and writes files with a <code>.bgz</code> suffix. There is no standard suffix for BGZF-compressed
 * files, and in fact <code>.gz</code> is commonly used, in which case {@link BGZFEnhancedGzipCodec}
 * should be used instead of this class.
 *
 * <p>To use BGZFCodec, set it on the configuration object as follows. {@code
 * conf.set("io.compression.codecs", BGZFCodec.class.getCanonicalName()) }
 *
 * @see BGZFEnhancedGzipCodec
 */
public class BGZFCodec extends GzipCodec implements SplittableCompressionCodec {

  public static final String DEFAULT_EXTENSION = ".bgz";

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return new BGZFCompressionOutputStream(out);
  }

  // compressors are not used, so ignore/return null

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
      throws IOException {
    return createOutputStream(out); // compressors are not used, so ignore
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return null; // compressors are not used, so return null
  }

  @Override
  public Compressor createCompressor() {
    return null; // compressors are not used, so return null
  }

  @Override
  public SplitCompressionInputStream createInputStream(
      InputStream seekableIn, Decompressor decompressor, long start, long end, READ_MODE readMode)
      throws IOException {

    String source = this.toString();
    SeekableStream ss = new HadoopFileSystemWrapper.SeekableHadoopStream(seekableIn, end, source);
    BgzfBlockGuesser splitGuesser = new BgzfBlockGuesser(ss, null);
    BgzfBlockGuesser.BgzfBlock bgzfBlock = splitGuesser.guessNextBGZFPos(start, end);
    long adjustedStart = bgzfBlock != null ? bgzfBlock.pos : end;
    ((Seekable) seekableIn).seek(adjustedStart);
    return new BGZFSplitCompressionInputStream(seekableIn, adjustedStart, end);
  }

  // fall back to GzipCodec for input streams without a start position

  @Override
  public String getDefaultExtension() {
    return DEFAULT_EXTENSION;
  }
}
