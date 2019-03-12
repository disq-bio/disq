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

import htsjdk.samtools.util.BlockCompressedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import org.apache.hadoop.io.compress.CompressionOutputStream;

/**
 * An implementation of {@code CompressionOutputStream} for BGZF, using {@link
 * BlockCompressedOutputStream} from htsjdk. Note that unlike {@link BlockCompressedOutputStream},
 * an empty gzip block file terminator is <i>not</i> written at the end of the stream. This is
 * because in Hadoop, multiple headerless files are often written in parallel, and merged afterwards
 * into a single file, and it's during the merge process the header and terminator are added.
 */
public class BGZFCompressionOutputStream extends CompressionOutputStream {

  private BlockCompressedOutputStream output;

  public BGZFCompressionOutputStream(OutputStream out) throws IOException {
    super(out);
    this.output = new BlockCompressedOutputStream(out, (Path) null);
  }

  public void write(int b) throws IOException {
    output.write(b);
  }

  public void write(byte[] b, int off, int len) throws IOException {
    output.write(b, off, len);
  }

  public void finish() throws IOException {
    output.flush();
  }

  public void resetState() throws IOException {
    output.flush();
    output = new BlockCompressedOutputStream(out, (Path) null);
  }

  public void close() throws IOException {
    output.flush(); // don't close as we don't want to write terminator (empty gzip block)
    out.close();
  }
}
