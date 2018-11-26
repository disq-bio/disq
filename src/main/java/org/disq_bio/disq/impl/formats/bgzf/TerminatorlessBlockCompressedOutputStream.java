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
package org.disq_bio.disq.impl.formats.bgzf;

import htsjdk.samtools.util.BlockCompressedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An extension of {@link BlockCompressedOutputStream} that doesn't write an empty BGZF block at the
 * end of the stream.
 */
public class TerminatorlessBlockCompressedOutputStream extends BlockCompressedOutputStream {
  private final OutputStream out;

  public TerminatorlessBlockCompressedOutputStream(OutputStream os, File file) {
    super(os, file);
    this.out = os;
  }

  @Override
  public void close() throws IOException {
    super.flush();
    out.close();
  }
}
