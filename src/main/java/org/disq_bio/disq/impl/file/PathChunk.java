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
package org.disq_bio.disq.impl.file;

import htsjdk.samtools.Chunk;
import java.io.Serializable;
import java.util.Objects;

/** Stores the virtual span of a partition for a file path. */
public class PathChunk implements Serializable {
  private final String path;
  private final Chunk span;

  public PathChunk(String path, Chunk span) {
    this.path = path;
    this.span = span;
  }

  public String getPath() {
    return path;
  }

  public Chunk getSpan() {
    return span;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PathChunk pathChunk = (PathChunk) o;
    return Objects.equals(path, pathChunk.path) && Objects.equals(span, pathChunk.span);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, span);
  }

  @Override
  public String toString() {
    return "PathChunk{" + "path=" + path + ", span=" + span + '}';
  }
}
