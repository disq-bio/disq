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
