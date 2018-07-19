package org.disq_bio.disq.impl.file;

import java.util.Objects;

/** Like Hadoop's FileSplit, but with a general path. */
public class PathSplit {
  private final String path;
  private final long start;
  private final long end;

  public PathSplit(String path, long start, long end) {
    this.path = path;
    this.start = start;
    this.end = end;
  }

  public String getPath() {
    return path;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PathSplit pathSplit = (PathSplit) o;
    return start == pathSplit.start && end == pathSplit.end && Objects.equals(path, pathSplit.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, start, end);
  }

  @Override
  public String toString() {
    return "PathSplit{" + "path='" + path + '\'' + ", start=" + start + ", end=" + end + '}';
  }
}
