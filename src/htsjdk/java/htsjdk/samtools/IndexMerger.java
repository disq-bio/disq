package htsjdk.samtools;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class IndexMerger<T> {
  protected final OutputStream out;
  protected final List<Long> partLengths;

  public IndexMerger(final OutputStream out, final long headerLength) {
    this.out = out;
    this.partLengths = new ArrayList<>();
    this.partLengths.add(headerLength);
  }

  public abstract void processIndex(T index, long partLength);

  public abstract void finish(long dataFileLength) throws IOException;
}
