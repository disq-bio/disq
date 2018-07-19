package org.disq_bio.disq.impl.formats;

import htsjdk.samtools.util.RuntimeIOException;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * An {@link Iterator} that automatically closes a resource when the end of the iteration is
 * reached.
 *
 * @param <E>
 */
public class AutocloseIteratorWrapper<E> implements Iterator<E> {

  private final Iterator<E> iterator;
  private final Closeable closeable;

  public AutocloseIteratorWrapper(Iterator<E> iterator, Closeable closeable) {
    this.iterator = iterator;
    this.closeable = closeable;
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = iterator.hasNext();
    if (!hasNext) {
      try {
        closeable.close();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }
    return hasNext;
  }

  @Override
  public E next() {
    return iterator.next();
  }

  @Override
  public void remove() {
    iterator.remove();
  }

  @Override
  public void forEachRemaining(Consumer<? super E> action) {
    iterator.forEachRemaining(action);
  }
}
