package org.disq_bio.disq;

import htsjdk.samtools.util.Locatable;
import java.util.List;

/**
 * Holds parameters controlling which reads get returned when reading a {@link HtsjdkReadsRdd}.
 * Parameters include the intervals to include, and a flag controlling whether unplaced, unmapped
 * reads should be returned.
 *
 * @param <T> the type of Locatable
 */
public class HtsjdkReadsTraversalParameters<T extends Locatable> {
  private final List<T> intervalsForTraversal;
  private final boolean traverseUnplacedUnmapped;

  public HtsjdkReadsTraversalParameters(
      List<T> intervalsForTraversal, boolean traverseUnplacedUnmapped) {
    this.intervalsForTraversal = intervalsForTraversal;
    this.traverseUnplacedUnmapped = traverseUnplacedUnmapped;
  }

  public List<T> getIntervalsForTraversal() {
    return intervalsForTraversal;
  }

  public boolean getTraverseUnplacedUnmapped() {
    return traverseUnplacedUnmapped;
  }
}
