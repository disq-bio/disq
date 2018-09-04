package org.disq_bio.disq.impl.formats;

import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.Locatable;
import java.util.List;

public class BoundedTraversalUtil {

  public static <T extends Locatable> QueryInterval[] prepareQueryIntervals(
      final List<T> rawIntervals, final SAMSequenceDictionary sequenceDictionary) {
    if (rawIntervals == null || rawIntervals.isEmpty()) {
      return null;
    }

    // Convert each SimpleInterval to a QueryInterval
    final QueryInterval[] convertedIntervals =
        rawIntervals
            .stream()
            .map(
                rawInterval ->
                    convertSimpleIntervalToQueryInterval(rawInterval, sequenceDictionary))
            .toArray(QueryInterval[]::new);

    // Intervals must be optimized (sorted and merged) in order to use the htsjdk query API
    return QueryInterval.optimizeIntervals(convertedIntervals);
  }
  /**
   * Converts an interval in SimpleInterval format into an htsjdk QueryInterval.
   *
   * <p>In doing so, a header lookup is performed to convert from contig name to index
   *
   * @param interval interval to convert
   * @param sequenceDictionary sequence dictionary used to perform the conversion
   * @return an equivalent interval in QueryInterval format
   */
  private static <T extends Locatable> QueryInterval convertSimpleIntervalToQueryInterval(
      final T interval, final SAMSequenceDictionary sequenceDictionary) {
    if (interval == null) {
      throw new IllegalArgumentException("interval may not be null");
    }
    if (sequenceDictionary == null) {
      throw new IllegalArgumentException("sequence dictionary may not be null");
    }

    final int contigIndex = sequenceDictionary.getSequenceIndex(interval.getContig());
    if (contigIndex == -1) {
      throw new IllegalArgumentException(
          "Contig " + interval.getContig() + " not present in reads sequence " + "dictionary");
    }

    return new QueryInterval(contigIndex, interval.getStart(), interval.getEnd());
  }
}
