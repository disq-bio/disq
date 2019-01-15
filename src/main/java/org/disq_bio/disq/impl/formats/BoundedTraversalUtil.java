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
