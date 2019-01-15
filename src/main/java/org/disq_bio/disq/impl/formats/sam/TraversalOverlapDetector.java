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
package org.disq_bio.disq.impl.formats.sam;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.Locatable;
import htsjdk.samtools.util.OverlapDetector;
import java.util.List;
import org.disq_bio.disq.HtsjdkReadsTraversalParameters;

class TraversalOverlapDetector<T extends Locatable> extends OverlapDetector<T> {
  private final boolean traverseUnplacedUnmapped;

  public TraversalOverlapDetector(HtsjdkReadsTraversalParameters<T> traversalParameters) {
    super(0, 0);
    this.traverseUnplacedUnmapped = traversalParameters.getTraverseUnplacedUnmapped();
    if (traversalParameters.getIntervalsForTraversal() != null
        && !traversalParameters.getIntervalsForTraversal().isEmpty()) {
      List<T> intervals = traversalParameters.getIntervalsForTraversal();
      addAll(intervals, intervals);
    }
  }

  @Override
  public boolean overlapsAny(Locatable locatable) {
    if (traverseUnplacedUnmapped && locatable instanceof SAMRecord) {
      SAMRecord record = (SAMRecord) locatable;
      if (record.getReadUnmappedFlag()
          && record.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START) {
        return true; // include record if unmapped records should be traversed and record is
        // unmapped
      }
    }
    return super.overlapsAny(locatable);
  }
}
