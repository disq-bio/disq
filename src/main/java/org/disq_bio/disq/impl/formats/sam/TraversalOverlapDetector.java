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
