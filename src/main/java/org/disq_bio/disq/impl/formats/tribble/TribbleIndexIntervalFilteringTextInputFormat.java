package org.disq_bio.disq.impl.formats.tribble;

import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.Locatable;
import htsjdk.tribble.index.Block;
import htsjdk.tribble.index.Index;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * A {@link TextInputFormat} that uses a tribble index to filter out any splits that don't overlap
 * any interval in a given set. Note that this format is not aware of the record type, so it cannot
 * filter records - this must be done by the code using this class.
 */
public class TribbleIndexIntervalFilteringTextInputFormat extends TextInputFormat {

  private static Index index;
  private static List<? extends Locatable> intervals;

  public static void setIndex(Index index) {
    TribbleIndexIntervalFilteringTextInputFormat.index = index;
  }

  public static void setIntervals(List<? extends Locatable> intervals) {
    TribbleIndexIntervalFilteringTextInputFormat.intervals = intervals;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);

    // filter out any splits that do not overlap intervals, by using the tribble index
    List<InputSplit> filteredSplits = new ArrayList<>();
    List<Block> blocks = new ArrayList<>();
    for (Locatable interval : intervals) {
      String contig = interval.getContig();
      int intervalStart = interval.getStart();
      int intervalEnd = interval.getEnd();
      blocks.addAll(index.getBlocks(contig, intervalStart, intervalEnd));
    }
    for (InputSplit split : splits) {
      if (!(split instanceof FileSplit)) {
        filteredSplits.add(split);
      } else {
        FileSplit fileSplit = (FileSplit) split;
        long virtualSplitStart =
            BlockCompressedFilePointerUtil.makeFilePointer(fileSplit.getStart());
        long virtualSplitEnd =
            BlockCompressedFilePointerUtil.makeFilePointer(
                fileSplit.getStart() + fileSplit.getLength());
        for (Block block : blocks) {
          long blockStart = block.getStartPosition();
          long blockEnd = block.getEndPosition();
          if (overlaps(virtualSplitStart, virtualSplitEnd, blockStart, blockEnd)) {
            filteredSplits.add(split);
            break;
          }
        }
      }
    }
    return filteredSplits;
  }

  private static boolean overlaps(long start, long end, long start2, long end2) {
    return (start2 >= start && start2 <= end)
        || (end2 >= start && end2 <= end)
        || (start >= start2 && end <= end2);
  }
}
