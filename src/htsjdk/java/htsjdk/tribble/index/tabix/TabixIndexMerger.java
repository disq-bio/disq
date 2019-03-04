package htsjdk.tribble.index.tabix;

import htsjdk.samtools.BAMIndexMerger;
import htsjdk.samtools.BinningIndexContent;
import htsjdk.samtools.IndexMerger;
import htsjdk.samtools.LinearIndex;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.tribble.util.LittleEndianOutputStream;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Merges tabix files for parts of a file that have been concatenated. */
public class TabixIndexMerger extends IndexMerger<TabixIndex> {

  private TabixFormat formatSpec;
  private final List<String> sequenceNames = new ArrayList<>();
  private final List<List<BinningIndexContent>> content = new ArrayList<>();

  public TabixIndexMerger(final OutputStream out, final long headerLength) {
    super(out, headerLength);
  }

  @Override
  public void processIndex(TabixIndex index, long partLength) {
    this.partLengths.add(partLength);
    if (content.isEmpty()) {
      formatSpec = index.getFormatSpec();
      if (index.getSequenceNames() != null) {
        sequenceNames.addAll(index.getSequenceNames());
      }
      for (int ref = 0; ref < sequenceNames.size(); ref++) {
        content.add(new ArrayList<>());
      }
    }
    if (!index.getFormatSpec().equals(formatSpec)) {
      throw new IllegalArgumentException(
          String.format("Cannot merge tabix files with different formats, %s and %s.", index.getFormatSpec(), formatSpec));
    }
    if (!sequenceNames.equals(index.getSequenceNames())) {
      throw new IllegalArgumentException(
          String.format("Cannot merge tabix files with different sequence names, %s and %s.", index.getSequenceNames(), sequenceNames));
    }
    for (int ref = 0; ref < sequenceNames.size(); ref++) {
      List<BinningIndexContent> binningIndexContentList = content.get(ref);
      binningIndexContentList.add(getBinningIndexContent(index, ref));
    }
  }

  @Override
  public void finish(long dataFileLength) throws IOException {
    if (content.isEmpty()) {
      throw new IllegalArgumentException("Cannot merge zero tabix files");
    }
    long[] offsets = partLengths.stream().mapToLong(i -> i).toArray();
    Arrays.parallelPrefix(offsets, (a, b) -> a + b); // cumulative offsets

    List<BinningIndexContent> mergedBinningIndexContentList = new ArrayList<>();
    for (int ref = 0; ref < sequenceNames.size(); ref++) {
      List<BinningIndexContent> binningIndexContentList = content.get(ref);
      BinningIndexContent binningIndexContent = mergeBinningIndexContent(ref, binningIndexContentList, offsets);
      mergedBinningIndexContentList.add(binningIndexContent);
    }
    TabixIndex tabixIndex = new TabixIndex(formatSpec, sequenceNames, mergedBinningIndexContentList.toArray(new BinningIndexContent[0]));
    try (LittleEndianOutputStream los = new LittleEndianOutputStream(new BlockCompressedOutputStream(out, (File) null))) {
      tabixIndex.write(los);
    }
  }

  /**
   * Get the binning indexes for a tabix index. This method is needed since
   * {@link TabixIndex} doesn't provide access.
   * @param tbi the tabix index
   * @return the array of binning indexes
   */
  public static BinningIndexContent[] getBinningIndexContents(TabixIndex tbi) {
    // TODO: change htsjdk to allow access
    try {
      Field indices = TabixIndex.class.getDeclaredField("indices");
      indices.setAccessible(true);
      BinningIndexContent[] contents = (BinningIndexContent[]) indices.get(tbi);
      // create a defensive copy, (shallow, but OK since objects are immutable)
      return Arrays.copyOf(contents, contents.length);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the binning index for a reference sequence for a tabix index. This
   * method is needed since {@link TabixIndex} doesn't provide access.
   * @param tbi the tabix index
   * @param ref the reference sequence
   * @return the binning index
   */
  public static BinningIndexContent getBinningIndexContent(TabixIndex tbi, int ref) {
    return getBinningIndexContents(tbi)[ref];
  }

  private static BinningIndexContent mergeBinningIndexContent(int referenceSequence, List<BinningIndexContent> binningIndexContentList, long[] offsets) {
    List<BinningIndexContent.BinList> binLists = new ArrayList<>();
    List<LinearIndex> linearIndexes = new ArrayList<>();
    for (BinningIndexContent binningIndexContent : binningIndexContentList) {
      binLists.add(binningIndexContent.getBins());
      linearIndexes.add(binningIndexContent.getLinearIndex());
    }
    return new BinningIndexContent(referenceSequence, BAMIndexMerger.mergeBins(binLists, offsets), BAMIndexMerger.mergeLinearIndexes(referenceSequence, linearIndexes, offsets));
  }
}
