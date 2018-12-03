package htsjdk.samtools;

import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.Log;

import java.io.OutputStream;

/** Merges SBI files for parts of a file that have been concatenated. */
public final class SBIIndexMerger {

  private static final Log log = Log.getInstance(SBIIndexMerger.class);

  private SBIIndexWriter indexWriter;
  private long granularity = -1;
  private long offset;
  private long recordCount;
  private long finalVirtualOffset;

  /**
   * Prepare to merge SBI indexes.
   *
   * @param out the stream to write the merged index to
   * @param headerLength the length of any header that precedes the first part of the data file with
   *     an index
   */
  public SBIIndexMerger(final OutputStream out, long headerLength) {
    this.indexWriter = new SBIIndexWriter(out);
    this.offset = headerLength;
  }

  /**
   * Add an index for a part of the data file to the merged index. This method should be called for
   * each index for the data file parts, in order.
   */
  public void processIndex(SBIIndex index) {
    long[] virtualOffsets = index.getVirtualOffsets();
    for (int i = 0; i < virtualOffsets.length - 1; i++) {
      indexWriter.writeVirtualOffset(shiftVirtualFilePointer(virtualOffsets[i], offset));
    }
    finalVirtualOffset = shiftVirtualFilePointer(virtualOffsets[virtualOffsets.length - 1], offset);

    SBIIndex.Header header = index.getHeader();
    offset += header.getFileLength();
    recordCount += header.getTotalNumberOfRecords();
    if (granularity == -1) { // first time being set
      granularity = header.getGranularity();
    } else if (granularity > 0 && granularity != header.getGranularity()) {
      log.warn("Different granularities so setting to 0 (unspecified)");
      granularity = 0;
    }
  }

  /**
   * Move a virtual file pointer by a given (non-virtual) offset.
   *
   * @param virtualFilePointer the original virtual file pointer
   * @param offset the offset in bytes
   * @return a new virtual file pointer shifted by the given offset
   */
  private static long shiftVirtualFilePointer(long virtualFilePointer, long offset) {
    long blockAddress = BlockCompressedFilePointerUtil.getBlockAddress(virtualFilePointer);
    int blockOffset = BlockCompressedFilePointerUtil.getBlockOffset(virtualFilePointer);
    return BlockCompressedFilePointerUtil.makeFilePointer(blockAddress + offset, blockOffset);
  }

  /**
   * Complete the index, and close the output stream.
   *
   * @param dataFileLength the length of the data file in bytes
   */
  public void finish(long dataFileLength) {
    SBIIndex.Header header =
        new SBIIndex.Header(
            dataFileLength,
            SBIIndexWriter.EMPTY_MD5,
            SBIIndexWriter.EMPTY_UUID,
            recordCount,
            granularity);
    indexWriter.finish(header, finalVirtualOffset);
  }
}
