package org.disq_bio.disq;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.spark.api.java.JavaRDD;

/**
 * A {@link HtsjdkReadsRdd} is the distributed equivalent of a htsjdk {@link
 * htsjdk.samtools.SamReader}. It represents a SAM, BAM, or CRAM file stored in a distributed
 * filesystem, and encapsulates a Spark RDD containing the reads in it.
 *
 * <p>Use a {@link HtsjdkReadsRddStorage} to read and write {@link HtsjdkReadsRdd}s.
 *
 * @see HtsjdkReadsRddStorage
 */
public class HtsjdkReadsRdd {

  private final SAMFileHeader header;
  private final JavaRDD<SAMRecord> reads;

  public HtsjdkReadsRdd(SAMFileHeader header, JavaRDD<SAMRecord> reads) {
    this.header = header;
    this.reads = reads;
  }

  /**
   * @return the header for the reads in this RDD. In the case that different reads have different
   *     headers, it is undefined which header this method returns.
   */
  public SAMFileHeader getHeader() {
    return header;
  }

  /** @return a RDD of {@link SAMRecord} with headers. */
  public JavaRDD<SAMRecord> getReads() {
    return reads;
  }
}
