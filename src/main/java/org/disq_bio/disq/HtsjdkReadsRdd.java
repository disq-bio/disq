/*
 * Disq
 *
 * MIT License
 *
 * Copyright (c) 2018 Disq contributors
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
