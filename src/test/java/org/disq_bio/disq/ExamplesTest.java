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
import htsjdk.samtools.util.Interval;
import java.io.IOException;
import java.util.Collections;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class ExamplesTest extends BaseTest {

  // First get a Spark context
  // JavaSparkContext jsc = ...

  @Test
  public void testReadBamFile() throws IOException {
    // Read a BAM file into a Spark RDD
    JavaRDD<SAMRecord> reads =
        HtsjdkReadsRddStorage.makeDefault(jsc).read("src/test/resources/1.bam").getReads();

    Assert.assertTrue(reads.count() > 0);
  }

  @Test
  public void testReadBamFileHeader() throws IOException {
    // Get the header and the reads
    HtsjdkReadsRddStorage readsStorage = HtsjdkReadsRddStorage.makeDefault(jsc);
    HtsjdkReadsRdd readsRdd = readsStorage.read("src/test/resources/1.bam");
    SAMFileHeader header = readsRdd.getHeader();
    JavaRDD<SAMRecord> reads = readsRdd.getReads();

    Assert.assertTrue(reads.count() > 0);
  }

  @Ignore
  @Test
  public void testReadBamFileOverlappingRegion() throws IOException {
    // Read a BAM file overlapping a region
    Interval chr1 = new Interval("1", 1, 1000135);
    HtsjdkReadsTraversalParameters<Interval> traversalParams =
        new HtsjdkReadsTraversalParameters<>(Collections.singletonList(chr1), false);
    JavaRDD<SAMRecord> reads =
        HtsjdkReadsRddStorage.makeDefault(jsc)
            .read("src/test/resources/1.bam", traversalParams)
            .getReads();

    Assert.assertTrue(reads.count() > 0);
  }
}
