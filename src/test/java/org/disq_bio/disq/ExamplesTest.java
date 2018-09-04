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
