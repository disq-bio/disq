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
package org.disq_bio.disq.impl.formats.bam;

import htsjdk.samtools.SBIIndex;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.disq_bio.disq.BaseTest;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

public class BamRecordGuesserCheckerTest extends BaseTest {

  private static final int SPLIT_SIZE = 128 * 1024;

  @Test
  public void testAllCorrectGranularityOne() throws Exception {
    String inputPath = ClassLoader.getSystemClassLoader().getResource("1.bam").toURI().toString();
    BamRecordGuesserChecker bamRecordGuesserChecker = new BamRecordGuesserChecker(true);
    JavaPairRDD<Long, BamRecordGuesserChecker.RecordStartResult> mismatchesRdd =
        bamRecordGuesserChecker.check(jsc, inputPath, SPLIT_SIZE);
    Assert.assertEquals(0, mismatchesRdd.count());
  }

  @Test
  public void testAllCorrectGranularityOverOne() throws Exception {
    String inputPath =
        ClassLoader.getSystemClassLoader()
            .getResource("1-with-splitting-index.bam")
            .toURI()
            .toString();
    BamRecordGuesserChecker bamRecordGuesserChecker = new BamRecordGuesserChecker(true);
    JavaPairRDD<Long, BamRecordGuesserChecker.RecordStartResult> mismatchesRdd =
        bamRecordGuesserChecker.check(jsc, inputPath, SPLIT_SIZE);
    Assert.assertEquals(0, mismatchesRdd.count());
  }

  @Test
  public void testFalsePositiveAndFalseNegativeDetected() throws Exception {
    String inputPath = ClassLoader.getSystemClassLoader().getResource("1.bam").toURI().toString();
    BamRecordGuesserChecker bamRecordGuesserChecker = new BamRecordGuesserChecker(true);
    SBIIndex sbiIndex = bamRecordGuesserChecker.getSBIIndex(jsc.hadoopConfiguration(), inputPath);

    // create a doctored index
    SBIIndex.Header header =
        new SBIIndex.Header(
            sbiIndex.dataFileLength(),
            new byte[16],
            new byte[16],
            sbiIndex.size(),
            sbiIndex.getGranularity());
    long[] virtualOffsets = sbiIndex.getVirtualOffsets();
    long missingOffset = virtualOffsets[0];
    long newOffset = missingOffset + 1;
    // remove first offset, which should then be detected as a false positive
    // and add new offset, which should then be detected as a false negative
    virtualOffsets[0] = newOffset;
    sbiIndex = new SBIIndex(header, virtualOffsets);

    JavaPairRDD<Long, BamRecordGuesserChecker.RecordStartResult> mismatchesRdd =
        bamRecordGuesserChecker.check(jsc, inputPath, SPLIT_SIZE, sbiIndex);
    List<Tuple2<Long, BamRecordGuesserChecker.RecordStartResult>> mismatches =
        mismatchesRdd.collect();
    Assert.assertEquals(2, mismatches.size());
    Assert.assertEquals(missingOffset, mismatches.get(0)._1.longValue());
    Assert.assertEquals(
        BamRecordGuesserChecker.RecordStartResult.FALSE_POSITIVE, mismatches.get(0)._2);
    Assert.assertEquals(newOffset, mismatches.get(1)._1.longValue());
    Assert.assertEquals(
        BamRecordGuesserChecker.RecordStartResult.FALSE_NEGATIVE, mismatches.get(1)._2);
  }
}
