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
package org.disq_bio.disq;

import htsjdk.samtools.BAMSBIIndexer;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SBIIndex;
import htsjdk.samtools.seekablestream.SeekablePathStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.Locatable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.disq_bio.disq.impl.formats.sam.SamFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class HtsjdkReadsRddTest extends BaseTest {

  private Object[] parametersForTestReadAndWrite() {
    return new Object[][] {
      {"1.bam", null, ReadsFormatWriteOption.BAM, 128 * 1024, false},
      {"1.bam", null, ReadsFormatWriteOption.BAM, 128 * 1024, true},
      {"valid.cram", "valid.fasta", ReadsFormatWriteOption.CRAM, 128 * 1024, false},
      {"valid.cram", "valid.fasta", ReadsFormatWriteOption.CRAM, 128 * 1024, true},
      {"valid_no_index.cram", "valid.fasta", ReadsFormatWriteOption.CRAM, 128 * 1024, false},
      {"test.sam", null, ReadsFormatWriteOption.SAM, 128 * 1024, false},
      {
        "gs://genomics-public-data/NA12878.chr20.sample.bam",
        null,
        ReadsFormatWriteOption.BAM,
        128 * 1024,
        true
      },
      {
        "gs://genomics-public-data/NA12878.chr20.sample.bam",
        null,
        ReadsFormatWriteOption.BAM,
        0,
        true
      },
      // The split size 76458 was chosen since it is a bgzf block boundary, and it exposes a bug
      // where reads were included in both splits either side of the boundary. See
      // BamSourceTest#testPathChunksDontOverlap
      {
        "HiSeq.1mb.1RG.2k_lines.alternate.recalibrated.DIQ.sharded.bam/part-r-00000.bam",
        null,
        ReadsFormatWriteOption.BAM,
        76458,
        false
      },
      {
        "HiSeq.1mb.1RG.2k_lines.alternate.recalibrated.DIQ.sharded.bam/part-r-00000.bam",
        null,
        ReadsFormatWriteOption.BAM,
        76458,
        true
      },
    };
  }

  @Test
  @Parameters
  public void testReadAndWrite(
      String inputFile,
      String cramReferenceFile,
      ReadsFormatWriteOption formatWriteOption,
      int splitSize,
      boolean useNio)
      throws Exception {
    String inputPath = getPath(inputFile);
    String refPath = getPath(cramReferenceFile);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc)
            .splitSize(splitSize)
            .useNio(useNio)
            .referenceSourcePath(refPath);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    // reduce to realize the RDD of reads
    Assert.assertNotNull(htsjdkReadsRdd.getReads().first());

    // read the file using htsjdk to get expected number of reads, then count the number in the RDD
    int expectedCount = AnySamTestUtil.countReads(inputPath, refPath);
    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());

    // write the RDD back to a file
    String outputPath =
        createTempPath(SamFormat.fromFormatWriteOption(formatWriteOption).getExtension());
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath);

    // check the new file has the number of expected reads
    Assert.assertEquals(expectedCount, AnySamTestUtil.countReads(outputPath, refPath));
    if (SamtoolsTestUtil.isSamtoolsAvailable()) {
      Assert.assertEquals(expectedCount, SamtoolsTestUtil.countReads(outputPath, refPath));
    }

    // check we can read back what we've just written
    Assert.assertEquals(expectedCount, htsjdkReadsRddStorage.read(outputPath).getReads().count());
  }

  private Object[] parametersForTestReadUsingSBIIndex() {
    return new Object[][] {
      {"1-with-splitting-index.bam", 128 * 1024, false},
      {"1-with-splitting-index.bam", 128 * 1024, true},
    };
  }

  @Test
  @Parameters
  public void testReadUsingSBIIndex(String inputFile, int splitSize, boolean useNio)
      throws Exception {
    String inputPath = getPath(inputFile);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc).splitSize(splitSize).useNio(useNio);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    // read the file using htsjdk to get expected number of reads, then count the number in the RDD
    int expectedCount = AnySamTestUtil.countReads(inputPath);
    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());
  }

  private Object[] parametersForTestWriteSBIIndex() {
    return new Object[][] {
      {"1.bam", 128 * 1024, false},
      {"1.bam", 128 * 1024, true},
    };
  }

  @Test
  @Parameters
  public void testWriteSBIIndex(String inputFile, int splitSize, boolean useNio) throws Exception {
    String inputPath = getPath(inputFile);

    // use a granularity of 1 for the index so we index every read - allows us to compare indexes
    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc)
            .splitSize(splitSize)
            .useNio(useNio)
            .sbiIndexGranularity(1);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    String outputPath =
        createTempPath(SamFormat.fromFormatWriteOption(ReadsFormatWriteOption.BAM).getExtension());
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath, SbiWriteOption.ENABLE);

    Path sbiFile = Paths.get(URI.create(outputPath + SBIIndex.FILE_EXTENSION));
    Assert.assertTrue(Files.exists(sbiFile));
    SBIIndex actualSbiIndex = SBIIndex.load(sbiFile);

    ByteArrayOutputStream expectedSbiContents = new ByteArrayOutputStream();
    try (SeekableStream in = new SeekablePathStream(Paths.get(URI.create(outputPath)))) {
      BAMSBIIndexer.createIndex(in, expectedSbiContents, 1);
    }
    SBIIndex expectedSbiIndex =
        SBIIndex.load(new ByteArrayInputStream(expectedSbiContents.toByteArray()));

    Assert.assertEquals(expectedSbiIndex.getHeader(), actualSbiIndex.getHeader());
    Assert.assertArrayEquals(
        expectedSbiIndex.getVirtualOffsets(), actualSbiIndex.getVirtualOffsets());
  }

  private Object[] parametersForTestReadAndWriteMultiple() {
    return new Object[][] {
      {null, false, ReadsFormatWriteOption.BAM},
      {"test.fa", false, ReadsFormatWriteOption.CRAM},
      {null, false, ReadsFormatWriteOption.SAM},
    };
  }

  @Test
  @Parameters
  public void testReadAndWriteMultiple(
      String cramReferenceFile, boolean useNio, ReadsFormatWriteOption formatWriteOption)
      throws Exception {

    String refPath = getPath(cramReferenceFile);

    // Read in a single large (generated) BAM/CRAM/SAM file
    String inputPath =
        AnySamTestUtil.writeAnySamFile(
            1000, SAMFileHeader.SortOrder.coordinate, formatWriteOption, refPath);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc)
            .splitSize(40000)
            .useNio(useNio)
            .referenceSourcePath(refPath);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    // check that there are multiple partitions
    Assert.assertTrue(htsjdkReadsRdd.getReads().getNumPartitions() > 1);

    // read the file using htsjdk to get expected number of reads, then count the number in the RDD
    int expectedCount = AnySamTestUtil.countReads(inputPath, refPath);
    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());

    // write as multiple BAM/CRAM/SAM files
    String outputPath = createTempPath("");
    htsjdkReadsRddStorage.write(
        htsjdkReadsRdd, outputPath, FileCardinalityWriteOption.MULTIPLE, formatWriteOption);

    // check the new file has the number of expected reads
    int totalCount = 0;
    for (String part : listPartFiles(outputPath)) {
      totalCount += AnySamTestUtil.countReads(part, refPath);
    }
    Assert.assertEquals(expectedCount, totalCount);

    // for multiple file, check there are no sbi files
    Assert.assertTrue(listSBIIndexFiles(outputPath).isEmpty());

    if (SamtoolsTestUtil.isSamtoolsAvailable()) {
      int totalCountSamtools = 0;
      for (String part : listPartFiles(outputPath)) {
        totalCountSamtools += SamtoolsTestUtil.countReads(part, refPath);
      }
      Assert.assertEquals(expectedCount, totalCountSamtools);
    }

    // check we can read back what we've just written
    Assert.assertEquals(expectedCount, htsjdkReadsRddStorage.read(outputPath).getReads().count());
  }

  private Object[] parametersForTestReadIntervals() {
    return new Object[][] {
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            false),
        ReadsFormatWriteOption.BAM
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 1, 1000135) // covers whole chromosome
                ),
            false),
        ReadsFormatWriteOption.BAM
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            true),
        ReadsFormatWriteOption.BAM
      },
      {null, new HtsjdkReadsTraversalParameters<>(null, true), ReadsFormatWriteOption.BAM},
      {
        null,
        new HtsjdkReadsTraversalParameters<>(Collections.emptyList(), true),
        ReadsFormatWriteOption.BAM
      },
      {
        "test.fa",
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            false),
        ReadsFormatWriteOption.CRAM
      },
      {
        "test.fa",
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 1, 1000135) // covers whole chromosome
                ),
            false),
        ReadsFormatWriteOption.CRAM
      },
      {
        "test.fa",
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            true),
        ReadsFormatWriteOption.CRAM
      },
      {"test.fa", new HtsjdkReadsTraversalParameters<>(null, true), ReadsFormatWriteOption.CRAM},
      {
        "test.fa",
        new HtsjdkReadsTraversalParameters<>(Collections.emptyList(), true),
        ReadsFormatWriteOption.CRAM
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            false),
        ReadsFormatWriteOption.SAM
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 1, 1000135) // covers whole chromosome
                ),
            false),
        ReadsFormatWriteOption.SAM
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            true),
        ReadsFormatWriteOption.SAM
      },
      {null, new HtsjdkReadsTraversalParameters<>(null, true), ReadsFormatWriteOption.SAM},
      {
        null,
        new HtsjdkReadsTraversalParameters<>(Collections.emptyList(), true),
        ReadsFormatWriteOption.SAM
      },
    };
  }

  @Test
  @Parameters
  public <T extends Locatable> void testReadIntervals(
      String cramReferenceFile,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      ReadsFormatWriteOption formatWriteOption)
      throws Exception {
    String refPath = getPath(cramReferenceFile);

    // Read in a single large (generated) BAM/CRAM/SAM file
    String inputPath =
        AnySamTestUtil.writeAnySamFile(
            1000, SAMFileHeader.SortOrder.coordinate, formatWriteOption, refPath);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc)
            .splitSize(40000)
            .useNio(false)
            .referenceSourcePath(refPath);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath, traversalParameters);

    // read the file using htsjdk to get expected number of reads, then count the number in the RDD
    int expectedCount = AnySamTestUtil.countReads(inputPath, refPath, traversalParameters);
    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());

    // also check the count with samtools (except for SAM since it cannot do intervals)
    if (SamtoolsTestUtil.isSamtoolsAvailable()
        && !formatWriteOption.equals(ReadsFormatWriteOption.SAM)) {
      int expectedCountSamtools =
          SamtoolsTestUtil.countReads(inputPath, refPath, traversalParameters);
      Assert.assertEquals(expectedCountSamtools, htsjdkReadsRdd.getReads().count());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMappedOnlyFails() throws Exception {
    String inputPath =
        AnySamTestUtil.writeAnySamFile(
            1000, SAMFileHeader.SortOrder.coordinate, ReadsFormatWriteOption.BAM, null);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc).splitSize(40000).useNio(false);

    htsjdkReadsRddStorage.read(inputPath, new HtsjdkReadsTraversalParameters<>(null, false));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWritingToADirectoryWithoutAFormatFails() throws IOException {

    String outputPath = createTempPath(""); // no extension to signal format

    HtsjdkReadsRddStorage htsjdkReadsRddStorage = HtsjdkReadsRddStorage.makeDefault(jsc);
    htsjdkReadsRddStorage.write(null, outputPath); // RDD is ignored, so OK to pass in null
  }

  @Test
  public void testOverwrite() throws IOException, URISyntaxException {
    String inputPath = getPath("1.bam");

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc).splitSize(128 * 1024).useNio(false);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);
    int expectedCount = AnySamTestUtil.countReads(inputPath);

    String outputPath = createTempPath(SamFormat.BAM.getExtension());
    Path p = Paths.get(URI.create(outputPath));
    Files.createFile(p); // create the file to check that overwrite works
    Assert.assertTrue(Files.exists(p));
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath);
    Assert.assertEquals(expectedCount, AnySamTestUtil.countReads(outputPath));
  }

  @Test
  public void testSBIIndexWrittenWhenNotCoordinateSorted() throws Exception {
    String inputPath =
        AnySamTestUtil.writeAnySamFile(
            1000, SAMFileHeader.SortOrder.queryname, ReadsFormatWriteOption.BAM, null);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage = HtsjdkReadsRddStorage.makeDefault(jsc);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    String outputPath =
        createTempPath(SamFormat.fromFormatWriteOption(ReadsFormatWriteOption.BAM).getExtension());
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath, SbiWriteOption.ENABLE);

    Assert.assertTrue(Files.exists(Paths.get(URI.create(outputPath + SBIIndex.FILE_EXTENSION))));
  }

  @Test
  public void testEnableBAIAndSBI() throws Exception {
    String inputPath =
        AnySamTestUtil.writeAnySamFile(
            1000, SAMFileHeader.SortOrder.coordinate, ReadsFormatWriteOption.BAM, null);

    HtsjdkReadsRddStorage htsjdkReadsRddStorage = HtsjdkReadsRddStorage.makeDefault(jsc);

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    String bamExtension =
        SamFormat.fromFormatWriteOption(ReadsFormatWriteOption.BAM).getExtension();

    // default (both disabled)
    String outputPath = createTempPath(bamExtension);
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath);
    Assert.assertFalse(
        Files.exists(Paths.get(URI.create(outputPath + BaiWriteOption.getIndexExtension()))));
    Assert.assertFalse(
        Files.exists(Paths.get(URI.create(outputPath + SbiWriteOption.getIndexExtension()))));

    // bai only
    outputPath = createTempPath(bamExtension);
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath, BaiWriteOption.ENABLE);
    Assert.assertTrue(
        Files.exists(Paths.get(URI.create(outputPath + BaiWriteOption.getIndexExtension()))));
    Assert.assertFalse(
        Files.exists(Paths.get(URI.create(outputPath + SbiWriteOption.getIndexExtension()))));

    // sbi only
    outputPath = createTempPath(bamExtension);
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath, SbiWriteOption.ENABLE);
    Assert.assertFalse(
        Files.exists(Paths.get(URI.create(outputPath + BaiWriteOption.getIndexExtension()))));
    Assert.assertTrue(
        Files.exists(Paths.get(URI.create(outputPath + SbiWriteOption.getIndexExtension()))));

    // both bai and sbi
    outputPath = createTempPath(bamExtension);
    htsjdkReadsRddStorage.write(
        htsjdkReadsRdd, outputPath, BaiWriteOption.ENABLE, SbiWriteOption.ENABLE);
    Assert.assertTrue(
        Files.exists(Paths.get(URI.create(outputPath + BaiWriteOption.getIndexExtension()))));
    Assert.assertTrue(
        Files.exists(Paths.get(URI.create(outputPath + SbiWriteOption.getIndexExtension()))));
  }
}
