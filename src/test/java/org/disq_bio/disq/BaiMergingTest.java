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

import static org.junit.Assert.assertArrayEquals;

import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.BaiEqualityChecker;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.RuntimeIOException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.disq_bio.disq.impl.formats.sam.SamFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class BaiMergingTest extends BaseTest {

  private Object[] parametersForTest() throws IOException {
    String dir = "src/test/resources";
    Object[] objects =
        Files.walk(Paths.get(dir))
            .filter(p -> p.toString().endsWith(".bam"))
            .filter(p -> size(p) > 64 * 1024)
            .filter(BaiMergingTest::isCoordinateSorted)
            .toArray();
    Assert.assertTrue(objects.length > 0);
    return objects;
  }

  private static long size(Path p) {
    try {
      return Files.size(p);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static boolean isCoordinateSorted(Path p) {
    return SamReaderFactory.makeDefault()
            .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS)
            .open(p)
            .getFileHeader()
            .getSortOrder()
        == SAMFileHeader.SortOrder.coordinate;
  }

  @Test
  @Parameters
  public void test(String inputFile) throws Exception {
    String inputPath = getPath(inputFile);

    long inputFileLength = new File(inputPath).length();
    int splitSize = (int) (inputFileLength + 1) / 3; // three part files
    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc)
            .splitSize(splitSize)
            .validationStringency(ValidationStringency.SILENT);
    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    String outputPath =
        createTempPath(SamFormat.fromFormatWriteOption(ReadsFormatWriteOption.BAM).getExtension());
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath, BaiWriteOption.ENABLE);

    File outputBam = new File(URI.create(outputPath));
    File outputBai = new File(outputBam.getParent(), outputBam.getName() + ".bai");
    String outputBaiHtsjdkPath = createTempPath(".bai");
    File outputBaiHtsjdk = indexBam(outputBam, new File(URI.create(outputBaiHtsjdkPath)));

    // Check equality on object before comparing file contents to get a better indication
    // of the difference in case they are not equal.
    BaiEqualityChecker.assertEquals(outputBam, outputBaiHtsjdk, outputBai);

    assertArrayEquals(
        com.google.common.io.Files.toByteArray(outputBaiHtsjdk),
        com.google.common.io.Files.toByteArray(outputBai));
  }

  private static File indexBam(File bam) throws IOException {
    File bai = new File(bam.getParent(), bam.getName() + ".bai");
    return indexBam(bam, bai);
  }

  private static File indexBam(File bam, File bai) throws IOException {
    try (SamReader in =
        SamReaderFactory.makeDefault()
            .validationStringency(ValidationStringency.SILENT)
            .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS)
            .disable(SamReaderFactory.Option.VALIDATE_CRC_CHECKSUMS)
            .open(SamInputResource.of(bam))) {

      final BAMIndexer indexer = new BAMIndexer(bai, in.getFileHeader());
      for (final SAMRecord rec : in) {
        indexer.processAlignment(rec);
      }

      indexer.finish();
    }
    textIndexBai(bai);
    return bai;
  }

  // create a human-readable BAI
  private static File textIndexBai(File bai) {
    File textBai = new File(bai.toString() + ".txt");
    BAMIndexer.createAndWriteIndex(bai, textBai, true);
    return textBai;
  }
}
