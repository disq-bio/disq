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

import static org.junit.Assert.assertArrayEquals;

import htsjdk.samtools.CRAMCRAIIndexer;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.RuntimeIOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
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
public class CraiMergingTest extends BaseTest {

  private static String cramReferenceFile = "human_g1k_v37.20.21.fasta.gz";

  private Object[] parametersForTest() throws IOException {
    try {
      String dir = "src/test/resources";
      Object[] objects =
          Files.walk(Paths.get(dir))
              .filter(p -> p.toString().endsWith(".cram"))
              .filter(p -> size(p) > 64 * 1024)
              .filter(CraiMergingTest::isCoordinateSorted)
              .toArray();
      Assert.assertTrue(objects.length > 0);
      return objects;
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private static long size(Path p) {
    try {
      return Files.size(p);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static boolean isCoordinateSorted(Path p) {
    try {
      SamReaderFactory factory =
          SamReaderFactory.makeDefault()
              .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS)
              .referenceSequence(Paths.get(URI.create(getPath(cramReferenceFile))));
      return factory.open(p).getFileHeader().getSortOrder() == SAMFileHeader.SortOrder.coordinate;
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Parameters
  public void test(String inputFile) throws Exception {
    String inputPath = getPath(inputFile);
    String refPath = getPath(cramReferenceFile);

    long inputFileLength = new File(inputPath).length();
    int splitSize = (int) (inputFileLength + 1) / 3; // three part files
    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc)
            .splitSize(splitSize)
            .referenceSourcePath(refPath)
            .validationStringency(ValidationStringency.SILENT);
    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    String outputPath =
        createTempPath(SamFormat.fromFormatWriteOption(ReadsFormatWriteOption.CRAM).getExtension());
    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath, CraiWriteOption.ENABLE);

    File outputCram = new File(URI.create(outputPath));
    File outputCrai = new File(outputCram.getParent(), outputCram.getName() + ".crai");
    String outputCraiHtsjdkPath = createTempPath(".crai");
    File outputCraiHtsjdk = indexCram(outputCram, new File(URI.create(outputCraiHtsjdkPath)));

    assertArrayEquals(
        com.google.common.io.Files.toByteArray(outputCraiHtsjdk),
        com.google.common.io.Files.toByteArray(outputCrai));
  }

  private static File indexCram(File cram, File crai) throws IOException {
    try (SeekableStream in = new SeekableFileStream(cram);
        OutputStream out = new FileOutputStream(crai)) {
      CRAMCRAIIndexer.writeIndex(in, out);
    }
    return crai;
  }
}
