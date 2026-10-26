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

import htsjdk.samtools.util.FileExtensions;
import htsjdk.samtools.util.RuntimeIOException;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.CloseableTribbleIterator;
import htsjdk.tribble.FeatureReader;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.disq_bio.disq.impl.formats.vcf.VcfFormat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class TbiMergingTest extends BaseTest {

  // Smaller than TBI's smallest bin, 16,384 bases, and not a divisor of it, so that queries
  // straddle bin boundaries.
  private static final int QUERY_WINDOW = 10_000;

  private Object[] parametersForTest() throws IOException {
    String dir = "src/test/resources";
    Object[] objects =
        Files.walk(Paths.get(dir))
            .filter(p -> p.toString().endsWith(".vcf.bgz"))
            .filter(p -> size(p) > 64 * 1024)
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

  @Test
  @Parameters
  public void test(String inputFile) throws Exception {
    String inputPath = getPath(inputFile);

    long inputFileLength = new File(inputPath).length();
    int splitSize = (int) (inputFileLength + 1) / 3; // three part files
    HtsjdkVariantsRddStorage htsjdkVariantsRddStorage =
        HtsjdkVariantsRddStorage.makeDefault(jsc).splitSize(splitSize);
    HtsjdkVariantsRdd htsjdkVariantsRdd = htsjdkVariantsRddStorage.read(inputPath);

    String outputPath =
        createTempPath(
            VcfFormat.fromFormatWriteOption(VariantsFormatWriteOption.VCF_BGZ).getExtension());
    htsjdkVariantsRddStorage.write(htsjdkVariantsRdd, outputPath, TabixIndexWriteOption.ENABLE);

    File outputVcf = new File(URI.create(outputPath));
    File outputTbi =
        new File(outputVcf.getParent(), outputVcf.getName() + FileExtensions.TABIX_INDEX);
    String outputTbiHtsjdkPath = createTempPath(FileExtensions.TABIX_INDEX);
    File outputTbiHtsjdk = indexVcf(outputVcf, new File(URI.create(outputTbiHtsjdkPath)));

    // The indexes need not be identical: htsjdk folds a small bin into its parent, and a bin can
    // look small in one part file when it is not in the whole file. So compare what they find.
    assertIndexesFindSameRecords(outputVcf, outputTbiHtsjdk, outputTbi);
  }

  /**
   * Asserts that querying a VCF through each of two indexes finds the same records, for every
   * window of {@link #QUERY_WINDOW} bases from each contig's first record to the end of its last.
   */
  private static void assertIndexesFindSameRecords(File vcf, File expectedTbi, File actualTbi)
      throws IOException {
    Map<String, int[]> contigSpans = new LinkedHashMap<>();
    try (FeatureReader<VariantContext> reader =
        AbstractFeatureReader.getFeatureReader(vcf.getPath(), new VCFCodec(), false)) {
      for (VariantContext vc : reader.iterator()) {
        contigSpans.merge(
            vc.getContig(),
            new int[] {vc.getStart(), vc.getEnd()},
            (a, b) -> new int[] {Math.min(a[0], b[0]), Math.max(a[1], b[1])});
      }
    }
    Assert.assertFalse("No records in " + vcf, contigSpans.isEmpty());

    try (FeatureReader<VariantContext> expectedReader = openWithIndex(vcf, expectedTbi);
        FeatureReader<VariantContext> actualReader = openWithIndex(vcf, actualTbi)) {
      for (Map.Entry<String, int[]> contigSpan : contigSpans.entrySet()) {
        String contig = contigSpan.getKey();
        int[] span = contigSpan.getValue();
        for (int start = span[0]; start <= span[1]; start += QUERY_WINDOW) {
          int end = start + QUERY_WINDOW - 1;
          Assert.assertEquals(
              contig + ":" + start + "-" + end,
              describe(expectedReader.query(contig, start, end)),
              describe(actualReader.query(contig, start, end)));
        }
      }
    }
  }

  private static FeatureReader<VariantContext> openWithIndex(File vcf, File tbi) {
    return AbstractFeatureReader.getFeatureReader(
        vcf.getPath(), tbi.getPath(), new VCFCodec(), true);
  }

  private static List<String> describe(CloseableTribbleIterator<VariantContext> records) {
    List<String> descriptions = new ArrayList<>();
    try (CloseableTribbleIterator<VariantContext> iterator = records) {
      iterator.forEachRemaining(
          vc ->
              descriptions.add(
                  vc.getContig()
                      + ":"
                      + vc.getStart()
                      + "-"
                      + vc.getEnd()
                      + " "
                      + vc.getAlleles()));
    }
    return descriptions;
  }

  private static File indexVcf(File vcf, File tbi) throws IOException {
    TabixIndex tabixIndex = IndexFactory.createTabixIndex(vcf.toPath(), new VCFCodec(), null);
    tabixIndex.write(tbi.toPath());
    return tbi;
  }
}
