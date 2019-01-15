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

import com.google.common.io.Files;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.Interval;
import htsjdk.tribble.util.TabixUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.*;
import java.net.URI;
import java.util.Iterator;

public class VcfTestUtil {
  public static VCFFileReader parseVcf(File vcf) throws IOException {
    File actualVcf;
    // work around TribbleIndexedFeatureReader not reading header from .bgz files
    if (vcf.getName().endsWith(".bgz")) {
      actualVcf = File.createTempFile(vcf.getName(), ".gz");
      actualVcf.deleteOnExit();
      Files.copy(vcf, actualVcf);
      File tbi = new File(vcf.getParent(), vcf.getName() + TabixUtils.STANDARD_INDEX_EXTENSION);
      if (tbi.exists()) {
        File actualTbi =
            new File(
                actualVcf.getParent(), actualVcf.getName() + TabixUtils.STANDARD_INDEX_EXTENSION);
        actualTbi.deleteOnExit();
        Files.copy(tbi, actualTbi);
      }
    } else {
      actualVcf = vcf;
    }
    return new VCFFileReader(actualVcf, false);
  }

  public static int countVariants(final String vcfPath) throws IOException {
    return countVariants(vcfPath, null);
  }

  public static int countVariants(final String vcfPath, Interval interval) throws IOException {
    File vcfFile = new File(URI.create(vcfPath));
    final VCFFileReader vcfFileReader = parseVcf(vcfFile);
    final Iterator<VariantContext> it;
    if (interval == null) {
      it = vcfFileReader.iterator();
    } else {
      it = vcfFileReader.query(interval.getContig(), interval.getStart(), interval.getEnd());
    }
    int recCount = 0;
    while (it.hasNext()) {
      it.next();
      recCount++;
    }
    vcfFileReader.close();
    return recCount;
  }

  public static boolean isBlockCompressed(String path) throws IOException {
    try (InputStream in =
        new BufferedInputStream(new FileInputStream(new File(URI.create(path))))) {
      return BlockCompressedInputStream.isValidFile(in);
    }
  }
}
