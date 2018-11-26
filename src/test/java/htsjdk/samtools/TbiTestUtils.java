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
package htsjdk.samtools;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;
import java.io.IOException;

public class TbiTestUtils {
  private static File indexVcfOnTheFly(File vcf, File tbi) throws IOException {
    File tmpVcf = File.createTempFile("vcf", ".vcf.bgz");
    VariantContextWriter writer =
        new VariantContextWriterBuilder()
            .setOutputFile(tmpVcf)
            .setOutputFileType(VariantContextWriterBuilder.OutputType.BLOCK_COMPRESSED_VCF)
            .build();

    try (VCFFileReader vcfFileReader = new VCFFileReader(vcf)) {
      writer.writeHeader(vcfFileReader.getFileHeader());
      for (VariantContext vc : vcfFileReader) {
        writer.add(vc);
      }
      writer.close();
    }

    new File(tmpVcf.getParent(), tmpVcf.getName() + ".tbi").renameTo(tbi);
    textIndexTbi(tbi);
    return tbi;
  }

  // create a human-readable tbi
  private static File textIndexTbi(File tbi) throws IOException {
    TabixPlainText.textIndexTbi(tbi);
    return tbi;
  }
}
