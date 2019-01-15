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

import htsjdk.samtools.util.Locatable;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.impl.formats.vcf.VcfFormat;
import org.disq_bio.disq.impl.formats.vcf.VcfSource;

/** The entry point for reading or writing a {@link HtsjdkVariantsRdd}. */
public class HtsjdkVariantsRddStorage {

  private JavaSparkContext sparkContext;
  private int splitSize;

  /**
   * Create a {@link HtsjdkVariantsRddStorage} from a Spark context object.
   *
   * @param sparkContext the Spark context to use
   * @return a {@link HtsjdkVariantsRddStorage}
   */
  public static HtsjdkVariantsRddStorage makeDefault(JavaSparkContext sparkContext) {
    return new HtsjdkVariantsRddStorage(sparkContext);
  }

  private HtsjdkVariantsRddStorage(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  /**
   * @param splitSize the requested size of file splits in bytes when reading
   * @return the current {@link HtsjdkVariantsRddStorage}
   */
  public HtsjdkVariantsRddStorage splitSize(int splitSize) {
    this.splitSize = splitSize;
    return this;
  }

  /**
   * Read variants from the given path. The input files must be VCF format, optionally compressed.
   *
   * @param path the file or directory to read from
   * @return a {@link HtsjdkVariantsRdd} that allows access to the variants
   * @throws IOException if an IO error occurs while reading the header
   */
  public HtsjdkVariantsRdd read(String path) throws IOException {
    return read(path, null);
  }

  /**
   * Read variants from the given path, using the given intervals to filter the variants. The input
   * files must be VCF format, optionally compressed.
   *
   * @param path the file or directory to read from
   * @param intervals intervals to filter variants by
   * @param <T> the type of Locatable for specifying intervals
   * @return a {@link HtsjdkVariantsRdd} that allows access to the variants
   * @throws IOException if an IO error occurs while reading the header
   */
  public <T extends Locatable> HtsjdkVariantsRdd read(String path, List<T> intervals)
      throws IOException {
    VcfSource vcfSource = new VcfSource();
    VCFHeader header = vcfSource.getFileHeader(sparkContext, path);
    JavaRDD<VariantContext> variants =
        vcfSource.getVariants(sparkContext, path, splitSize, intervals);
    return new HtsjdkVariantsRdd(header, variants);
  }

  /**
   * Write variants to a file or files specified by the given path. Write options may be specified
   * to control the format and compression options to use (if not clear from the path extension),
   * and the number of files to write (single vs. multiple).
   *
   * @param htsjdkVariantsRdd a {@link HtsjdkVariantsRdd} containing the header and the variants
   * @param path the file or directory to write to
   * @param writeOptions options to control aspects of how to write the variants (e.g. {@link
   *     VariantsFormatWriteOption} and {@link FileCardinalityWriteOption}
   * @throws IOException if an IO error occurs while writing
   */
  public void write(HtsjdkVariantsRdd htsjdkVariantsRdd, String path, WriteOption... writeOptions)
      throws IOException {
    VariantsFormatWriteOption formatWriteOption = null;
    FileCardinalityWriteOption fileCardinalityWriteOption = null;
    TempPartsDirectoryWriteOption tempPartsDirectoryWriteOption = null;
    List<String> indexesToEnable = new ArrayList<>();
    for (WriteOption writeOption : writeOptions) {
      if (writeOption instanceof VariantsFormatWriteOption) {
        formatWriteOption = (VariantsFormatWriteOption) writeOption;
      } else if (writeOption instanceof FileCardinalityWriteOption) {
        fileCardinalityWriteOption = (FileCardinalityWriteOption) writeOption;
      } else if (writeOption instanceof TempPartsDirectoryWriteOption) {
        tempPartsDirectoryWriteOption = (TempPartsDirectoryWriteOption) writeOption;
      } else if (writeOption instanceof TabixIndexWriteOption
          && writeOption == TabixIndexWriteOption.ENABLE) {
        indexesToEnable.add(TabixIndexWriteOption.getIndexExtension());
      }
    }

    if (formatWriteOption == null) {
      formatWriteOption = VcfFormat.formatWriteOptionFromPath(path);
    }

    if (formatWriteOption == null) {
      throw new IllegalArgumentException(
          "Path does not end in VCF extension, and format not specified.");
    }

    if (fileCardinalityWriteOption == null) {
      fileCardinalityWriteOption = VcfFormat.fileCardinalityWriteOptionFromPath(path);
    }

    String tempPartsDirectory = null;
    if (tempPartsDirectoryWriteOption != null) {
      tempPartsDirectory = tempPartsDirectoryWriteOption.getTempPartsDirectory();
    } else if (fileCardinalityWriteOption == FileCardinalityWriteOption.SINGLE) {
      tempPartsDirectory = path + ".parts";
    }

    fileCardinalityWriteOption
        .getAbstractVcfSink(formatWriteOption)
        .save(
            sparkContext,
            htsjdkVariantsRdd.getHeader(),
            htsjdkVariantsRdd.getVariants(),
            path,
            tempPartsDirectory,
            indexesToEnable);
  }
}
