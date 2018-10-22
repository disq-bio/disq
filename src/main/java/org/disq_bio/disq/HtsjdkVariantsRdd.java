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

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.spark.api.java.JavaRDD;

/**
 * A {@link HtsjdkVariantsRdd} is the distributed equivalent of a htsjdk {@link
 * htsjdk.variant.vcf.VCFFileReader}. It represents a VCF file stored in a distributed filesystem,
 * and encapsulates a Spark RDD containing the variant records in it.
 *
 * <p>Use a {@link HtsjdkVariantsRddStorage} to read and write {@link HtsjdkVariantsRdd}s.
 *
 * @see HtsjdkVariantsRddStorage
 */
public class HtsjdkVariantsRdd {

  private final VCFHeader header;
  private final JavaRDD<VariantContext> variants;

  public HtsjdkVariantsRdd(VCFHeader header, JavaRDD<VariantContext> variants) {
    this.header = header;
    this.variants = variants;
  }

  /**
   * @return the header for the variants in this RDD. In the case that different variants have
   *     different headers, it is undefined which header this method returns.
   */
  public VCFHeader getHeader() {
    return header;
  }

  /** @return a RDD of {@link VariantContext} with headers. */
  public JavaRDD<VariantContext> getVariants() {
    return variants;
  }
}
