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
package org.disq_bio.disq.impl.formats.vcf;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

public class VcfSinkMultiple extends AbstractVcfSink implements Serializable {

  private String extension;

  public VcfSinkMultiple(VcfFormat vcfFormat) {
    this.extension = vcfFormat.getExtension();
  }

  @Override
  public void save(
      JavaSparkContext jsc,
      VCFHeader vcfHeader,
      JavaRDD<VariantContext> variants,
      String path,
      String tempPartsDirectory) {
    Broadcast<VCFHeader> headerBroadcast = jsc.broadcast(vcfHeader);
    variants
        .mapPartitions(
            readIterator -> {
              VcfOutputFormat.setHeader(headerBroadcast.getValue());
              VcfOutputFormat.setExtension(extension);
              return readIterator;
            })
        .mapToPair(
            (PairFunction<VariantContext, Void, VariantContext>)
                variantContext -> new Tuple2<>(null, variantContext))
        .saveAsNewAPIHadoopFile(
            path,
            Void.class,
            VariantContext.class,
            VcfOutputFormat.class,
            jsc.hadoopConfiguration());
  }
}
