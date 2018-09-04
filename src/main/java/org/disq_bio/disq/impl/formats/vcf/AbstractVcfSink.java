package org.disq_bio.disq.impl.formats.vcf;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class AbstractVcfSink {
  public abstract void save(
      JavaSparkContext jsc,
      VCFHeader vcfHeader,
      JavaRDD<VariantContext> variants,
      String path,
      String tempPartsDirectory)
      throws IOException;
}
