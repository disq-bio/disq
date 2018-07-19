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
