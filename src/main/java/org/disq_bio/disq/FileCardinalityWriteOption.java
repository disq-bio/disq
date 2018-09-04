package org.disq_bio.disq;

import java.util.function.Function;
import org.disq_bio.disq.impl.formats.sam.AbstractSamSink;
import org.disq_bio.disq.impl.formats.sam.AnySamSinkMultiple;
import org.disq_bio.disq.impl.formats.sam.SamFormat;
import org.disq_bio.disq.impl.formats.vcf.AbstractVcfSink;
import org.disq_bio.disq.impl.formats.vcf.VcfFormat;
import org.disq_bio.disq.impl.formats.vcf.VcfSink;
import org.disq_bio.disq.impl.formats.vcf.VcfSinkMultiple;

/** An option for configuring whether to write output in a single file, or multiple files. */
public enum FileCardinalityWriteOption implements WriteOption {
  /** Write a single file specified by the path. */
  SINGLE(ReadsFormatWriteOption::createAbstractSamSink, variantsFormatWriteOption -> new VcfSink()),
  /** Write multiple files in a directory specified by the path. */
  MULTIPLE(
      readsFormatWriteOption ->
          new AnySamSinkMultiple(SamFormat.fromFormatWriteOption(readsFormatWriteOption)),
      variantsFormatWriteOption ->
          new VcfSinkMultiple(VcfFormat.fromFormatWriteOption(variantsFormatWriteOption)));

  private final transient Function<ReadsFormatWriteOption, AbstractSamSink> samSinkProvider;
  private final transient Function<VariantsFormatWriteOption, AbstractVcfSink> vcfSinkProvider;

  FileCardinalityWriteOption(
      Function<ReadsFormatWriteOption, AbstractSamSink> samSinkProvider,
      Function<VariantsFormatWriteOption, AbstractVcfSink> vcfSinkProvider) {
    this.samSinkProvider = samSinkProvider;
    this.vcfSinkProvider = vcfSinkProvider;
  }

  AbstractSamSink getAbstractSamSink(ReadsFormatWriteOption readsFormatWriteOption) {
    return samSinkProvider.apply(readsFormatWriteOption);
  }

  AbstractVcfSink getAbstractVcfSink(VariantsFormatWriteOption variantsFormatWriteOption) {
    return vcfSinkProvider.apply(variantsFormatWriteOption);
  }
}
