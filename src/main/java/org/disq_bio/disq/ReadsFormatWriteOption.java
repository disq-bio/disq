package org.disq_bio.disq;

import java.util.function.Supplier;
import org.disq_bio.disq.impl.formats.bam.BamSink;
import org.disq_bio.disq.impl.formats.cram.CramSink;
import org.disq_bio.disq.impl.formats.sam.AbstractSamSink;
import org.disq_bio.disq.impl.formats.sam.SamSink;

/** An option for configuring which format to write a {@link HtsjdkReadsRdd} as. */
public enum ReadsFormatWriteOption implements WriteOption {
  /** BAM format */
  BAM(BamSink::new),
  /** CRAM format */
  CRAM(CramSink::new),
  /** SAM format */
  SAM(SamSink::new);

  private final transient Supplier<AbstractSamSink> sinkProvider;

  ReadsFormatWriteOption(Supplier<AbstractSamSink> sinkProvider) {
    this.sinkProvider = sinkProvider;
  }

  AbstractSamSink createAbstractSamSink() {
    return sinkProvider.get();
  }
}
