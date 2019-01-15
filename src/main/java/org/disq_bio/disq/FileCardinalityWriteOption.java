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
