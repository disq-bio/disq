package org.disq_bio.disq;

/** An option for configuring which format to write a {@link HtsjdkVariantsRdd} as. */
public enum VariantsFormatWriteOption implements WriteOption {
  /** VCF format */
  VCF,
  /** block compressed VCF format (.vcf.bgz) */
  VCF_BGZ,
  /** block compressed VCF format (.vcf.gz) */
  VCF_GZ
}
