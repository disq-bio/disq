package org.disq_bio.disq.impl.formats.vcf;

import static org.disq_bio.disq.FileCardinalityWriteOption.MULTIPLE;
import static org.disq_bio.disq.FileCardinalityWriteOption.SINGLE;

import org.disq_bio.disq.FileCardinalityWriteOption;
import org.disq_bio.disq.VariantsFormatWriteOption;

public enum VcfFormat {
  VCF(".vcf", ".idx", false),
  VCF_BGZ(".vcf.bgz", ".tbi", true),
  VCF_GZ(".vcf.gz", ".tbi", true);

  private String extension;
  private String indexExtension;
  private boolean compressed;

  VcfFormat(String extension, String indexExtension, boolean compressed) {
    this.extension = extension;
    this.indexExtension = indexExtension;
    this.compressed = compressed;
  }

  public String getExtension() {
    return extension;
  }

  public String getIndexExtension() {
    return indexExtension;
  }

  public boolean fileMatches(String path) {
    return path.endsWith(extension);
  }

  public boolean isCompressed() {
    return compressed;
  }

  public VariantsFormatWriteOption toFormatWriteOption() {
    return VariantsFormatWriteOption.valueOf(name()); // one-to-one correspondence between names
  }

  public static VcfFormat fromFormatWriteOption(VariantsFormatWriteOption formatWriteOption) {
    return valueOf(formatWriteOption.name());
  }

  public static VcfFormat fromExtension(String extension) {
    for (VcfFormat format : values()) {
      if (extension.equals(format.extension)) {
        return format;
      }
    }
    return null;
  }

  public static VcfFormat fromPath(String path) {
    for (VcfFormat format : values()) {
      if (path.endsWith(format.extension)) {
        return format;
      }
    }
    return null;
  }

  public static FileCardinalityWriteOption fileCardinalityWriteOptionFromPath(String path) {
    return fromPath(path) == null ? MULTIPLE : SINGLE;
  }

  public static VariantsFormatWriteOption formatWriteOptionFromPath(String path) {
    VcfFormat vcfFormat = fromPath(path);
    return vcfFormat == null ? null : vcfFormat.toFormatWriteOption();
  }
}
