package org.disq_bio.disq.impl.formats.sam;

import org.disq_bio.disq.HtsjdkReadsRddStorage;

public enum SamFormat {
  BAM(".bam", ".bai"),
  CRAM(".cram", ".crai"),
  SAM(".sam", null);

  private String extension;
  private String indexExtension;

  SamFormat(String extension, String indexExtension) {
    this.extension = extension;
    this.indexExtension = indexExtension;
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

  public HtsjdkReadsRddStorage.FormatWriteOption toFormatWriteOption() {
    return HtsjdkReadsRddStorage.FormatWriteOption.valueOf(
        name()); // one-to-one correspondence between names
  }

  public static SamFormat fromFormatWriteOption(
      HtsjdkReadsRddStorage.FormatWriteOption formatWriteOption) {
    return valueOf(formatWriteOption.name());
  }

  public static SamFormat fromExtension(String extension) {
    for (SamFormat format : values()) {
      if (extension.equals(format.extension)) {
        return format;
      }
    }
    return null;
  }

  public static SamFormat fromPath(String path) {
    for (SamFormat format : values()) {
      if (path.endsWith(format.extension)) {
        return format;
      }
    }
    return null;
  }
}
