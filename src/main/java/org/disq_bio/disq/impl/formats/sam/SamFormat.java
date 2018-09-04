package org.disq_bio.disq.impl.formats.sam;

import static org.disq_bio.disq.FileCardinalityWriteOption.MULTIPLE;
import static org.disq_bio.disq.FileCardinalityWriteOption.SINGLE;

import java.util.function.Function;
import org.disq_bio.disq.FileCardinalityWriteOption;
import org.disq_bio.disq.ReadsFormatWriteOption;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.formats.bam.BamSource;
import org.disq_bio.disq.impl.formats.cram.CramSource;

public enum SamFormat {
  BAM(".bam", ".bai", BamSource::new),
  CRAM(".cram", ".crai", CramSource::new),
  SAM(".sam", null, fileSystemWrapper -> new SamSource());

  private final String extension;
  private final String indexExtension;
  private final Function<FileSystemWrapper, AbstractSamSource> sourceProvider;

  SamFormat(
      String extension,
      String indexExtension,
      Function<FileSystemWrapper, AbstractSamSource> sourceProvider) {
    this.extension = extension;
    this.indexExtension = indexExtension;
    this.sourceProvider = sourceProvider;
  }

  public String getExtension() {
    return extension;
  }

  public String getIndexExtension() {
    return indexExtension;
  }

  public AbstractSamSource createAbstractSamSource(FileSystemWrapper fileSystemWrapper) {
    return sourceProvider.apply(fileSystemWrapper);
  }

  public boolean fileMatches(String path) {
    return path.endsWith(extension);
  }

  public ReadsFormatWriteOption toFormatWriteOption() {
    return ReadsFormatWriteOption.valueOf(name()); // one-to-one correspondence between names
  }

  public static SamFormat fromFormatWriteOption(ReadsFormatWriteOption formatWriteOption) {
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

  public static FileCardinalityWriteOption fileCardinalityWriteOptionFromPath(String path) {
    return fromPath(path) == null ? MULTIPLE : SINGLE;
  }

  public static ReadsFormatWriteOption formatWriteOptionFromPath(String path) {
    SamFormat samFormat = fromPath(path);
    return samFormat == null ? null : samFormat.toFormatWriteOption();
  }
}
