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
