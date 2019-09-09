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

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SBIIndexWriter;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.NioFileSystemWrapper;
import org.disq_bio.disq.impl.formats.sam.AbstractSamSource;
import org.disq_bio.disq.impl.formats.sam.SamFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The entry point for reading or writing a {@link HtsjdkReadsRdd}. */
public class HtsjdkReadsRddStorage {

  private static final Logger logger = LoggerFactory.getLogger(HtsjdkReadsRddStorage.class);

  private JavaSparkContext sparkContext;
  private int splitSize;
  private ValidationStringency validationStringency = ValidationStringency.DEFAULT_STRINGENCY;
  private boolean useNio;
  private String referenceSourcePath;
  private long sbiIndexGranularity = SBIIndexWriter.DEFAULT_GRANULARITY;

  /**
   * Create a {@link HtsjdkReadsRddStorage} from a Spark context object.
   *
   * @param sparkContext the Spark context to use
   * @return a {@link HtsjdkReadsRddStorage}
   */
  public static HtsjdkReadsRddStorage makeDefault(JavaSparkContext sparkContext) {
    return new HtsjdkReadsRddStorage(sparkContext);
  }

  private HtsjdkReadsRddStorage(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  /**
   * @param splitSize the requested size of file splits in bytes when reading
   * @return the current {@link HtsjdkReadsRddStorage}
   */
  public HtsjdkReadsRddStorage splitSize(int splitSize) {
    this.splitSize = splitSize;
    return this;
  }

  /**
   * @param validationStringency the validation stringency for reading
   * @return the current {@link HtsjdkReadsRddStorage}
   */
  public HtsjdkReadsRddStorage validationStringency(ValidationStringency validationStringency) {
    this.validationStringency = validationStringency;
    return this;
  }

  /**
   * @param useNio whether to use NIO or the Hadoop filesystem (default) for file operations
   * @return the current {@link HtsjdkReadsRddStorage}
   */
  public HtsjdkReadsRddStorage useNio(boolean useNio) {
    this.useNio = useNio;
    return this;
  }

  /**
   * @param referenceSourcePath path to the reference; only required when reading CRAM.
   * @return the current {@link HtsjdkReadsRddStorage}
   */
  public HtsjdkReadsRddStorage referenceSourcePath(String referenceSourcePath) {
    this.referenceSourcePath = referenceSourcePath;
    return this;
  }

  /**
   * @param sbiIndexGranularity the granularity to use when writing SBI index files; only used when
   *     writing single BAM files.
   * @return the current {@link HtsjdkReadsRddStorage}
   */
  public HtsjdkReadsRddStorage sbiIndexGranularity(long sbiIndexGranularity) {
    this.sbiIndexGranularity = sbiIndexGranularity;
    return this;
  }

  /**
   * Read reads from the given path. The input files may be in any format (BAM/CRAM/SAM).
   *
   * @param path the file or directory to read from
   * @return a {@link HtsjdkReadsRdd} that allows access to the reads
   * @throws IOException if an IO error occurs while determining the format of the files and reading
   *     the header
   */
  public HtsjdkReadsRdd read(String path) throws IOException {
    return read(path, null);
  }

  /**
   * Read reads from the given path, using the given traversal parameters to filter the reads. The
   * input files may be in any format (BAM/CRAM/SAM).
   *
   * @param path the file or directory to read from
   * @param traversalParameters parameters that determine which reads should be returned, allows
   *     filtering by interval
   * @param <T> the type of Locatable for specifying intervals
   * @return a {@link HtsjdkReadsRdd} that allows access to the reads
   * @throws IOException if an IO error occurs while determining the format of the files
   */
  public <T extends Locatable> HtsjdkReadsRdd read(
      String path, HtsjdkReadsTraversalParameters<T> traversalParameters) throws IOException {

    FileSystemWrapper fileSystemWrapper =
        useNio ? new NioFileSystemWrapper() : new HadoopFileSystemWrapper();

    String firstSamPath;
    if (fileSystemWrapper.isDirectory(sparkContext.hadoopConfiguration(), path)) {
      firstSamPath =
          fileSystemWrapper.firstFileInDirectory(sparkContext.hadoopConfiguration(), path);
    } else {
      firstSamPath = path;
    }
    SamFormat samFormat = SamFormat.fromPath(firstSamPath);

    if (samFormat == null) {
      throw new IllegalArgumentException("Cannot find format extension for " + path);
    }

    AbstractSamSource abstractSamSource = samFormat.createAbstractSamSource(fileSystemWrapper);

    SAMFileHeader header =
        abstractSamSource.getFileHeader(
            sparkContext, path, validationStringency, referenceSourcePath);
    JavaRDD<SAMRecord> reads =
        abstractSamSource.getReads(
            sparkContext,
            path,
            splitSize,
            traversalParameters,
            validationStringency,
            referenceSourcePath);
    return new HtsjdkReadsRdd(header, reads);
  }

  /**
   * Write reads to a file or files specified by the given path. Write options may be specified to
   * control the format to write in (BAM/CRAM/SAM, if not clear from the path extension), and the
   * number of files to write (single vs. multiple).
   *
   * @param htsjdkReadsRdd a {@link HtsjdkReadsRdd} containing the header and the reads
   * @param path the file or directory to write to
   * @param writeOptions options to control aspects of how to write the reads (e.g. {@link
   *     ReadsFormatWriteOption} and {@link FileCardinalityWriteOption}
   * @throws IOException if an IO error occurs while writing
   */
  public void write(HtsjdkReadsRdd htsjdkReadsRdd, String path, WriteOption... writeOptions)
      throws IOException {
    ReadsFormatWriteOption formatWriteOption = null;
    FileCardinalityWriteOption fileCardinalityWriteOption = null;
    TempPartsDirectoryWriteOption tempPartsDirectoryWriteOption = null;
    List<String> indexesToEnable = new ArrayList<>();
    for (WriteOption writeOption : writeOptions) {
      if (writeOption instanceof ReadsFormatWriteOption) {
        formatWriteOption = (ReadsFormatWriteOption) writeOption;
      } else if (writeOption instanceof FileCardinalityWriteOption) {
        fileCardinalityWriteOption = (FileCardinalityWriteOption) writeOption;
      } else if (writeOption instanceof TempPartsDirectoryWriteOption) {
        tempPartsDirectoryWriteOption = (TempPartsDirectoryWriteOption) writeOption;
      } else if (writeOption instanceof BaiWriteOption && writeOption == BaiWriteOption.ENABLE) {
        indexesToEnable.add(BaiWriteOption.getIndexExtension());
      } else if (writeOption instanceof CraiWriteOption && writeOption == CraiWriteOption.ENABLE) {
        indexesToEnable.add(CraiWriteOption.getIndexExtension());
      } else if (writeOption instanceof SbiWriteOption && writeOption == SbiWriteOption.ENABLE) {
        indexesToEnable.add(SbiWriteOption.getIndexExtension());
      } else {
        logger.warn("Unrecognized write option: {}", writeOption);
      }
    }

    if (formatWriteOption == null) {
      formatWriteOption = SamFormat.formatWriteOptionFromPath(path);
    }

    if (formatWriteOption == null) {
      throw new IllegalArgumentException(
          "Path does not end in BAM, CRAM, or SAM extension, and format not specified.");
    }

    if (fileCardinalityWriteOption == null) {
      fileCardinalityWriteOption = SamFormat.fileCardinalityWriteOptionFromPath(path);
    }

    String tempPartsDirectory = null;
    if (tempPartsDirectoryWriteOption != null) {
      tempPartsDirectory = tempPartsDirectoryWriteOption.getTempPartsDirectory();
    } else if (fileCardinalityWriteOption == FileCardinalityWriteOption.SINGLE) {
      tempPartsDirectory = path + ".parts";
    }

    fileCardinalityWriteOption
        .getAbstractSamSink(formatWriteOption)
        .save(
            sparkContext,
            htsjdkReadsRdd.getHeader(),
            htsjdkReadsRdd.getReads(),
            path,
            referenceSourcePath,
            tempPartsDirectory,
            sbiIndexGranularity,
            indexesToEnable);
  }
}
