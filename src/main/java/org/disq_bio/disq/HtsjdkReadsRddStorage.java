package org.disq_bio.disq;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.NioFileSystemWrapper;
import org.disq_bio.disq.impl.formats.bam.BamSink;
import org.disq_bio.disq.impl.formats.bam.BamSource;
import org.disq_bio.disq.impl.formats.cram.CramSink;
import org.disq_bio.disq.impl.formats.cram.CramSource;
import org.disq_bio.disq.impl.formats.sam.AbstractSamSink;
import org.disq_bio.disq.impl.formats.sam.AbstractSamSource;
import org.disq_bio.disq.impl.formats.sam.AnySamSinkMultiple;
import org.disq_bio.disq.impl.formats.sam.SamFormat;
import org.disq_bio.disq.impl.formats.sam.SamSink;
import org.disq_bio.disq.impl.formats.sam.SamSource;

/** The entry point for reading or writing a {@link HtsjdkReadsRdd}. */
public class HtsjdkReadsRddStorage {

  /** An option for configuring how to write a {@link HtsjdkReadsRdd}. */
  public interface WriteOption {}

  /** An option for configuring which format to write a {@link HtsjdkReadsRdd} as. */
  public enum FormatWriteOption implements WriteOption {
    /** BAM format */
    BAM,
    /** CRAM format */
    CRAM,
    /** SAM format */
    SAM;
  }

  /** An option for configuring the number of files to write a {@link HtsjdkReadsRdd} as. */
  public enum FileCardinalityWriteOption implements WriteOption {
    /** Write a single file specified by the path. */
    SINGLE,
    /** Write multiple files in a directory specified by the path. */
    MULTIPLE
  }

  /**
   * An option for controlling which directory to write temporary part files to when writing a
   * {@link HtsjdkReadsRdd} as a single file.
   */
  public static class TempPartsDirectoryWriteOption implements WriteOption {
    private String tempPartsDirectory;

    public TempPartsDirectoryWriteOption(String tempPartsDirectory) {
      this.tempPartsDirectory = tempPartsDirectory;
    }

    String getTempPartsDirectory() {
      return tempPartsDirectory;
    }
  }

  private JavaSparkContext sparkContext;
  private int splitSize;
  private ValidationStringency validationStringency = ValidationStringency.DEFAULT_STRINGENCY;
  private boolean useNio;
  private String referenceSourcePath;

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
   * @param splitSize the requested size of file splits when reading
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

    AbstractSamSource abstractSamSource;
    switch (samFormat) {
      case BAM:
        abstractSamSource = new BamSource(useNio);
        break;
      case CRAM:
        abstractSamSource = new CramSource(useNio);
        break;
      case SAM:
        abstractSamSource = new SamSource();
        break;
      default:
        throw new IllegalArgumentException("File does not end in BAM, CRAM, or SAM extension.");
    }

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
   *     FormatWriteOption} and {@link FileCardinalityWriteOption}
   * @throws IOException if an IO error occurs while writing
   */
  public void write(HtsjdkReadsRdd htsjdkReadsRdd, String path, WriteOption... writeOptions)
      throws IOException {
    FormatWriteOption formatWriteOption = null;
    FileCardinalityWriteOption fileCardinalityWriteOption = null;
    TempPartsDirectoryWriteOption tempPartsDirectoryWriteOption = null;
    for (WriteOption writeOption : writeOptions) {
      if (writeOption instanceof FormatWriteOption) {
        formatWriteOption = (FormatWriteOption) writeOption;
      } else if (writeOption instanceof FileCardinalityWriteOption) {
        fileCardinalityWriteOption = (FileCardinalityWriteOption) writeOption;
      } else if (writeOption instanceof TempPartsDirectoryWriteOption) {
        tempPartsDirectoryWriteOption = (TempPartsDirectoryWriteOption) writeOption;
      }
    }

    if (formatWriteOption == null) {
      formatWriteOption = inferFormatFromPath(path);
    }

    if (formatWriteOption == null) {
      throw new IllegalArgumentException(
          "Path does not end in BAM, CRAM, or SAM extension, and format not specified.");
    }

    if (fileCardinalityWriteOption == null) {
      fileCardinalityWriteOption = inferCardinalityFromPath(path);
    }

    String tempPartsDirectory = null;
    if (tempPartsDirectoryWriteOption != null) {
      tempPartsDirectory = tempPartsDirectoryWriteOption.getTempPartsDirectory();
    } else if (fileCardinalityWriteOption == FileCardinalityWriteOption.SINGLE) {
      tempPartsDirectory = path + ".parts";
    }

    getSink(formatWriteOption, fileCardinalityWriteOption)
        .save(
            sparkContext,
            htsjdkReadsRdd.getHeader(),
            htsjdkReadsRdd.getReads(),
            path,
            referenceSourcePath,
            tempPartsDirectory);
  }

  private FormatWriteOption inferFormatFromPath(String path) {
    SamFormat samFormat = SamFormat.fromPath(path);
    return samFormat == null ? null : samFormat.toFormatWriteOption();
  }

  private FileCardinalityWriteOption inferCardinalityFromPath(String path) {
    SamFormat samFormat = SamFormat.fromPath(path);
    return samFormat == null
        ? FileCardinalityWriteOption.MULTIPLE
        : FileCardinalityWriteOption.SINGLE;
  }

  private AbstractSamSink getSink(
      FormatWriteOption formatWriteOption, FileCardinalityWriteOption fileCardinalityWriteOption) {
    switch (fileCardinalityWriteOption) {
      case SINGLE:
        switch (formatWriteOption) {
          case BAM:
            return new BamSink();
          case CRAM:
            return new CramSink();
          case SAM:
            return new SamSink();
          default:
            throw new IllegalArgumentException("Unrecognized format: " + formatWriteOption);
        }
      case MULTIPLE:
        return new AnySamSinkMultiple(SamFormat.fromFormatWriteOption(formatWriteOption));
      default:
        throw new IllegalArgumentException(
            "Unrecognized cardinality: " + fileCardinalityWriteOption);
    }
  }
}
