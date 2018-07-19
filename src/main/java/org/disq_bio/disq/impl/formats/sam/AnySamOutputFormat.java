package org.disq_bio.disq.impl.formats.sam;

import htsjdk.samtools.CRAMFileWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.disq_bio.disq.HtsjdkReadsRdd;

/**
 * An output format for writing {@link SAMRecord} objects to BAM/CRAM/SAM files (including header
 * and terminator, where appropriate). This class should not be used directly.
 *
 * @see HtsjdkReadsRdd
 */
public class AnySamOutputFormat extends FileOutputFormat<Void, SAMRecord> {

  static class AnySamRecordWriter extends RecordWriter<Void, SAMRecord> {

    private final SAMFileWriter samFileWriter;

    public AnySamRecordWriter(
        Configuration conf,
        Path file,
        SAMFileHeader header,
        SamFormat samFormat,
        CRAMReferenceSource refSource)
        throws IOException {
      OutputStream out = file.getFileSystem(conf).create(file);
      SAMFileWriterFactory writerFactory = new SAMFileWriterFactory().setUseAsyncIo(false);
      switch (samFormat) {
        case BAM:
          samFileWriter = writerFactory.makeBAMWriter(header, true, out);
          break;
        case CRAM:
          samFileWriter = new CRAMFileWriter(out, refSource, header, null);
          break;
        case SAM:
          samFileWriter = writerFactory.makeSAMWriter(header, true, out);
          break;
        default:
          throw new IllegalArgumentException("Unrecognized format: " + samFormat);
      }
    }

    @Override
    public void write(Void ignore, SAMRecord samRecord) {
      samFileWriter.addAlignment(samRecord);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
      samFileWriter.close();
    }
  }

  private static SAMFileHeader header;
  private static SamFormat samFormat;
  private static CRAMReferenceSource refSource;

  public static void setHeader(SAMFileHeader samFileHeader) {
    AnySamOutputFormat.header = samFileHeader;
  }

  public static void setSamFormat(SamFormat samFormat) {
    AnySamOutputFormat.samFormat = samFormat;
  }

  public static void setReferenceSource(CRAMReferenceSource referenceSource) {
    AnySamOutputFormat.refSource = referenceSource;
  }

  @Override
  public RecordWriter<Void, SAMRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Path file = getDefaultWorkFile(taskAttemptContext, samFormat.getExtension());
    return new AnySamRecordWriter(
        taskAttemptContext.getConfiguration(), file, header, samFormat, refSource);
  }
}
