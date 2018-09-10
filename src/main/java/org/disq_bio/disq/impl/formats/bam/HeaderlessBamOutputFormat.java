package org.disq_bio.disq.impl.formats.bam;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.disq_bio.disq.HtsjdkReadsRdd;

/**
 * An output format for writing {@link SAMRecord} objects to BAM files that don't have a header (or
 * terminator), so they can be merged into a single file easily. Files do not have the usual ".bam"
 * extension since they are not complete BAM files. This class should not be used directly.
 *
 * @see HtsjdkReadsRdd
 */
public class HeaderlessBamOutputFormat extends FileOutputFormat<Void, SAMRecord> {

  static class BamRecordWriter extends RecordWriter<Void, SAMRecord> {

    private final OutputStream out;
    private final BinaryCodec binaryCodec;
    private final BAMRecordCodec bamRecordCodec;

    public BamRecordWriter(Configuration conf, Path file, SAMFileHeader header) throws IOException {
      this.out = file.getFileSystem(conf).create(file);
      BlockCompressedOutputStream compressedOut = new BlockCompressedOutputStream(out, (File) null);
      binaryCodec = new BinaryCodec(compressedOut);
      bamRecordCodec = new BAMRecordCodec(header);
      bamRecordCodec.setOutputStream(compressedOut);
    }

    @Override
    public void write(Void ignore, SAMRecord samRecord) {
      bamRecordCodec.encode(samRecord);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
      binaryCodec.getOutputStream().flush();
      out.close(); // don't close BlockCompressedOutputStream since we don't want to write the
      // terminator
    }
  }

  private static SAMFileHeader header;

  public static void setHeader(SAMFileHeader samFileHeader) {
    header = samFileHeader;
  }

  @Override
  public RecordWriter<Void, SAMRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Path file = getDefaultWorkFile(taskAttemptContext, "");
    return new BamRecordWriter(taskAttemptContext.getConfiguration(), file, header);
  }
}
