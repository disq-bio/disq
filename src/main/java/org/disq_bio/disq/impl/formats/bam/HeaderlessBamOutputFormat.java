/*
 * Disq
 *
 * MIT License
 *
 * Copyright (c) 2018 Disq contributors
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
package org.disq_bio.disq.impl.formats.bam;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SBIIndex;
import htsjdk.samtools.SBIIndexWriter;
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

    private final Configuration conf;
    private final Path file;
    private final OutputStream out;
    private final BlockCompressedOutputStream compressedOut;
    private final BinaryCodec binaryCodec;
    private final BAMRecordCodec bamRecordCodec;
    private final SBIIndexWriter sbiIndexWriter;

    public BamRecordWriter(
        Configuration conf, Path file, SAMFileHeader header, Path sbiFile, long sbiIndexGranularity)
        throws IOException {
      this.conf = conf;
      this.file = file;
      this.out = file.getFileSystem(conf).create(file);
      this.compressedOut = new BlockCompressedOutputStream(out, (File) null);
      this.binaryCodec = new BinaryCodec(compressedOut);
      this.bamRecordCodec = new BAMRecordCodec(header);
      bamRecordCodec.setOutputStream(compressedOut);
      if (sbiFile != null) {
        this.sbiIndexWriter =
            new SBIIndexWriter(sbiFile.getFileSystem(conf).create(sbiFile), sbiIndexGranularity);
      } else {
        this.sbiIndexWriter = null;
      }
    }

    @Override
    public void write(Void ignore, SAMRecord samRecord) {
      if (sbiIndexWriter != null) {
        sbiIndexWriter.processRecord(compressedOut.getFilePointer());
      }
      bamRecordCodec.encode(samRecord);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
      binaryCodec.getOutputStream().flush();
      // don't close BlockCompressedOutputStream since we don't want to write the
      // terminator
      out.close();
      long finalVirtualOffset = compressedOut.getFilePointer();
      long dataFileLength = file.getFileSystem(conf).getFileStatus(file).getLen();
      if (sbiIndexWriter != null) {
        sbiIndexWriter.finish(finalVirtualOffset, dataFileLength);
      }
    }
  }

  private static SAMFileHeader header;
  private static boolean writeSbiFile;
  private static long sbiIndexGranularity;

  public static void setHeader(SAMFileHeader samFileHeader) {
    header = samFileHeader;
  }

  public static void setWriteSbiFile(boolean writeSbiFile) {
    HeaderlessBamOutputFormat.writeSbiFile = writeSbiFile;
  }

  public static void setSbiIndexGranularity(long sbiIndexGranularity) {
    HeaderlessBamOutputFormat.sbiIndexGranularity = sbiIndexGranularity;
  }

  @Override
  public RecordWriter<Void, SAMRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Path file = getDefaultWorkFile(taskAttemptContext, "");
    Path sbiFile;
    if (writeSbiFile) {
      // ensure sbi files are hidden so they don't interfere with merging of part files
      sbiFile = new Path(file.getParent(), "." + file.getName() + SBIIndex.FILE_EXTENSION);
    } else {
      sbiFile = null;
    }
    return new BamRecordWriter(
        taskAttemptContext.getConfiguration(), file, header, sbiFile, sbiIndexGranularity);
  }
}
