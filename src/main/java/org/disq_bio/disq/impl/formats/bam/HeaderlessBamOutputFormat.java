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
