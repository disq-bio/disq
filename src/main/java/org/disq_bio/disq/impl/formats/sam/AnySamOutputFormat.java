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
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.formats.cram.CramReferenceSourceBuilder;

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
        String referenceSourcePath)
        throws IOException {
      OutputStream out = file.getFileSystem(conf).create(file);
      SAMFileWriterFactory writerFactory = new SAMFileWriterFactory().setUseAsyncIo(false);
      switch (samFormat) {
        case BAM:
          samFileWriter = writerFactory.makeBAMWriter(header, true, out);
          break;
        case CRAM:
          FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();
          CRAMReferenceSource referenceSource =
              CramReferenceSourceBuilder.build(fileSystemWrapper, conf, referenceSourcePath);
          samFileWriter = new CRAMFileWriter(out, referenceSource, header, null);
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
  private static String referenceSourcePath;

  public static void setHeader(SAMFileHeader samFileHeader) {
    AnySamOutputFormat.header = samFileHeader;
  }

  public static void setSamFormat(SamFormat samFormat) {
    AnySamOutputFormat.samFormat = samFormat;
  }

  public static void setReferenceSourcePath(String referenceSourcePath) {
    AnySamOutputFormat.referenceSourcePath = referenceSourcePath;
  }

  @Override
  public RecordWriter<Void, SAMRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Path file = getDefaultWorkFile(taskAttemptContext, samFormat.getExtension());
    return new AnySamRecordWriter(
        taskAttemptContext.getConfiguration(), file, header, samFormat, referenceSourcePath);
  }
}
