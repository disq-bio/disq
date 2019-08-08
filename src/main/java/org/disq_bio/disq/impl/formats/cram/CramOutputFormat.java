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
package org.disq_bio.disq.impl.formats.cram;

import htsjdk.samtools.CRAMCRAIIndexer;
import htsjdk.samtools.CRAMContainerStreamWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.disq_bio.disq.CraiWriteOption;
import org.disq_bio.disq.HtsjdkReadsRdd;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;

/**
 * An output format for writing {@link SAMRecord} objects to CRAM files. Should not be used
 * directly.
 *
 * @see HtsjdkReadsRdd
 */
public class CramOutputFormat extends FileOutputFormat<Void, SAMRecord> {

  private static SAMFileHeader header;
  private static String referenceSourcePath;
  private static boolean writeCraiFile;

  public static void setHeader(SAMFileHeader samFileHeader) {
    header = samFileHeader;
  }

  public static void setReferenceSourcePath(String referenceSourcePath) {
    CramOutputFormat.referenceSourcePath = referenceSourcePath;
  }

  public static void setWriteCraiFile(boolean writeCraiFile) {
    CramOutputFormat.writeCraiFile = writeCraiFile;
  }

  @Override
  public RecordWriter<Void, SAMRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Path file = getDefaultWorkFile(taskAttemptContext, "");
    Path craiFile;
    if (writeCraiFile) {
      // ensure CRAI files are hidden so they don't interfere with merging of part files
      craiFile =
          new Path(file.getParent(), "." + file.getName() + CraiWriteOption.getIndexExtension());
    } else {
      craiFile = null;
    }
    return new CramRecordWriter(
        taskAttemptContext.getConfiguration(), file, header, referenceSourcePath, craiFile);
  }

  static class CramRecordWriter extends RecordWriter<Void, SAMRecord> {

    private final OutputStream out;
    private final CRAMContainerStreamWriter cramWriter;

    public CramRecordWriter(
        Configuration conf,
        Path file,
        SAMFileHeader header,
        String referenceSourcePath,
        Path craiFile)
        throws IOException {
      this.out = file.getFileSystem(conf).create(file);
      FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();
      CRAMReferenceSource referenceSource =
          CramReferenceSourceBuilder.build(fileSystemWrapper, conf, referenceSourcePath);
      OutputStream indexStream;
      if (craiFile != null) {
        indexStream = craiFile.getFileSystem(conf).create(craiFile);
      } else {
        indexStream = null;
      }
      cramWriter =
          new CRAMContainerStreamWriter(
              out,
              referenceSource,
              header,
              file.toString(),
              indexStream == null ? null : new CRAMCRAIIndexer(indexStream, header));
    }

    @Override
    public void write(Void ignore, SAMRecord samRecord) {
      cramWriter.writeAlignment(samRecord);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
      cramWriter.finish(false); // don't write terminator
      out.close();
    }
  }
}
