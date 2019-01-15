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
package org.disq_bio.disq.impl.formats.bam;

import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.BAMIndexer2;
import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileSource;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordHelper;
import htsjdk.samtools.SBIIndex;
import htsjdk.samtools.SBIIndexWriter;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.disq_bio.disq.HtsjdkReadsRdd;
import org.disq_bio.disq.impl.formats.bgzf.TerminatorlessBlockCompressedOutputStream;

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
    private final BAMIndexer2 bamIndexer;

    private SAMRecord previousSamRecord;
    private Chunk previousSamRecordChunk;

    public BamRecordWriter(
        Configuration conf,
        Path file,
        SAMFileHeader header,
        Path sbiFile,
        Path baiFile,
        long sbiIndexGranularity)
        throws IOException {
      this.conf = conf;
      this.file = file;
      this.out = file.getFileSystem(conf).create(file);
      this.compressedOut = new TerminatorlessBlockCompressedOutputStream(out, null);
      this.binaryCodec = new BinaryCodec(compressedOut);
      this.bamRecordCodec = new BAMRecordCodec(header);
      bamRecordCodec.setOutputStream(compressedOut);
      if (sbiFile != null) {
        this.sbiIndexWriter =
            new SBIIndexWriter(sbiFile.getFileSystem(conf).create(sbiFile), sbiIndexGranularity);
      } else {
        this.sbiIndexWriter = null;
      }
      if (baiFile != null) {
        this.bamIndexer = new BAMIndexer2(baiFile.getFileSystem(conf).create(baiFile), header);
      } else {
        this.bamIndexer = null;
      }
    }

    @Override
    public void write(Void ignore, SAMRecord samRecord) {
      if (bamIndexer != null && previousSamRecord != null) {
        // index the previous record since we know it's not the last one (which needs special
        // handling, see the close method)
        SAMRecordHelper.setFileSource(
            previousSamRecord, new SAMFileSource(null, new BAMFileSpan(previousSamRecordChunk)));
        bamIndexer.processAlignment(previousSamRecord);
      }
      final long startOffset = compressedOut.getFilePointer();
      if (sbiIndexWriter != null) {
        sbiIndexWriter.processRecord(compressedOut.getFilePointer());
      }
      bamRecordCodec.encode(samRecord);
      final long stopOffset = compressedOut.getFilePointer();
      previousSamRecord = samRecord;
      previousSamRecordChunk = new Chunk(startOffset, stopOffset);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
      binaryCodec.close();

      long finalVirtualOffset = compressedOut.getFilePointer();

      long dataFileLength = file.getFileSystem(conf).getFileStatus(file).getLen();
      if (sbiIndexWriter != null) {
        sbiIndexWriter.finish(finalVirtualOffset, dataFileLength);
      }

      // Adjust the end of the chunk in previousSamRecordChunk so that it is a valid virtual offset
      // the flush operation (above) forces the final block to be written out, and makes sure
      // that finalVirtualOffset has an uncompressed offset of 0, which is always valid even after
      // concatenating BGZF files and shifting their virtual offsets.
      // If we didn't do this then we would have an invalid virtual file pointer if a BGZF file
      // were concatenated following this one.
      if (bamIndexer != null) {
        if (previousSamRecord != null) {
          previousSamRecordChunk =
              new Chunk(previousSamRecordChunk.getChunkStart(), finalVirtualOffset);
          SAMRecordHelper.setFileSource(
              previousSamRecord, new SAMFileSource(null, new BAMFileSpan(previousSamRecordChunk)));
          bamIndexer.processAlignment(previousSamRecord);
        }
        bamIndexer.finish();
      }
    }
  }

  private static SAMFileHeader header;
  private static boolean writeSbiFile;
  private static boolean writeBaiFile;
  private static long sbiIndexGranularity;

  public static void setHeader(SAMFileHeader samFileHeader) {
    header = samFileHeader;
  }

  public static void setWriteSbiFile(boolean writeSbiFile) {
    HeaderlessBamOutputFormat.writeSbiFile = writeSbiFile;
  }

  public static void setWriteBaiFile(boolean writeBaiFile) {
    HeaderlessBamOutputFormat.writeBaiFile = writeBaiFile;
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
    Path baiFile;
    if (writeBaiFile) {
      baiFile = new Path(file.getParent(), "." + file.getName() + BAMIndex.BAMIndexSuffix);
    } else {
      baiFile = null;
    }
    return new BamRecordWriter(
        taskAttemptContext.getConfiguration(), file, header, sbiFile, baiFile, sbiIndexGranularity);
  }
}
