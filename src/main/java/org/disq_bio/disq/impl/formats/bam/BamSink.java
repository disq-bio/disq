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

import htsjdk.samtools.BAMFileWriter;
import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SBIIndex;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.BaiWriteOption;
import org.disq_bio.disq.HtsjdkReadsRdd;
import org.disq_bio.disq.SbiWriteOption;
import org.disq_bio.disq.impl.file.BaiMerger;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.HiddenFileFilter;
import org.disq_bio.disq.impl.file.Merger;
import org.disq_bio.disq.impl.file.SbiMerger;
import org.disq_bio.disq.impl.formats.sam.AbstractSamSink;
import scala.Tuple2;

/**
 * Write reads to a single BAM file on Spark. This is done by writing to multiple headerless BAM
 * files in parallel, then merging the resulting files into a single BAM file.
 *
 * @see BamSource
 * @see HtsjdkReadsRdd
 */
public class BamSink extends AbstractSamSink {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  @Override
  public void save(
      JavaSparkContext jsc,
      SAMFileHeader header,
      JavaRDD<SAMRecord> reads,
      String path,
      String referenceSourcePath,
      String tempPartsDirectory,
      long sbiIndexGranularity,
      List<String> indexesToEnable)
      throws IOException {

    Broadcast<SAMFileHeader> headerBroadcast = jsc.broadcast(header);
    boolean writeSbiFile = indexesToEnable.contains(SbiWriteOption.getIndexExtension());
    boolean writeBaiFile =
        header.getSortOrder() == SAMFileHeader.SortOrder.coordinate
            && indexesToEnable.contains(BaiWriteOption.getIndexExtension());
    Configuration conf = jsc.hadoopConfiguration();
    reads
        .mapPartitions(
            readIterator -> {
              HeaderlessBamOutputFormat.setHeader(headerBroadcast.getValue());
              HeaderlessBamOutputFormat.setWriteSbiFile(writeSbiFile);
              HeaderlessBamOutputFormat.setWriteBaiFile(writeBaiFile);
              HeaderlessBamOutputFormat.setSbiIndexGranularity(sbiIndexGranularity);
              return readIterator;
            })
        .mapToPair(
            (PairFunction<SAMRecord, Void, SAMRecord>) samRecord -> new Tuple2<>(null, samRecord))
        .saveAsNewAPIHadoopFile(
            tempPartsDirectory, Void.class, SAMRecord.class, HeaderlessBamOutputFormat.class, conf);

    String headerFile = tempPartsDirectory + "/header";
    try (OutputStream out = fileSystemWrapper.create(conf, headerFile)) {
      BAMFileWriter.writeHeader(out, header);
    }
    long headerLength = fileSystemWrapper.getFileLength(conf, headerFile);

    String terminatorFile = tempPartsDirectory + "/terminator";
    try (OutputStream out = fileSystemWrapper.create(conf, terminatorFile)) {
      out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
    }

    List<String> bamParts =
        fileSystemWrapper
            .listDirectory(conf, tempPartsDirectory)
            .stream()
            .filter(new HiddenFileFilter())
            .collect(Collectors.toList());
    List<Long> partLengths = new ArrayList<>();
    for (String part : bamParts) {
      partLengths.add(fileSystemWrapper.getFileLength(conf, part));
    }

    new Merger().mergeParts(conf, tempPartsDirectory, path);
    long fileLength = fileSystemWrapper.getFileLength(conf, path);
    if (writeSbiFile) {
      new SbiMerger(fileSystemWrapper)
          .mergeParts(
              conf, tempPartsDirectory, path + SBIIndex.FILE_EXTENSION, headerLength, fileLength);
    }
    if (writeBaiFile) {
      new BaiMerger(fileSystemWrapper)
          .mergeParts(
              conf, tempPartsDirectory, path + BAMIndex.BAMIndexSuffix, header, partLengths);
    }
    fileSystemWrapper.delete(conf, tempPartsDirectory);
  }
}
