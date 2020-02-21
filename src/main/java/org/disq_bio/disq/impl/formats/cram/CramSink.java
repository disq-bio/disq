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

import htsjdk.samtools.CRAMContainerStreamWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.common.CramVersions;
import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.CraiWriteOption;
import org.disq_bio.disq.HtsjdkReadsRdd;
import org.disq_bio.disq.impl.file.CraiMerger;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.HiddenFileFilter;
import org.disq_bio.disq.impl.file.Merger;
import org.disq_bio.disq.impl.formats.sam.AbstractSamSink;
import scala.Tuple2;

/**
 * Write reads to a single CRAM file on Spark. This is done by writing to multiple headerless CRAM
 * files in parallel, then merging the resulting files into a single CRAM file.
 *
 * @see CramSource
 * @see HtsjdkReadsRdd
 */
public class CramSink extends AbstractSamSink {

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
    boolean writeCraiFile =
        header.getSortOrder() == SAMFileHeader.SortOrder.coordinate
            && indexesToEnable.contains(CraiWriteOption.getIndexExtension());
    Configuration conf = jsc.hadoopConfiguration();
    reads
        .mapPartitions(
            readIterator -> {
              CramOutputFormat.setHeader(headerBroadcast.getValue());
              CramOutputFormat.setReferenceSourcePath(referenceSourcePath);
              CramOutputFormat.setWriteCraiFile(writeCraiFile);
              return readIterator;
            })
        .mapToPair(
            (PairFunction<SAMRecord, Void, SAMRecord>) samRecord -> new Tuple2<>(null, samRecord))
        .saveAsNewAPIHadoopFile(
            tempPartsDirectory, Void.class, SAMRecord.class, CramOutputFormat.class, conf);

    String headerFile = tempPartsDirectory + "/header";
    try (OutputStream out = fileSystemWrapper.create(conf, headerFile)) {
      CRAMReferenceSource referenceSource =
          CramReferenceSourceBuilder.build(fileSystemWrapper, conf, referenceSourcePath);
      writeHeader(header, out, headerFile, referenceSource);
    }

    String terminatorFile = tempPartsDirectory + "/terminator";
    try (OutputStream out = fileSystemWrapper.create(conf, terminatorFile)) {
      CramIO.writeCRAMEOF(CramVersions.DEFAULT_CRAM_VERSION, out);
    }

    List<FileSystemWrapper.FileStatus> cramParts =
        fileSystemWrapper.listDirectoryStatus(conf, tempPartsDirectory).stream()
            .filter(fs -> new HiddenFileFilter().test(fs.getPath()))
            .collect(Collectors.toList());
    List<Long> partLengths =
        cramParts.stream()
            .mapToLong(FileSystemWrapper.FileStatus::getLength)
            .boxed()
            .collect(Collectors.toList());

    new Merger(fileSystemWrapper).mergeParts(conf, cramParts, path);
    if (writeCraiFile) {
      long fileLength = fileSystemWrapper.getFileLength(conf, path);
      new CraiMerger(fileSystemWrapper)
          .mergeParts(
              conf,
              tempPartsDirectory,
              path + CraiWriteOption.getIndexExtension(),
              header,
              partLengths,
              fileLength);
    }
    fileSystemWrapper.delete(conf, tempPartsDirectory);
  }

  private void writeHeader(
      SAMFileHeader header,
      OutputStream out,
      String headerFile,
      CRAMReferenceSource referenceSource) {
    CRAMContainerStreamWriter cramWriter =
        new CRAMContainerStreamWriter(out, null, referenceSource, header, headerFile);
    cramWriter.writeHeader(header);
    cramWriter.finish(false); // don't write terminator
  }
}
