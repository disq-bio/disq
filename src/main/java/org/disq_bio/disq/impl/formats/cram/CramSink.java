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
import htsjdk.samtools.cram.ref.ReferenceSource;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.HtsjdkReadsRdd;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.Merger;
import org.disq_bio.disq.impl.file.NioFileSystemWrapper;
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
    reads
        .mapPartitions(
            readIterator -> {
              CramOutputFormat.setHeader(headerBroadcast.getValue());

              // Don't broadcast the reference source since it is not always serializable,
              // even using Kryo (e.g. for GCS)
              ReferenceSource referenceSource =
                  new ReferenceSource(NioFileSystemWrapper.asPath(referenceSourcePath));
              CramOutputFormat.setReferenceSource(referenceSource);
              return readIterator;
            })
        .mapToPair(
            (PairFunction<SAMRecord, Void, SAMRecord>) samRecord -> new Tuple2<>(null, samRecord))
        .saveAsNewAPIHadoopFile(
            tempPartsDirectory,
            Void.class,
            SAMRecord.class,
            CramOutputFormat.class,
            jsc.hadoopConfiguration());

    String headerFile = tempPartsDirectory + "/header";
    try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), headerFile)) {
      ReferenceSource referenceSource =
          new ReferenceSource(NioFileSystemWrapper.asPath(referenceSourcePath));
      writeHeader(header, out, headerFile, referenceSource);
    }

    String terminatorFile = tempPartsDirectory + "/terminator";
    try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), terminatorFile)) {
      CramIO.issueEOF(CramVersions.DEFAULT_CRAM_VERSION, out);
    }

    new Merger().mergeParts(jsc.hadoopConfiguration(), tempPartsDirectory, path);
    fileSystemWrapper.delete(jsc.hadoopConfiguration(), tempPartsDirectory);
  }

  private void writeHeader(
      SAMFileHeader header, OutputStream out, String headerFile, ReferenceSource referenceSource) {
    CRAMContainerStreamWriter cramWriter =
        new CRAMContainerStreamWriter(out, null, referenceSource, header, headerFile);
    cramWriter.writeHeader(header);
    cramWriter.finish(false); // don't write terminator
  }
}
