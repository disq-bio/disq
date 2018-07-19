package org.disq_bio.disq.impl.formats.sam;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import htsjdk.samtools.cram.ref.ReferenceSource;
import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.HtsjdkReadsRdd;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.NioFileSystemWrapper;
import org.disq_bio.disq.impl.formats.cram.CramSink;
import scala.Tuple2;

/**
 * Write reads to multiple BAM/CRAM/SAM files in a directory on Spark. This is more efficient than
 * {@link org.disq_bio.disq.impl.formats.bam.BamSink}, {@link CramSink}, and {@link SamSink} since
 * it avoids the cost of merging the headerless files at the end, however multiple files may not be
 * as easy to consume for some external systems.
 *
 * @see org.disq_bio.disq.impl.formats.bam.BamSink
 * @see CramSink
 * @see SamSink
 * @see HtsjdkReadsRdd
 */
public class AnySamSinkMultiple extends AbstractSamSink implements Serializable {

  private SamFormat samFormat;

  public AnySamSinkMultiple(SamFormat samFormat) {
    this.samFormat = samFormat;
  }

  @Override
  public void save(
      JavaSparkContext jsc,
      SAMFileHeader header,
      JavaRDD<SAMRecord> reads,
      String path,
      String referenceSourcePath,
      String tempPartsDirectory)
      throws IOException {

    FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();
    if (fileSystemWrapper.exists(
        jsc.hadoopConfiguration(), path)) { // delete output path if it exists
      fileSystemWrapper.delete(jsc.hadoopConfiguration(), path);
    }

    ReferenceSource referenceSource =
        referenceSourcePath == null
            ? null
            : new ReferenceSource(NioFileSystemWrapper.asPath(referenceSourcePath));
    Broadcast<SAMFileHeader> headerBroadcast = jsc.broadcast(header);
    Broadcast<CRAMReferenceSource> referenceSourceBroadCast = jsc.broadcast(referenceSource);
    reads
        .mapPartitions(
            readIterator -> {
              AnySamOutputFormat.setHeader(headerBroadcast.getValue());
              AnySamOutputFormat.setSamFormat(samFormat);
              AnySamOutputFormat.setReferenceSource(referenceSourceBroadCast.getValue());
              return readIterator;
            })
        .mapToPair(
            (PairFunction<SAMRecord, Void, SAMRecord>) samRecord -> new Tuple2<>(null, samRecord))
        .saveAsNewAPIHadoopFile(
            path, Void.class, SAMRecord.class, AnySamOutputFormat.class, jsc.hadoopConfiguration());
  }
}
