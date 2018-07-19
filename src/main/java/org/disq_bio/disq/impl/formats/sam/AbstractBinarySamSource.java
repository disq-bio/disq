package org.disq_bio.disq.impl.formats.sam;

import com.google.common.collect.Iterators;
import htsjdk.samtools.AbstractBAMFileIndex;
import htsjdk.samtools.BAMFileReader;
import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileSpan;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.HtsjdkReadsTraversalParameters;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.PathChunk;
import org.disq_bio.disq.impl.formats.AutocloseIteratorWrapper;
import org.disq_bio.disq.impl.formats.BoundedTraversalUtil;
import org.disq_bio.disq.impl.formats.SerializableHadoopConfiguration;

public abstract class AbstractBinarySamSource extends AbstractSamSource {

  protected AbstractBinarySamSource(FileSystemWrapper fileSystemWrapper) {
    super(fileSystemWrapper);
  }

  /**
   * @return an RDD of reads for a bounded traversal (intervals and whether to return unplaced,
   *     unmapped reads).
   */
  @Override
  public <T extends Locatable> JavaRDD<SAMRecord> getReads(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      ValidationStringency validationStringency,
      String referenceSourcePath)
      throws IOException {
    if (traversalParameters != null
        && traversalParameters.getIntervalsForTraversal() == null
        && !traversalParameters.getTraverseUnplacedUnmapped()) {
      throw new IllegalArgumentException("Traversing mapped reads only is not supported.");
    }

    Broadcast<HtsjdkReadsTraversalParameters<T>> traversalParametersBroadcast =
        traversalParameters == null ? null : jsc.broadcast(traversalParameters);
    SerializableHadoopConfiguration confSer =
        new SerializableHadoopConfiguration(jsc.hadoopConfiguration());

    return getPathChunks(jsc, path, splitSize, validationStringency, referenceSourcePath)
        .mapPartitions(
            (FlatMapFunction<Iterator<PathChunk>, SAMRecord>)
                pathChunks -> {
                  Configuration c = confSer.getConf();
                  if (!pathChunks.hasNext()) {
                    return Collections.emptyIterator();
                  }
                  PathChunk pathChunk = pathChunks.next();
                  if (pathChunks.hasNext()) {
                    throw new IllegalArgumentException(
                        "Should not have more than one path chunk per partition");
                  }
                  String p = pathChunk.getPath();
                  SamReader samReader =
                      createSamReader(c, p, validationStringency, referenceSourcePath);
                  BAMFileSpan splitSpan = new BAMFileSpan(pathChunk.getSpan());
                  HtsjdkReadsTraversalParameters<T> traversal =
                      traversalParametersBroadcast == null
                          ? null
                          : traversalParametersBroadcast.getValue();
                  if (traversal == null) {
                    // no intervals or unplaced, unmapped reads
                    return new AutocloseIteratorWrapper<>(
                        getIterator(samReader, splitSpan), samReader);
                  } else {
                    if (!samReader.hasIndex()) {
                      samReader.close();
                      throw new IllegalArgumentException(
                          "Intervals set but no index file found for " + p);
                    }
                    BAMIndex idx = samReader.indexing().getIndex();
                    long startOfLastLinearBin = idx.getStartOfLastLinearBin();
                    long noCoordinateCount = ((AbstractBAMFileIndex) idx).getNoCoordinateCount();
                    Iterator<SAMRecord> intervalReadsIterator;
                    if (traversal.getIntervalsForTraversal() == null
                        || traversal.getIntervalsForTraversal().isEmpty()) {
                      intervalReadsIterator = Collections.emptyIterator();
                      samReader.close(); // not used from this point on
                    } else {
                      SAMFileHeader header = samReader.getFileHeader();
                      QueryInterval[] queryIntervals =
                          BoundedTraversalUtil.prepareQueryIntervals(
                              traversal.getIntervalsForTraversal(), header.getSequenceDictionary());
                      BAMFileSpan span = BAMFileReader.getFileSpan(queryIntervals, idx);
                      span = (BAMFileSpan) span.removeContentsBefore(splitSpan);
                      span = (BAMFileSpan) span.removeContentsAfter(splitSpan);
                      intervalReadsIterator =
                          new AutocloseIteratorWrapper<>(
                              createIndexIterator(
                                  samReader, queryIntervals, false, span.toCoordinateArray()),
                              samReader);
                    }

                    // add on unplaced unmapped reads if there are any in this range
                    if (traversal.getTraverseUnplacedUnmapped()) {
                      if (startOfLastLinearBin != -1
                          && noCoordinateCount >= getMinUnplacedUnmappedReadsCoordinateCount()) {
                        long unplacedUnmappedStart = startOfLastLinearBin;
                        if (pathChunk.getSpan().getChunkStart() <= unplacedUnmappedStart
                            && unplacedUnmappedStart < pathChunk.getSpan().getChunkEnd()) {
                          SamReader unplacedUnmappedReadsSamReader =
                              createSamReader(c, p, validationStringency, referenceSourcePath);
                          Iterator<SAMRecord> unplacedUnmappedReadsIterator =
                              new AutocloseIteratorWrapper<>(
                                  unplacedUnmappedReadsSamReader.queryUnmapped(),
                                  unplacedUnmappedReadsSamReader);
                          return Iterators.concat(
                              intervalReadsIterator, unplacedUnmappedReadsIterator);
                        }
                      }
                    }
                    return intervalReadsIterator;
                  }
                });
  }

  protected abstract JavaRDD<PathChunk> getPathChunks(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      ValidationStringency validationStringency,
      String referenceSourcePath)
      throws IOException;

  protected abstract CloseableIterator<SAMRecord> getIterator(
      SamReader samReader, SAMFileSpan chunks);

  protected abstract CloseableIterator<SAMRecord> createIndexIterator(
      SamReader samReader, QueryInterval[] intervals, boolean contained, long[] filePointers);

  protected int getMinUnplacedUnmappedReadsCoordinateCount() {
    return 1;
  }
}
