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

import htsjdk.samtools.BAMFileReader;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileSpan;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SBIIndex;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReader.PrimitiveSamReaderToSamReaderAdapter;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.samtools.util.FileExtensions;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.HtsjdkReadsRdd;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.PathChunk;
import org.disq_bio.disq.impl.file.PathSplitSource;
import org.disq_bio.disq.impl.formats.SerializableHadoopConfiguration;
import org.disq_bio.disq.impl.formats.bgzf.BgzfBlockGuesser;
import org.disq_bio.disq.impl.formats.bgzf.BgzfBlockSource;
import org.disq_bio.disq.impl.formats.sam.AbstractBinarySamSource;
import org.disq_bio.disq.impl.formats.sam.SamFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load reads from a BAM file on Spark.
 *
 * @see BamSink
 * @see HtsjdkReadsRdd
 */
public class BamSource extends AbstractBinarySamSource implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(BamSource.class);

  private static final int MAX_READ_SIZE = 10_000_000;

  private final BgzfBlockSource bgzfBlockSource;
  private final PathSplitSource pathSplitSource;

  public BamSource(FileSystemWrapper fileSystemWrapper) {
    super(fileSystemWrapper);
    this.bgzfBlockSource = new BgzfBlockSource(fileSystemWrapper);
    this.pathSplitSource = new PathSplitSource(fileSystemWrapper);
  }

  @Override
  public SamFormat getSamFormat() {
    return SamFormat.BAM;
  }

  @Override
  protected JavaRDD<PathChunk> getPathChunks(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      ValidationStringency stringency,
      String referenceSourcePath)
      throws IOException {

    String sbiPath = path + FileExtensions.SBI;
    if (fileSystemWrapper.exists(jsc.hadoopConfiguration(), sbiPath)) {
      logger.debug("Using SBI file {} for finding splits", sbiPath);
      try (SeekableStream sbiStream = fileSystemWrapper.open(jsc.hadoopConfiguration(), sbiPath)) {
        SBIIndex sbiIndex = SBIIndex.load(sbiStream);
        Broadcast<SBIIndex> sbiIndexBroadcast = jsc.broadcast(sbiIndex);
        pathSplitSource
            .getPathSplits(jsc, path, splitSize)
            .flatMap(
                pathSplit -> {
                  SBIIndex index = sbiIndexBroadcast.getValue();
                  Chunk chunk = index.getChunk(pathSplit.getStart(), pathSplit.getEnd());
                  if (chunk == null) {
                    return Collections.emptyIterator();
                  } else {
                    PathChunk pathChunk = new PathChunk(path, chunk);
                    logger.debug("PathChunk: {}", pathChunk);
                    return Collections.singleton(pathChunk).iterator();
                  }
                });
      }
    }

    logger.debug("Using guessing for finding splits");
    SerializableHadoopConfiguration confSer =
        new SerializableHadoopConfiguration(jsc.hadoopConfiguration());
    return bgzfBlockSource
        .getBgzfBlocks(jsc, path, splitSize)
        .mapPartitionsWithIndex(
            (Function2<Integer, Iterator<BgzfBlockGuesser.BgzfBlock>, Iterator<PathChunk>>)
                (partitionIndex, bgzfBlocks) -> {
                  Configuration conf = confSer.getConf();
                  PathChunk pathChunk =
                      getFirstReadInPartition(conf, bgzfBlocks, stringency, referenceSourcePath);
                  logger.debug("PathChunk for partition {}: {}", partitionIndex, pathChunk);
                  if (pathChunk == null) {
                    return Collections.emptyIterator();
                  }
                  return Collections.singleton(pathChunk).iterator();
                },
            true);
  }

  /**
   * @return the {@link PathChunk} for the partition, or null if there is none (e.g. in the case of
   *     long reads, and/or very small partitions).
   */
  private <T extends Locatable> PathChunk getFirstReadInPartition(
      Configuration conf,
      Iterator<BgzfBlockGuesser.BgzfBlock> bgzfBlocks,
      ValidationStringency stringency,
      String referenceSourcePath)
      throws IOException {
    BamRecordGuesser bamRecordGuesser = null;
    try {
      String partitionPath = null;
      int index = 0; // limit search to MAX_READ_SIZE positions
      while (bgzfBlocks.hasNext()) {
        BgzfBlockGuesser.BgzfBlock block = bgzfBlocks.next();
        if (partitionPath == null) { // assume each partition comes from only a single file path
          partitionPath = block.path;
          try (SamReader samReader =
              createSamReader(conf, partitionPath, stringency, referenceSourcePath)) {
            SAMFileHeader header = samReader.getFileHeader();
            bamRecordGuesser = getBamRecordGuesser(conf, partitionPath, header);
          }
        }
        for (int up = 0; up < block.uSize; up++) {
          index++;
          if (index > MAX_READ_SIZE) {
            return null;
          }
          long vPos = BlockCompressedFilePointerUtil.makeFilePointer(block.pos, up);
          long vEnd = BlockCompressedFilePointerUtil.makeFilePointer(block.end); // end is exclusive
          if (bamRecordGuesser.checkRecordStart(vPos)) {
            block.end();
            return new PathChunk(partitionPath, new Chunk(vPos, vEnd));
          }
        }
      }
    } finally {
      if (bamRecordGuesser != null) {
        bamRecordGuesser.close();
      }
    }
    return null;
  }

  private BamRecordGuesser getBamRecordGuesser(
      Configuration conf, String path, SAMFileHeader header) throws IOException {
    SeekableStream ss = fileSystemWrapper.open(conf, path);
    return new BamRecordGuesser(ss, header.getSequenceDictionary().size(), header);
  }

  private BAMFileReader getUnderlyingBamFileReader(SamReader samReader) {
    BAMFileReader bamFileReader =
        (BAMFileReader) ((PrimitiveSamReaderToSamReaderAdapter) samReader).underlyingReader();
    if (bamFileReader.hasIndex()) {
      bamFileReader
          .getIndex(); // force BAMFileReader#mIndex to be populated so the index stream is properly
      // closed by the close() method
    }
    return bamFileReader;
  }

  @Override
  protected CloseableIterator<SAMRecord> getIterator(SamReader samReader, SAMFileSpan chunks) {
    return getUnderlyingBamFileReader(samReader).getIterator(chunks);
  }

  @Override
  protected CloseableIterator<SAMRecord> createIndexIterator(
      SamReader samReader, QueryInterval[] intervals, boolean contained, long[] filePointers) {
    return getUnderlyingBamFileReader(samReader)
        .createIndexIterator(intervals, contained, filePointers);
  }
}
