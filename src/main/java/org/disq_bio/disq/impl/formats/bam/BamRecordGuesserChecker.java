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

import htsjdk.samtools.BAMSBIIndexer;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SBIIndex;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.HtsjdkReadsRddStorage;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.NioFileSystemWrapper;
import org.disq_bio.disq.impl.formats.SerializableHadoopConfiguration;
import org.disq_bio.disq.impl.formats.bgzf.BgzfBlockGuesser;
import org.disq_bio.disq.impl.formats.bgzf.BgzfBlockSource;
import scala.Tuple2;

/**
 * Uses an SBI index as a source of ground truth for read start positions, in order to test that
 * {@link BamRecordGuesser} is working correctly. For an index with a granularity of 1, all
 * positions can be checked to see if read starts are correctly detected, otherwise only those
 * positions in the index can be checked for false negatives (i.e. if the checker thinks they are
 * not record starts).
 */
public class BamRecordGuesserChecker implements Serializable {

  public enum RecordStartResult {
    FALSE_POSITIVE,
    FALSE_NEGATIVE
  }

  private final FileSystemWrapper fileSystemWrapper;

  public BamRecordGuesserChecker(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

  public BamRecordGuesserChecker(boolean useNio) {
    this(useNio ? new NioFileSystemWrapper() : new HadoopFileSystemWrapper());
  }

  SBIIndex getSBIIndex(Configuration conf, String bamFile) throws IOException {
    String sbiFile = bamFile + SBIIndex.FILE_EXTENSION;
    if (!fileSystemWrapper.exists(conf, sbiFile)) {
      // create SBI file
      try (SeekableStream in = fileSystemWrapper.open(conf, bamFile);
          OutputStream out = fileSystemWrapper.create(conf, sbiFile)) {
        BAMSBIIndexer.createIndex(in, out, 1);
      }
    }
    try (SeekableStream in = fileSystemWrapper.open(conf, sbiFile)) {
      return SBIIndex.load(in);
    }
  }

  /**
   * Check all positions in the BAM file specified by the path, using an SBI index for the BAM
   * (which is generated if it doesn't exist).
   *
   * @param jsc Spark context
   * @param path path
   * @param splitSize split size
   * @return a pair RDD where the key is the virtual file offset, and the value indicates if the
   *     guess was a false positive or a false negative
   * @throws IOException if an I/O error occurs
   */
  public JavaPairRDD<Long, RecordStartResult> check(
      JavaSparkContext jsc, String path, int splitSize) throws IOException {
    return check(jsc, path, splitSize, getSBIIndex(jsc.hadoopConfiguration(), path));
  }

  /**
   * Check all positions in the BAM file specified by the path, using the given SBI index.
   *
   * @param jsc Spark context
   * @param path path
   * @param splitSize split size
   * @param sbiIndex SBI index
   * @return a pair RDD where the key is the virtual file offset, and the value indicates if the
   *     guess was a false positive or a false negative
   * @throws IOException if an I/O error occurs
   */
  public JavaPairRDD<Long, RecordStartResult> check(
      JavaSparkContext jsc, String path, int splitSize, SBIIndex sbiIndex) throws IOException {
    SAMFileHeader header = HtsjdkReadsRddStorage.makeDefault(jsc).read(path).getHeader();

    Broadcast<SBIIndex> indexBroadcast = jsc.broadcast(sbiIndex);

    SerializableHadoopConfiguration confSer =
        new SerializableHadoopConfiguration(jsc.hadoopConfiguration());
    return new BgzfBlockSource(fileSystemWrapper)
        .getBgzfBlocks(jsc, path, splitSize)
        .mapPartitionsToPair(
            (PairFlatMapFunction<Iterator<BgzfBlockGuesser.BgzfBlock>, Long, RecordStartResult>)
                bgzfBlockIterator -> {
                  SBIIndex index = indexBroadcast.getValue();
                  long granularity = index.getGranularity();
                  long[] virtualOffsets = index.getVirtualOffsets();
                  try (SeekableStream in = fileSystemWrapper.open(confSer.getConf(), path)) {
                    BamRecordGuesser bamRecordGuesser =
                        new BamRecordGuesser(in, header.getSequenceDictionary().size(), header);
                    List<Tuple2<Long, RecordStartResult>> mismatches = new ArrayList<>();
                    while (bgzfBlockIterator.hasNext()) {
                      BgzfBlockGuesser.BgzfBlock block = bgzfBlockIterator.next();
                      if (granularity == 1) {
                        // every offset is recorded, so we can detect false positives too
                        for (int up = 0; up < block.uSize; up++) {
                          long vPos = BlockCompressedFilePointerUtil.makeFilePointer(block.pos, up);
                          boolean isActualRecordStart =
                              Arrays.binarySearch(virtualOffsets, vPos) >= 0;
                          boolean isRecordStart = bamRecordGuesser.checkRecordStart(vPos);
                          if (isActualRecordStart != isRecordStart) {
                            if (isRecordStart) {
                              mismatches.add(new Tuple2<>(vPos, RecordStartResult.FALSE_POSITIVE));
                            } else {
                              mismatches.add(new Tuple2<>(vPos, RecordStartResult.FALSE_NEGATIVE));
                            }
                          }
                        }
                      } else {
                        // just check virtual offsets in the index for false negatives
                        NavigableSet<Long> virtualOffsetsSet =
                            new TreeSet<>(
                                Arrays.stream(virtualOffsets).boxed().collect(Collectors.toList()));
                        for (Long vPos : virtualOffsetsSet.subSet(block.pos, block.end)) {
                          boolean isRecordStart = bamRecordGuesser.checkRecordStart(vPos);
                          if (!isRecordStart) {
                            mismatches.add(new Tuple2<>(vPos, RecordStartResult.FALSE_NEGATIVE));
                          }
                        }
                      }
                    }
                    return mismatches.iterator();
                  }
                });
  }
}
