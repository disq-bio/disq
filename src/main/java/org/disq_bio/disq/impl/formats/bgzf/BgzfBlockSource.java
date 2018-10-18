/*
 * Disq
 *
 * MIT License
 *
 * Copyright (c) 2018 Disq contributors
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
package org.disq_bio.disq.impl.formats.bgzf;

import htsjdk.samtools.util.AbstractIterator;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.disq_bio.disq.impl.file.*;
import org.disq_bio.disq.impl.formats.SerializableHadoopConfiguration;
import org.disq_bio.disq.impl.formats.bam.BamSource;
import org.disq_bio.disq.impl.formats.bgzf.BgzfBlockGuesser.BgzfBlock;

/**
 * This class can find BGZF block boundaries in a distributed manner, and then iterate over all the
 * blocks efficiently. It is not meant to be used directly by users, but instead is used by other
 * libraries that are based on BGZF, such as {@link BamSource}.
 *
 * @see BamSource
 */
public class BgzfBlockSource implements Serializable {

  private final PathSplitSource pathSplitSource;
  private final FileSystemWrapper fileSystemWrapper;

  public BgzfBlockSource(FileSystemWrapper fileSystemWrapper) {
    this.pathSplitSource = new PathSplitSource(fileSystemWrapper);
    this.fileSystemWrapper = fileSystemWrapper;
  }

  public JavaRDD<BgzfBlock> getBgzfBlocks(JavaSparkContext jsc, String path, int splitSize)
      throws IOException {

    final Configuration conf = jsc.hadoopConfiguration();
    SerializableHadoopConfiguration confSer = new SerializableHadoopConfiguration(conf);

    return pathSplitSource
        .getPathSplits(jsc, path, splitSize)
        .flatMap(
            (FlatMapFunction<PathSplit, BgzfBlock>)
                pathSplit -> {
                  BgzfBlockGuesser bgzfBlockGuesser =
                      getBgzfSplitGuesser(confSer.getConf(), pathSplit.getPath());
                  return getBgzfBlockIterator(bgzfBlockGuesser, pathSplit);
                });
  }

  private BgzfBlockGuesser getBgzfSplitGuesser(Configuration conf, String path) throws IOException {
    return new BgzfBlockGuesser(fileSystemWrapper.open(conf, path), path);
  }

  /**
   * @return an iterator over all the {@link BgzfBlock}s that start in the given {@link FileSplit}.
   */
  private static Iterator<BgzfBlock> getBgzfBlockIterator(
      BgzfBlockGuesser bgzfBlockGuesser, PathSplit split) {
    return getBgzfBlockIterator(bgzfBlockGuesser, split.getStart(), split.getEnd());
  }

  private static Iterator<BgzfBlock> getBgzfBlockIterator(
      BgzfBlockGuesser bgzfBlockGuesser, long splitStart, long splitEnd) {
    return new AbstractIterator<BgzfBlock>() {
      long start = splitStart;

      @Override
      protected BgzfBlock advance() {
        if (start >= splitEnd) { // splitEnd is exclusive, so if we reach it then stop
          bgzfBlockGuesser.close();
          return null; // end iteration
        }
        BgzfBlock bgzfBlock = bgzfBlockGuesser.guessNextBGZFPos(start, splitEnd);
        if (bgzfBlock == null) {
          bgzfBlockGuesser.close();
          return null; // end iteration
        } else {
          start = bgzfBlock.pos + bgzfBlock.cSize;
        }
        return bgzfBlock;
      }
    };
  }
}
