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

  /**
   * @param useNio if true use the NIO filesystem APIs rather than the Hadoop filesystem APIs. This
   *     is appropriate for cloud stores where file locality is not relied upon.
   */
  public BgzfBlockSource(boolean useNio) {
    this.pathSplitSource = new PathSplitSource(useNio);
    this.fileSystemWrapper = useNio ? new NioFileSystemWrapper() : new HadoopFileSystemWrapper();
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
        if (start > splitEnd) {
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
