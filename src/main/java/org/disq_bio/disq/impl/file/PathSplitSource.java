package org.disq_bio.disq.impl.file;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class PathSplitSource implements Serializable {

  private static final int DEFAULT_NIO_SPLIT_SIZE = 128 * 1024 * 1024;
  private static final Logger logger = LoggerFactory.getLogger(PathSplitSource.class);

  private final FileSystemWrapper fileSystemWrapper;

  /** @param fileSystemWrapper the filesystem wrapper to use when constructing splits. */
  public PathSplitSource(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

  /**
   * Load {@link PathSplit}s for the given filesystem path.
   *
   * @param jsc the Spark context to use
   * @param path the path to load splits for
   * @param splitSize the requested size of file splits in bytes. If zero, the default split size
   *     for the Hadoop file system will be used (for the Hadoop filesystem wrapper), or for NIO
   *     splits of size 128MB will be used.
   * @return an RDD of path splits
   * @throws IOException if an IO error occurs
   */
  public JavaRDD<PathSplit> getPathSplits(JavaSparkContext jsc, String path, int splitSize)
      throws IOException {
    if (fileSystemWrapper.usesNio()) {
      // Use Java NIO by creating splits with Spark parallelize. File locality is not maintained,
      // but this is not an issue if reading from a cloud store.

      long actualSplitSize = splitSize <= 0 ? DEFAULT_NIO_SPLIT_SIZE : splitSize;
      long len = fileSystemWrapper.getFileLength(null, path);
      int numSplits = (int) Math.ceil((double) len / actualSplitSize);

      List<Long> range = LongStream.range(0, numSplits).boxed().collect(Collectors.toList());
      return jsc.parallelize(range, numSplits)
          .map(
              idx -> {
                long splitStart = idx * actualSplitSize;
                final long splitEnd =
                    splitStart + actualSplitSize > len ? len : splitStart + actualSplitSize;
                PathSplit pathSplit = new PathSplit(path, splitStart, splitEnd);
                logger.debug("PathSplit for partition {}: {}", idx, pathSplit);
                return pathSplit;
              });
    } else {
      // Use Hadoop FileSystem API to maintain file locality by using Hadoop's FileInputFormat

      final Configuration conf = jsc.hadoopConfiguration();
      if (splitSize > 0) {
        conf.setInt(FileInputFormat.SPLIT_MAXSIZE, splitSize);
      }
      return jsc.newAPIHadoopFile(
              path, FileSplitInputFormat.class, Void.class, FileSplit.class, conf)
          .mapPartitionsWithIndex(
              (Function2<Integer, Iterator<Tuple2<Void, FileSplit>>, Iterator<PathSplit>>)
                  (idx, it) -> {
                    Tuple2<Void, FileSplit> t2 = it.next(); // one file split per partition
                    FileSplit fileSplit = t2._2();
                    PathSplit pathSplit =
                        new PathSplit(
                            fileSplit.getPath().toString(),
                            fileSplit.getStart(),
                            fileSplit.getStart() + fileSplit.getLength());
                    logger.debug("PathSplit for partition {}: {}", idx, pathSplit);
                    return Collections.singleton(pathSplit).iterator();
                  },
              true);
    }
  }
}
