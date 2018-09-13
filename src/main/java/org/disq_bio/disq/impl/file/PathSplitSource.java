package org.disq_bio.disq.impl.file;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

public class PathSplitSource implements Serializable {

  private static final int DEFAULT_NIO_SPLIT_SIZE = 128 * 1024 * 1024;

  private final FileSystemWrapper fileSystemWrapper;

  /** @param fileSystemWrapper the filesystem wrapper to use when constructing splits. */
  public PathSplitSource(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

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
          .map(idx -> idx * actualSplitSize)
          .flatMap(
              splitStart -> {
                final long splitEnd =
                    splitStart + actualSplitSize > len ? len : splitStart + actualSplitSize;
                return Collections.singleton(new PathSplit(path, splitStart, splitEnd)).iterator();
              });
    } else {
      // Use Hadoop FileSystem API to maintain file locality by using Hadoop's FileInputFormat

      final Configuration conf = jsc.hadoopConfiguration();
      if (splitSize > 0) {
        conf.setInt(FileInputFormat.SPLIT_MAXSIZE, splitSize);
      }
      return jsc.newAPIHadoopFile(
              path, FileSplitInputFormat.class, Void.class, FileSplit.class, conf)
          .flatMap(
              (FlatMapFunction<Tuple2<Void, FileSplit>, PathSplit>)
                  t2 -> {
                    FileSplit fileSplit = t2._2();
                    PathSplit pathSplit =
                        new PathSplit(
                            fileSplit.getPath().toString(),
                            fileSplit.getStart(),
                            fileSplit.getStart() + fileSplit.getLength());
                    return Collections.singleton(pathSplit).iterator();
                  });
    }
  }
}
