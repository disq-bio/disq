package org.disq_bio.disq.impl.formats.sam;

import htsjdk.samtools.DefaultSAMRecordFactory;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMLineParser;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.HtsjdkReadsTraversalParameters;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;

public class SamSource extends AbstractSamSource implements Serializable {

  public SamSource() {
    super(new HadoopFileSystemWrapper());
  }

  @Override
  public SamFormat getSamFormat() {
    return SamFormat.SAM;
  }

  @Override
  public <T extends Locatable> JavaRDD<SAMRecord> getReads(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      ValidationStringency validationStringency,
      String referenceSourcePath)
      throws IOException {

    // Use Hadoop FileSystem API to maintain file locality by using Hadoop's FileInputFormat

    final Configuration conf = jsc.hadoopConfiguration();
    if (splitSize > 0) {
      conf.setInt(FileInputFormat.SPLIT_MAXSIZE, splitSize);
    }

    SAMFileHeader samHeader = getFileHeader(jsc, path, validationStringency, referenceSourcePath);
    Broadcast<SAMFileHeader> samHeaderBroadcast = jsc.broadcast(samHeader);
    Broadcast<HtsjdkReadsTraversalParameters<T>> traversalParametersBroadcast =
        traversalParameters == null ? null : jsc.broadcast(traversalParameters);

    return textFile(jsc, path)
        .mapPartitions(
            (FlatMapFunction<Iterator<String>, SAMRecord>)
                lines -> {
                  SAMLineParser samLineParser =
                      new SAMLineParser(
                          new DefaultSAMRecordFactory(),
                          validationStringency,
                          samHeaderBroadcast.getValue(),
                          null,
                          null);
                  final TraversalOverlapDetector<T> overlapDetector =
                      traversalParametersBroadcast == null
                          ? null
                          : new TraversalOverlapDetector<>(traversalParametersBroadcast.getValue());
                  return stream(lines)
                      .filter(line -> !line.startsWith("@"))
                      .map(samLineParser::parseLine)
                      .filter(
                          record -> overlapDetector == null || overlapDetector.overlapsAny(record))
                      .iterator();
                });
  }

  private <T extends Locatable> JavaRDD<String> textFile(JavaSparkContext jsc, String path) {
    // Use this over JavaSparkContext#textFile since this allows the configuration to be passed in
    return jsc.newAPIHadoopFile(
            path, TextInputFormat.class, LongWritable.class, Text.class, jsc.hadoopConfiguration())
        .map(pair -> pair._2.toString())
        .setName(path);
  }
}
