package org.disq_bio.disq.impl.formats.vcf;

import htsjdk.samtools.SamStreams;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.Locatable;
import htsjdk.samtools.util.OverlapDetector;
import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.tribble.index.Index;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.tribble.util.TabixUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.formats.bgzf.BGZFCodec;
import org.disq_bio.disq.impl.formats.bgzf.BGZFEnhancedGzipCodec;
import org.disq_bio.disq.impl.formats.tribble.TribbleIndexIntervalFilteringTextInputFormat;

public class VcfSource implements Serializable {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  public VCFHeader getFileHeader(JavaSparkContext jsc, String path) throws IOException {
    try (SeekableStream headerIn =
        fileSystemWrapper.open(jsc.hadoopConfiguration(), getFirstPath(jsc, path))) {
      InputStream is = bufferAndDecompressIfNecessary(headerIn);
      FeatureCodecHeader featureCodecHeader =
          new VCFCodec().readHeader(new AsciiLineReaderIterator(AsciiLineReader.from(is)));
      return (VCFHeader) featureCodecHeader.getHeaderValue();
    }
  }

  private VCFCodec getVCFCodec(JavaSparkContext jsc, String path) throws IOException {
    try (SeekableStream headerIn =
        fileSystemWrapper.open(jsc.hadoopConfiguration(), getFirstPath(jsc, path))) {
      InputStream is = bufferAndDecompressIfNecessary(headerIn);
      VCFCodec vcfCodec = new VCFCodec();
      vcfCodec.readHeader(new AsciiLineReaderIterator(AsciiLineReader.from(is)));
      return vcfCodec;
    }
  }

  private String getFirstPath(JavaSparkContext jsc, String path) throws IOException {
    Configuration conf = jsc.hadoopConfiguration();
    String firstPath;
    if (fileSystemWrapper.isDirectory(conf, path)) {
      firstPath = fileSystemWrapper.firstFileInDirectory(conf, path);
    } else {
      firstPath = path;
    }
    return firstPath;
  }

  private static InputStream bufferAndDecompressIfNecessary(final InputStream in)
      throws IOException {
    BufferedInputStream bis = new BufferedInputStream(in);
    // despite the name, SamStreams.isGzippedSAMFile looks for any gzipped stream (including block
    // compressed)
    return SamStreams.isGzippedSAMFile(bis) ? new GZIPInputStream(bis) : bis;
  }

  public <T extends Locatable> JavaRDD<VariantContext> getVariants(
      JavaSparkContext jsc, String path, int splitSize, List<T> intervals) throws IOException {

    // Use Hadoop FileSystem API to maintain file locality by using Hadoop's FileInputFormat

    final Configuration conf = jsc.hadoopConfiguration();
    if (splitSize > 0) {
      conf.setInt(FileInputFormat.SPLIT_MAXSIZE, splitSize);
    }
    enableBGZFCodecs(conf);

    VCFCodec vcfCodec = getVCFCodec(jsc, path);
    VCFHeader header = vcfCodec.getHeader();
    // get the version separately since htsjdk doesn't provide a way to get it from the header
    VCFHeaderVersion version = vcfCodec.getVersion();
    Broadcast<VCFHeader> headerBroadcast = jsc.broadcast(header);
    Broadcast<List<T>> intervalsBroadcast = intervals == null ? null : jsc.broadcast(intervals);

    return textFile(jsc, conf, path, intervals)
        .mapPartitions(
            (FlatMapFunction<Iterator<String>, VariantContext>)
                lines -> {
                  // VCFCodec is not threadsafe, so create a new one for each task
                  VCFCodec codec = new VCFCodec();
                  codec.setVCFHeader(headerBroadcast.getValue(), version);
                  final OverlapDetector<T> overlapDetector =
                      intervalsBroadcast == null
                          ? null
                          : OverlapDetector.create(intervalsBroadcast.getValue());
                  return stream(lines)
                      .filter(line -> !line.startsWith("#"))
                      .map(codec::decode)
                      .filter(vc -> overlapDetector == null || overlapDetector.overlapsAny(vc))
                      .iterator();
                });
  }

  private void enableBGZFCodecs(Configuration conf) {
    List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
    if (!codecs.contains(BGZFEnhancedGzipCodec.class)) {
      codecs.remove(GzipCodec.class);
      codecs.add(BGZFEnhancedGzipCodec.class);
    }
    if (!codecs.contains(BGZFCodec.class)) {
      codecs.add(BGZFCodec.class);
    }
    CompressionCodecFactory.setCodecClasses(conf, new ArrayList<>(codecs));
  }

  private <T extends Locatable> JavaRDD<String> textFile(
      JavaSparkContext jsc, Configuration conf, String path, List<T> intervals) throws IOException {
    if (intervals == null) {
      // Use this over JavaSparkContext#textFile since this allows the configuration to be passed in
      return jsc.newAPIHadoopFile(
              path,
              TextInputFormat.class,
              LongWritable.class,
              Text.class,
              jsc.hadoopConfiguration())
          .map(pair -> pair._2.toString())
          .setName(path);
    } else {
      String indexPath;
      VcfFormat vcfFormat = VcfFormat.fromPath(path);
      if (vcfFormat == null) {
        indexPath = path + TabixUtils.STANDARD_INDEX_EXTENSION; // try tabix
      } else {
        indexPath = path + vcfFormat.getIndexExtension();
      }
      if (!fileSystemWrapper.exists(conf, indexPath)) {
        throw new IllegalArgumentException(
            "Intervals set but no index file found for " + path + " at " + indexPath);
      }
      try (InputStream indexIn =
          indexFileInputStream(indexPath, fileSystemWrapper.open(conf, indexPath))) {
        Index index = IndexFactory.loadIndex(indexPath, indexIn);
        TribbleIndexIntervalFilteringTextInputFormat.setIndex(index);
        TribbleIndexIntervalFilteringTextInputFormat.setIntervals(intervals);
        return jsc.newAPIHadoopFile(
                path,
                TribbleIndexIntervalFilteringTextInputFormat.class,
                LongWritable.class,
                Text.class,
                jsc.hadoopConfiguration())
            .map(pair -> pair._2.toString())
            .setName(path);
      }
    }
  }

  private static InputStream indexFileInputStream(String indexPath, InputStream inputStreamInitial)
      throws IOException {
    if (indexPath.endsWith(".gz")) {
      return new GZIPInputStream(inputStreamInitial);
    } else if (indexPath.endsWith(TabixUtils.STANDARD_INDEX_EXTENSION)) {
      return new BlockCompressedInputStream(inputStreamInitial);
    } else {
      return inputStreamInitial;
    }
  }

  private static <T> Stream<T> stream(final Iterator<T> iterator) {
    return StreamSupport.stream(((Iterable<T>) () -> iterator).spliterator(), false);
  }
}
