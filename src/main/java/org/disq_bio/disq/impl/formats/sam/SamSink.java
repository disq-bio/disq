package org.disq_bio.disq.impl.formats.sam;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.AsciiWriter;
import java.io.IOException;
import java.io.Writer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.HtsjdkReadsRdd;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.Merger;

/**
 * Write reads to a single SAM file on Spark. This is done by writing to multiple headerless SAM
 * files in parallel, then merging the resulting files into a single SAM file.
 *
 * @see SamSource
 * @see HtsjdkReadsRdd
 */
public class SamSink extends AbstractSamSink {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  @Override
  public void save(
      JavaSparkContext jsc,
      SAMFileHeader header,
      JavaRDD<SAMRecord> reads,
      String path,
      String referenceSourcePath,
      String tempPartsDirectory)
      throws IOException {

    reads.map(SAMRecord::getSAMString).map(String::trim).saveAsTextFile(tempPartsDirectory);

    String headerFile = tempPartsDirectory + "/header";
    try (Writer out =
        new AsciiWriter(fileSystemWrapper.create(jsc.hadoopConfiguration(), headerFile))) {
      new SAMTextHeaderCodec().encode(out, header);
    }
    new Merger().mergeParts(jsc.hadoopConfiguration(), tempPartsDirectory, path);
    fileSystemWrapper.delete(jsc.hadoopConfiguration(), tempPartsDirectory);
  }
}
