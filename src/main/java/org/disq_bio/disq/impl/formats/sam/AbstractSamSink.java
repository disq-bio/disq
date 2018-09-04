package org.disq_bio.disq.impl.formats.sam;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class AbstractSamSink {
  public abstract void save(
      JavaSparkContext jsc,
      SAMFileHeader header,
      JavaRDD<SAMRecord> reads,
      String path,
      String referenceSourcePath,
      String tempPartsDirectory)
      throws IOException;
}
