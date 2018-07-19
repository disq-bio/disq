package org.disq_bio.disq.impl.formats.sam;

import htsjdk.samtools.*;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.HtsjdkReadsTraversalParameters;
import org.disq_bio.disq.impl.file.FileSystemWrapper;

public abstract class AbstractSamSource implements Serializable {

  protected final FileSystemWrapper fileSystemWrapper;

  protected AbstractSamSource(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

  public abstract SamFormat getSamFormat();

  public SAMFileHeader getFileHeader(
      JavaSparkContext jsc,
      String path,
      ValidationStringency stringency,
      String referenceSourcePath)
      throws IOException {
    Configuration conf = jsc.hadoopConfiguration();
    String firstSamPath;
    if (fileSystemWrapper.isDirectory(conf, path)) {
      firstSamPath = fileSystemWrapper.firstFileInDirectory(conf, path);
    } else {
      firstSamPath = path;
    }
    try (SamReader samReader =
        createSamReader(conf, firstSamPath, stringency, referenceSourcePath)) {
      return samReader.getFileHeader();
    }
  }

  public abstract <T extends Locatable> JavaRDD<SAMRecord> getReads(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      ValidationStringency validationStringency,
      String referenceSourcePath)
      throws IOException;

  protected SamReader createSamReader(
      Configuration conf, String path, ValidationStringency stringency, String referenceSourcePath)
      throws IOException {
    SeekableStream in = fileSystemWrapper.open(conf, path);
    SeekableStream indexStream = findIndex(conf, path);
    SamReaderFactory readerFactory =
        SamReaderFactory.makeDefault()
            .setOption(SamReaderFactory.Option.CACHE_FILE_BASED_INDEXES, true)
            .setOption(SamReaderFactory.Option.EAGERLY_DECODE, false)
            .setUseAsyncIo(false);
    if (stringency != null) {
      readerFactory.validationStringency(stringency);
    }
    if (referenceSourcePath != null) {
      SeekableStream refIn = fileSystemWrapper.open(conf, referenceSourcePath);
      try (SeekableStream indexIn = fileSystemWrapper.open(conf, referenceSourcePath + ".fai")) {
        FastaSequenceIndex index = new FastaSequenceIndex(indexIn);
        ReferenceSequenceFile refSeqFile =
            ReferenceSequenceFileFactory.getReferenceSequenceFile(
                referenceSourcePath, refIn, index);
        readerFactory.referenceSource(new ReferenceSource(refSeqFile));
      }
    }
    SamInputResource resource = SamInputResource.of(in);
    if (indexStream != null) {
      resource.index(indexStream);
    }
    SamReader samReader = readerFactory.open(resource);
    ensureIndexWillBeClosed(samReader);
    return samReader;
  }

  protected SeekableStream findIndex(Configuration conf, String path) throws IOException {
    SamFormat samFormat = getSamFormat();
    if (samFormat.getIndexExtension() == null) {
      return null; // doesn't support indexes
    }
    String index = path + samFormat.getIndexExtension();
    if (fileSystemWrapper.exists(conf, index)) {
      return fileSystemWrapper.open(conf, index);
    }
    index =
        path.replaceFirst(
            Pattern.quote(samFormat.getExtension()) + "$", samFormat.getIndexExtension());
    if (fileSystemWrapper.exists(conf, index)) {
      return fileSystemWrapper.open(conf, index);
    }
    return null;
  }

  private SamReader ensureIndexWillBeClosed(SamReader samReader) {
    SamReader.PrimitiveSamReader underlyingReader =
        ((SamReader.PrimitiveSamReaderToSamReaderAdapter) samReader).underlyingReader();
    if (underlyingReader.hasIndex()) {
      // force BAMFileReader#mIndex to be populated so the index stream is properly
      // closed by the close() method
      underlyingReader.getIndex();
    }
    return samReader;
  }

  protected static <T> Stream<T> stream(final Iterator<T> iterator) {
    return StreamSupport.stream(((Iterable<T>) () -> iterator).spliterator(), false);
  }
}
