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
package org.disq_bio.disq;

import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.CRAMCRAIIndexer;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordSetBuilder;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.SamStreams;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.seekablestream.SeekablePathStream;
import htsjdk.samtools.util.Locatable;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import org.disq_bio.disq.impl.file.NioFileSystemWrapper;
import org.disq_bio.disq.impl.formats.BoundedTraversalUtil;
import org.disq_bio.disq.impl.formats.sam.SamFormat;
import org.junit.Assert;

public class AnySamTestUtil {
  // use a target contig size that corresponds to the size of the contig in the test.fa reference
  // file used by the tests
  private static final int TARGET_CONTIG_SIZE = 47600;

  public static String writeAnySamFile(
      int numPairs,
      SAMFileHeader.SortOrder sortOrder,
      ReadsFormatWriteOption formatWriteOption,
      String refPath)
      throws IOException {
    SamFormat samFormat = SamFormat.fromFormatWriteOption(formatWriteOption);
    // file will be both queryname and coordinate sorted, so use one or the other
    SAMRecordSetBuilder samRecordSetBuilder =
        new SAMRecordSetBuilder(true, sortOrder, true, TARGET_CONTIG_SIZE);
    for (int i = 0; i < numPairs; i++) {
      int chr = 20;
      int start1 = (i + 1) * 40;
      int start2 = start1 + 100;
      if (start1 > TARGET_CONTIG_SIZE || start2 > TARGET_CONTIG_SIZE) {
        // reads that are mapped outside of the target reference contig span can cause problems for
        // CRAM files
        throw new IllegalStateException(
            String.format("Read is mapped outside of the target span: %d", TARGET_CONTIG_SIZE));
      }
      if (i == 5) { // add two unmapped fragments instead of a mapped pair
        samRecordSetBuilder.addFrag(
            String.format("test-read-%03d-1", i), chr, start1, false, true, null, null, -1, false);
        samRecordSetBuilder.addFrag(
            String.format("test-read-%03d-2", i), chr, start2, false, true, null, null, -1, false);
      } else {
        samRecordSetBuilder.addPair(String.format("test-read-%03d", i), chr, start1, start2);
      }
    }
    if (numPairs > 0) { // add two unplaced unmapped fragments if non-empty
      samRecordSetBuilder.addUnmappedFragment(
          String.format("test-read-%03d-unplaced-unmapped", numPairs++));
      samRecordSetBuilder.addUnmappedFragment(
          String.format("test-read-%03d-unplaced-unmapped", numPairs++));
    }

    final Path bamFile = Files.createTempFile("test", samFormat.getExtension());
    try {
      bamFile.toFile().deleteOnExit();
    } catch (UnsupportedOperationException e) {
      // non-local files will be cleaned up by deleting the directory
    }
    SAMFileHeader samHeader = samRecordSetBuilder.getHeader();
    final SAMFileWriter bamWriter;
    if (samFormat.equals(SamFormat.CRAM)) {
      bamWriter =
          new SAMFileWriterFactory()
              .makeCRAMWriter(samHeader, true, bamFile, NioFileSystemWrapper.asPath(refPath));
    } else {
      bamWriter = new SAMFileWriterFactory().makeSAMOrBAMWriter(samHeader, true, bamFile);
    }
    for (final SAMRecord rec : samRecordSetBuilder.getRecords()) {
      bamWriter.addAlignment(rec);
    }
    bamWriter.close();

    // create BAM index
    if (sortOrder.equals(SAMFileHeader.SortOrder.coordinate)) {
      if (samFormat.equals(SamFormat.CRAM)) {
        OutputStream out =
            Files.newOutputStream(
                bamFile.resolveSibling(bamFile.getFileName() + samFormat.getIndexExtension()));
        CRAMCRAIIndexer.writeIndex(new SeekablePathStream(bamFile), out);
      } else if (samFormat.equals(SamFormat.BAM)) {
        SamReader samReader =
            SamReaderFactory.makeDefault()
                .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS)
                .open(bamFile);
        BAMIndexer.createIndex(
            samReader,
            bamFile.resolveSibling(
                bamFile
                    .getFileName()
                    .toString()
                    .replaceFirst(samFormat.getExtension() + "$", samFormat.getIndexExtension())));
      }
      // no index for SAM
    }

    return bamFile.toUri().toString();
  }

  public static int countReads(final String samPath) throws IOException {
    return countReads(samPath, null);
  }

  public static int countReads(final String samPath, String refPath) throws IOException {
    return countReads(samPath, refPath, null);
  }

  public static <T extends Locatable> int countReads(
      final String samPath, String refPath, HtsjdkReadsTraversalParameters<T> traversalParameters)
      throws IOException {
    return countReads(
        samPath, refPath, traversalParameters, ValidationStringency.DEFAULT_STRINGENCY);
  }

  public static <T extends Locatable> int countReads(
      final String samPath,
      String refPath,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      ValidationStringency validationStringency)
      throws IOException {

    final Path samFile = NioFileSystemWrapper.asPath(samPath);
    final Path refFile = refPath == null ? null : NioFileSystemWrapper.asPath(refPath);

    // test file contents is consistent with extension
    try (InputStream in = new BufferedInputStream(Files.newInputStream(samFile))) {
      SamFormat samFormat = SamFormat.fromPath(samFile.toString());
      if (samFormat == null) {
        Assert.fail("File is not recognized: " + samFile);
      }
      switch (samFormat) {
        case BAM:
          Assert.assertTrue("Not a BAM file", SamStreams.isBAMFile(in));
          break;
        case CRAM:
          Assert.assertTrue("Not a CRAM file", SamStreams.isCRAMFile(in));
          break;
        case SAM:
          Assert.assertTrue("Not a SAM file", in.read() == '@');
          break;
        default:
          Assert.fail("File is not BAM, CRAM or SAM.");
      }
    }

    ReferenceSource referenceSource = refFile == null ? null : new ReferenceSource(refFile);
    int recCount = 0;

    if (SamFormat.SAM.fileMatches(samFile.toString())) {
      // we can't call query() on SamReader for SAM files, so we have to do interval filtering here
      try (SamReader samReader =
          SamReaderFactory.makeDefault()
              .validationStringency(validationStringency)
              .referenceSource(referenceSource)
              .open(SamInputResource.of(samFile))) {
        for (SAMRecord record : samReader) {
          Assert.assertNotNull(record.getHeader());
          if (traversalParameters == null) {
            recCount++;
          } else {
            if (traversalParameters.getIntervalsForTraversal() != null) {
              for (T interval : traversalParameters.getIntervalsForTraversal()) {
                if (interval.overlaps(record)) {
                  recCount++;
                  break;
                }
              }
            }
            if (traversalParameters.getTraverseUnplacedUnmapped()
                && record.getReadUnmappedFlag()
                && record.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START) {
              recCount++;
            }
          }
        }
      }
      return recCount;
    }

    try (SamReader bamReader =
        SamReaderFactory.makeDefault()
            .validationStringency(validationStringency)
            .referenceSource(referenceSource)
            .open(SamInputResource.of(samFile))) {
      Iterator<SAMRecord> it;
      if (traversalParameters == null) {
        it = bamReader.iterator();
      } else if (traversalParameters.getIntervalsForTraversal() == null
          || traversalParameters.getIntervalsForTraversal().isEmpty()) {
        it = Collections.emptyIterator();
      } else {
        SAMSequenceDictionary sequenceDictionary =
            bamReader.getFileHeader().getSequenceDictionary();
        QueryInterval[] queryIntervals =
            BoundedTraversalUtil.prepareQueryIntervals(
                traversalParameters.getIntervalsForTraversal(), sequenceDictionary);
        it = bamReader.queryOverlapping(queryIntervals);
      }
      recCount += size(it);
    }

    if (traversalParameters != null && traversalParameters.getTraverseUnplacedUnmapped()) {
      try (SamReader bamReader =
          SamReaderFactory.makeDefault()
              .referenceSource(referenceSource)
              .open(SamInputResource.of(samFile))) {
        Iterator<SAMRecord> it = bamReader.queryUnmapped();
        recCount += size(it);
      }
    }

    return recCount;
  }

  private static int size(Iterator<SAMRecord> iterator) {
    int count = 0;
    while (iterator.hasNext()) {
      SAMRecord next = iterator.next();
      Assert.assertNotNull(next.getHeader());
      count++;
    }
    return count;
  }
}
