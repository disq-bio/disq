package org.disq_bio.disq;

import htsjdk.samtools.util.Locatable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamtoolsTestUtil {

  private static final Logger logger = LoggerFactory.getLogger(SamtoolsTestUtil.class);

  private static final String SAMTOOLS_BIN_PROPERTY = "disq.samtools.bin";

  public static boolean isSamtoolsAvailable() {
    String bin = getSamtoolsBin();
    if (bin == null) {
      logger.warn(
          "No samtools binary found. Set property {} to enable testing with samtools.",
          SAMTOOLS_BIN_PROPERTY);
      return false;
    }
    File binFile = new File(bin);
    if (!binFile.exists()) {
      throw new IllegalArgumentException(
          String.format(
              "%s property is set to non-existent file: %s", SAMTOOLS_BIN_PROPERTY, binFile));
    }
    return true;
  }

  private static String getSamtoolsBin() {
    return System.getProperty(SAMTOOLS_BIN_PROPERTY);
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

    final File samFile = new File(URI.create(samPath));
    final File refFile = refPath == null ? null : new File(URI.create(refPath));
    CommandLine commandLine = new CommandLine(getSamtoolsBin());
    commandLine.addArgument("view");
    commandLine.addArgument("-c"); // count
    if (refFile != null) {
      commandLine.addArgument("-T");
      commandLine.addArgument(refFile.getAbsolutePath());
    }
    commandLine.addArgument(samFile.getAbsolutePath());
    if (traversalParameters != null) {
      if (traversalParameters.getIntervalsForTraversal() != null) {
        for (T locatable : traversalParameters.getIntervalsForTraversal()) {
          commandLine.addArgument(
              String.format(
                  "%s:%s-%s", locatable.getContig(), locatable.getStart(), locatable.getEnd()));
        }
      }
      if (traversalParameters.getTraverseUnplacedUnmapped()) {
        commandLine.addArgument("*");
      }
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ByteArrayOutputStream errorStream = new ByteArrayOutputStream();

    DefaultExecutor exec = new DefaultExecutor();
    PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, errorStream);
    exec.setStreamHandler(streamHandler);
    int rc = exec.execute(commandLine);
    String result = outputStream.toString().trim();
    String error = errorStream.toString();

    if (rc != 0) {
      throw new IllegalStateException(
          String.format(
              "Samtools failed processing file %s with code %s. Stderr: %s", samFile, rc, error));
    }
    if (error.length() > 0) {
      System.err.println(
          String.format(
              "Samtools produced stderr while processing file %s. Stderr: %s", samFile, error));
    }
    return Integer.parseInt(result);
  }
}
