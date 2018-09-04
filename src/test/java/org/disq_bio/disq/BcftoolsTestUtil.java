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

public class BcftoolsTestUtil {

  private static final Logger logger = LoggerFactory.getLogger(SamtoolsTestUtil.class);

  private static final String BCFTOOLS_BIN_PROPERTY = "disq.bcftools.bin";

  public static boolean isBcftoolsAvailable() {
    String bin = getBcftoolsBin();
    if (bin == null) {
      logger.warn(
          "No bcftools binary found. Set property {} to enable testing with bcftools.",
          BCFTOOLS_BIN_PROPERTY);
      return false;
    }
    File binFile = new File(bin);
    if (!binFile.exists()) {
      throw new IllegalArgumentException(
          String.format(
              "%s property is set to non-existent file: %s", BCFTOOLS_BIN_PROPERTY, binFile));
    }
    return true;
  }

  private static String getBcftoolsBin() {
    return System.getProperty(BCFTOOLS_BIN_PROPERTY);
  }

  public static <T extends Locatable> int countVariants(String vcfPath) throws IOException {
    return countVariants(vcfPath, null);
  }

  public static <T extends Locatable> int countVariants(String vcfPath, T interval)
      throws IOException {
    File vcfFile = new File(URI.create(vcfPath));
    CommandLine commandLine = new CommandLine(getBcftoolsBin());
    commandLine.addArgument("view");
    commandLine.addArgument("-H"); // no header
    commandLine.addArgument(vcfFile.getAbsolutePath());
    if (interval != null) {
      commandLine.addArgument(
          String.format("%s:%s-%s", interval.getContig(), interval.getStart(), interval.getEnd()));
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ByteArrayOutputStream errorStream = new ByteArrayOutputStream();

    DefaultExecutor exec = new DefaultExecutor();
    PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, errorStream);
    exec.setStreamHandler(streamHandler);
    int rc = exec.execute(commandLine);
    // the actual variants, one per line (bcftools doesn't have a count option)
    String result = outputStream.toString().trim();
    String error = errorStream.toString();

    if (rc != 0) {
      throw new IllegalStateException(
          String.format(
              "Bcftools failed processing file %s with code %s. Stderr: %s", vcfFile, rc, error));
    }
    if (error.length() > 0) {
      System.err.println(
          String.format(
              "Bcftools produced stderr while processing file %s. Stderr: %s", vcfFile, error));
    }
    return result.split("\r\n|\r|\n").length;
  }
}
