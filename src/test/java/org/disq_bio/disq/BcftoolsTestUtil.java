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

import htsjdk.samtools.util.Locatable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    Path binFile = Paths.get(bin);
    if (!Files.exists(binFile)) {
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
    Path vcfFile = Paths.get(URI.create(vcfPath));
    CommandLine commandLine = new CommandLine(getBcftoolsBin());
    commandLine.addArgument("view");
    commandLine.addArgument("-H"); // no header
    commandLine.addArgument(vcfFile.toAbsolutePath().toString());
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
    final String[] lines = result.split("\r\n|\r|\n");
    if (lines.length == 1 && lines[0].isEmpty()) {
      return 0;
    }
    return lines.length;
  }
}
