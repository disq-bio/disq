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
    Path binFile = Paths.get(bin);
    if (!Files.exists(binFile)) {
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

    final Path samFile = Paths.get(URI.create(samPath));
    final Path refFile = refPath == null ? null : Paths.get(URI.create(refPath));
    CommandLine commandLine = new CommandLine(getSamtoolsBin());
    commandLine.addArgument("view");
    commandLine.addArgument("-c"); // count
    if (refFile != null) {
      commandLine.addArgument("-T");
      commandLine.addArgument(refFile.toAbsolutePath().toString());
    }
    commandLine.addArgument(samFile.toAbsolutePath().toString());
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
