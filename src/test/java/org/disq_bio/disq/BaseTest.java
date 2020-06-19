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

import htsjdk.samtools.util.FileExtensions;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class BaseTest {
  protected static JavaSparkContext jsc;

  @BeforeClass
  public static void setup() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.driver.bindAddress", "127.0.0.1"); 
    sparkConf.set("spark.kryo.registrator", "org.disq_bio.disq.serializer.DisqKryoRegistrator");
    sparkConf.set("spark.kryo.referenceTracking", "true");
    sparkConf.set("spark.kryo.registrationRequired", "true");
    jsc = new JavaSparkContext("local[*]", "disq-test", sparkConf);
  }

  @AfterClass
  public static void teardown() {
    jsc.stop();
  }

  protected static void copyLocalResourcesToTargetFilesystem(Path targetBaseDir) throws Exception {
    Path source = Paths.get("src/test/resources");
    Files.walkFileTree(
        source,
        EnumSet.of(FileVisitOption.FOLLOW_LINKS),
        Integer.MAX_VALUE,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            Path rel = source.relativize(dir);
            Path targetdir = targetBaseDir.resolve(rel.toString());
            try {
              Files.copy(dir, targetdir);
            } catch (FileAlreadyExistsException e) {
              if (!Files.isDirectory(targetdir)) {
                throw e;
              }
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.copy(file, targetBaseDir.resolve(source.relativize(file).toString()));
            return FileVisitResult.CONTINUE;
          }
        });
  }

  protected static String getPathOrLocalResource(String pathOrLocalResource)
      throws URISyntaxException {
    if (pathOrLocalResource == null) {
      return null;
    }
    URL resource = ClassLoader.getSystemClassLoader().getResource(pathOrLocalResource);
    if (resource == null) {
      return pathOrLocalResource;
    }
    return resource.toURI().toString();
  }

  protected String getPath(String pathOrLocalResource) throws URISyntaxException {
    return getPathOrLocalResource(pathOrLocalResource);
  }

  /**
   * @return a path for the temporary directory, or <code>null</code> to for the default local
   *     temporary directory
   */
  protected Path getTempDir() {
    return null;
  }

  protected String createTempPath(String extension) throws IOException {
    Path tempDir = getTempDir();
    final Path tempFile =
        tempDir == null
            ? Files.createTempFile("test", extension)
            : Files.createTempFile(tempDir, "test", extension);
    Files.deleteIfExists(tempFile);
    try {
      tempFile.toFile().deleteOnExit();
    } catch (UnsupportedOperationException e) {
      // not local filesystem, ignore
    }
    return tempFile.toUri().toString();
  }

  protected List<String> listPartFiles(String dir) throws IOException {
    return Files.list(Paths.get(URI.create(dir)))
        .filter(path -> path.getFileName().toString().startsWith("part-"))
        .map(path -> path.toUri().toString())
        .collect(Collectors.toList());
  }

  protected List<String> listSBIIndexFiles(String dir) throws IOException {
    return Files.list(Paths.get(URI.create(dir)))
        .filter(path -> path.getFileName().toString().endsWith(FileExtensions.SBI))
        .map(path -> path.toUri().toString())
        .collect(Collectors.toList());
  }
}
