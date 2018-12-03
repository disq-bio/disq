/*
 * Disq
 *
 * MIT License
 *
 * Copyright (c) 2018 Disq contributors
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

import htsjdk.samtools.SBIIndex;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
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
    sparkConf.set("spark.kryo.registrator", "org.disq_bio.disq.serializer.DisqKryoRegistrator");
    sparkConf.set("spark.kryo.referenceTracking", "true");
    sparkConf.set("spark.kryo.registrationRequired", "true");
    jsc = new JavaSparkContext("local[*]", "disq-test", sparkConf);
  }

  @AfterClass
  public static void teardown() {
    jsc.stop();
  }

  protected String getPath(String pathOrLocalResource) throws URISyntaxException {
    if (pathOrLocalResource == null) {
      return null;
    }
    URL resource = ClassLoader.getSystemClassLoader().getResource(pathOrLocalResource);
    if (resource == null) {
      return pathOrLocalResource;
    }
    return resource.toURI().toString();
  }

  private File createTempFile(String extension) throws IOException {
    File file = File.createTempFile("test", extension);
    file.delete();
    file.deleteOnExit();
    return file;
  }

  protected String createTempPath(String extension) throws IOException {
    return createTempFile(extension).toURI().toString();
  }

  protected List<String> listPartFiles(String dir) {
    return Arrays.stream(
            new File(URI.create(dir)).listFiles(file -> file.getName().startsWith("part-")))
        .map(f -> f.toURI().toString())
        .collect(Collectors.toList());
  }

  protected List<String> listSBIIndexFiles(String dir) {
    return Arrays.stream(
            new File(URI.create(dir))
                .listFiles(file -> file.getName().endsWith(SBIIndex.FILE_EXTENSION)))
        .map(f -> f.toURI().toString())
        .collect(Collectors.toList());
  }
}
