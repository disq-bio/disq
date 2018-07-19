package org.disq_bio.disq;

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
    jsc = new JavaSparkContext("local", "myapp", sparkConf);
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
}
