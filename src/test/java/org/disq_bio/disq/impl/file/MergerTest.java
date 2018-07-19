package org.disq_bio.disq.impl.file;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MergerTest {

  private static MiniDFSCluster cluster;
  private static URI clusterUri;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = startMini(MergerTest.class.getName());
    clusterUri = formalizeClusterURI(cluster.getFileSystem().getUri());
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static MiniDFSCluster startMini(String testName) throws IOException {
    File baseDir = new File("./target/hdfs/" + testName).getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    MiniDFSCluster hdfsCluster = builder.clusterId(testName).build();
    hdfsCluster.waitActive();
    return hdfsCluster;
  }

  protected static URI formalizeClusterURI(URI clusterUri) throws URISyntaxException {
    if (clusterUri.getPath() == null) {
      return new URI(
          clusterUri.getScheme(),
          null,
          clusterUri.getHost(),
          clusterUri.getPort(),
          "/",
          null,
          null);
    } else if (clusterUri.getPath().trim() == "") {
      return new URI(
          clusterUri.getScheme(),
          null,
          clusterUri.getHost(),
          clusterUri.getPort(),
          "/",
          null,
          null);
    }
    return clusterUri;
  }

  @Test
  public void testLocalFiles() throws IOException {
    Configuration conf = new Configuration();
    File dir = File.createTempFile("dir", "");
    dir.delete();
    dir.mkdir();
    File file1 = File.createTempFile("file1", ".txt", dir);
    File file2 = File.createTempFile("file2", ".txt", dir);
    File file3 = File.createTempFile("file3", ".txt");
    file3.delete();

    Files.write("contents1", file1, Charset.forName("UTF8"));
    Files.write("contents2", file2, Charset.forName("UTF8"));

    new Merger().mergeParts(conf, dir.toURI().toString(), file3.toURI().toString());

    Assert.assertEquals("contents1contents2", Files.toString(file3, Charset.forName("UTF8")));
  }

  @Test
  public void testHdfsFiles() throws IOException {
    File dir = File.createTempFile("dir", "");
    dir.delete();
    dir.mkdir();
    File file1 = File.createTempFile("file1", ".txt", dir);
    File file2 = File.createTempFile("file2", ".txt", dir);
    File file3 = File.createTempFile("file3", ".txt");
    file3.delete();

    Files.write("contents1", file1, Charset.forName("UTF8"));
    Files.write("contents2", file2, Charset.forName("UTF8"));

    // copy to HDFS
    Path hdfsDir = new Path(".");
    Path hdfsFile1 = new Path("file1.txt");
    Path hdfsFile2 = new Path("file2.txt");
    Path hdfsFile3 = new Path("file3.txt");
    hdfsDir = cluster.getFileSystem().makeQualified(hdfsDir);
    hdfsFile3 = cluster.getFileSystem().makeQualified(hdfsFile3);
    cluster.getFileSystem().copyFromLocalFile(new Path(file1.toURI().toString()), hdfsFile1);
    cluster.getFileSystem().copyFromLocalFile(new Path(file2.toURI().toString()), hdfsFile2);

    new Merger()
        .mergeParts(
            cluster.getConfiguration(0), hdfsDir.toUri().toString(), hdfsFile3.toUri().toString());

    Assert.assertEquals(
        "contents1contents2", IOUtils.toString(cluster.getFileSystem().open(hdfsFile3)));
  }
}
