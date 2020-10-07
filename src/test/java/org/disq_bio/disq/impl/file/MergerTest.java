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
package org.disq_bio.disq.impl.file;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.disq_bio.disq.HdfsTestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MergerTest {

  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = HdfsTestUtil.startMiniCluster(MergerTest.class.getName());
  }

  @AfterClass
  public static void teardownClass() {
    if (cluster != null) {
      cluster.shutdown();
    }
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

    new Merger(new HadoopFileSystemWrapper())
        .mergeParts(conf, dir.toURI().toString(), file3.toURI().toString());

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

    new Merger(new HadoopFileSystemWrapper())
        .mergeParts(
            cluster.getConfiguration(0), hdfsDir.toUri().toString(), hdfsFile3.toUri().toString());

    Assert.assertEquals(
        "contents1contents2",
        IOUtils.toString(cluster.getFileSystem().open(hdfsFile3), Charsets.UTF_8));
  }
}
