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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import junitparams.JUnitParamsRunner;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class HtsjdkReadsRddHdfsTest extends HtsjdkReadsRddTest {

  private static MiniDFSCluster cluster;
  private static Path targetBaseDir;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = HdfsTestUtil.startMiniCluster(HtsjdkReadsRddHdfsTest.class.getName());

    URI clusterURI = cluster.getURI();
    URI hdfsDir =
        new URI(
            clusterURI.getScheme(),
            null,
            clusterURI.getHost(),
            clusterURI.getPort(),
            "/root",
            null,
            null);
    targetBaseDir = Paths.get(hdfsDir);
    Files.createDirectories(targetBaseDir);
    copyLocalResourcesToTargetFilesystem(targetBaseDir);
  }

  @AfterClass
  public static void teardownClass() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Override
  protected String getPath(String pathOrLocalResource) {
    if (pathOrLocalResource == null) {
      return null;
    }
    // resolve path relative to targetBaseDir base directory
    Path targetPath = targetBaseDir.resolve(pathOrLocalResource);
    return targetPath.toUri().toString();
  }

  @Override
  protected Path getTempDir() {
    return targetBaseDir.resolve("/tmp/");
  }

  @Override
  protected boolean inputFileMatches(String inputFile) {
    return !inputFile.startsWith("gs://"); // ignore gs: files
  }
}
