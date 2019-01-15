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
package org.disq_bio.disq.impl.formats.bgzf;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.spark.api.java.JavaRDD;
import org.disq_bio.disq.BaseTest;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.NioFileSystemWrapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class BgzfBlockSourceTest extends BaseTest {

  @Test
  @Parameters({"false", "true"})
  public void testFindAllBlocks(boolean useNio) throws IOException, URISyntaxException {
    String inputPath = ClassLoader.getSystemClassLoader().getResource("1.bam").toURI().toString();
    int splitSize = 128 * 1024;

    // find all the blocks in each partition
    JavaRDD<BgzfBlockGuesser.BgzfBlock> bgzfBlocks =
        new BgzfBlockSource(useNio ? new NioFileSystemWrapper() : new HadoopFileSystemWrapper())
            .getBgzfBlocks(jsc, inputPath, splitSize);
    List<BgzfBlockGuesser.BgzfBlock> collect = bgzfBlocks.collect();

    Assert.assertEquals(26, collect.size());

    Assert.assertEquals(0, collect.get(0).pos);
    Assert.assertEquals(14146, collect.get(0).cSize);
    Assert.assertEquals(65498, collect.get(0).uSize);
  }
}
