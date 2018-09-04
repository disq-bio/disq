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
