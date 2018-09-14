package org.disq_bio.disq.impl.formats.bam;

import static org.junit.Assert.fail;

import htsjdk.samtools.Chunk;
import htsjdk.samtools.ValidationStringency;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.disq_bio.disq.BaseTest;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.PathChunk;
import org.disq_bio.disq.impl.formats.bgzf.BgzfBlockGuesser;
import org.disq_bio.disq.impl.formats.bgzf.BgzfBlockSource;
import org.junit.Test;

public class BamSourceTest extends BaseTest {

  @Test
  public void testPathChunksDontOverlap() throws Exception {
    String inputFile =
        "HiSeq.1mb.1RG.2k_lines.alternate.recalibrated.DIQ.sharded.bam/part-r-00000.bam";

    HadoopFileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();
    BgzfBlockSource bgzfBlockSource = new BgzfBlockSource(fileSystemWrapper);
    List<BgzfBlockGuesser.BgzfBlock> bgzfBlocks =
        bgzfBlockSource.getBgzfBlocks(jsc, getPath(inputFile), 128 * 1024).collect();

    for (BgzfBlockGuesser.BgzfBlock block : bgzfBlocks) {
      if (block.pos > 64 * 1024) { // look for blocks bigger than uncompressed BGZF block size
        int splitSize = (int) block.pos; // try splits that are the BGZF block size

        BamSource bamSource = new BamSource(fileSystemWrapper);
        List<Chunk> chunks =
            bamSource
                .getPathChunks(
                    jsc, getPath(inputFile), splitSize, ValidationStringency.SILENT, null)
                .map(PathChunk::getSpan)
                .collect();
        chunks = new ArrayList<>(chunks);
        Collections.sort(chunks);
        for (int i = 0; i < chunks.size() - 1; i++) {
          if (chunks.get(i).overlaps(chunks.get(i + 1))) {
            fail(
                String.format(
                    "Overlapping chunks for split size %s: %s and %s (chunks: %s)",
                    splitSize, chunks.get(i), chunks.get(i + 1), chunks));
          }
        }
      }
    }
  }
}
