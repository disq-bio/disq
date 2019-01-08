package htsjdk.variant.variantcontext.writer;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.tribble.index.IndexCreator;

import java.io.OutputStream;
import java.nio.file.Path;

public class VCFWriterHelper {

  // TODO: change htsjdk so this is not needed
  public static VCFWriter buildVCFWriter(OutputStream writerStream, SAMSequenceDictionary refDict, IndexCreator idxCreator, boolean enableOnTheFlyIndexing) {
    return new VCFWriter((Path) null, writerStream, refDict, idxCreator,
        enableOnTheFlyIndexing,
        false,
        false,
        false);
  }
}
