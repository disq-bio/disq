package org.disq_bio.disq;

import com.google.common.io.Files;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.Interval;
import htsjdk.tribble.util.TabixUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.*;
import java.net.URI;
import java.util.Iterator;

public class VcfTestUtil {
  public static VCFFileReader parseVcf(File vcf) throws IOException {
    File actualVcf;
    // work around TribbleIndexedFeatureReader not reading header from .bgz files
    if (vcf.getName().endsWith(".bgz")) {
      actualVcf = File.createTempFile(vcf.getName(), ".gz");
      actualVcf.deleteOnExit();
      Files.copy(vcf, actualVcf);
      File tbi = new File(vcf.getParent(), vcf.getName() + TabixUtils.STANDARD_INDEX_EXTENSION);
      if (tbi.exists()) {
        File actualTbi =
            new File(
                actualVcf.getParent(), actualVcf.getName() + TabixUtils.STANDARD_INDEX_EXTENSION);
        actualTbi.deleteOnExit();
        Files.copy(tbi, actualTbi);
      }
    } else {
      actualVcf = vcf;
    }
    return new VCFFileReader(actualVcf, false);
  }

  public static int countVariants(final String vcfPath) throws IOException {
    return countVariants(vcfPath, null);
  }

  public static int countVariants(final String vcfPath, Interval interval) throws IOException {
    File vcfFile = new File(URI.create(vcfPath));
    final VCFFileReader vcfFileReader = parseVcf(vcfFile);
    final Iterator<VariantContext> it;
    if (interval == null) {
      it = vcfFileReader.iterator();
    } else {
      it = vcfFileReader.query(interval.getContig(), interval.getStart(), interval.getEnd());
    }
    int recCount = 0;
    while (it.hasNext()) {
      it.next();
      recCount++;
    }
    vcfFileReader.close();
    return recCount;
  }

  public static boolean isBlockCompressed(String path) throws IOException {
    try (InputStream in =
        new BufferedInputStream(new FileInputStream(new File(URI.create(path))))) {
      return BlockCompressedInputStream.isValidFile(in);
    }
  }
}
