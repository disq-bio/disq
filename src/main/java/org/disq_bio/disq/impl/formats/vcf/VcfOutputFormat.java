package org.disq_bio.disq.impl.formats.vcf;

import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFHeader;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.disq_bio.disq.HtsjdkVariantsRdd;
import org.disq_bio.disq.impl.formats.bgzf.BGZFCodec;

/**
 * An output format for writing {@link VariantContext} objects to VCF files (including header). This
 * class should not be used directly.
 *
 * @see HtsjdkVariantsRdd
 */
public class VcfOutputFormat extends FileOutputFormat<Void, VariantContext> {

  static class VcfRecordWriter extends RecordWriter<Void, VariantContext> {

    private final VariantContextWriter variantContextWriter;

    public VcfRecordWriter(Configuration conf, Path file, VCFHeader header, String extension)
        throws IOException {
      OutputStream out = file.getFileSystem(conf).create(file);
      boolean compressed =
          extension.endsWith(BGZFCodec.DEFAULT_EXTENSION) || extension.endsWith(".gz");
      if (compressed) {
        out = new BlockCompressedOutputStream(out, null);
      }
      variantContextWriter =
          new VariantContextWriterBuilder().clearOptions().setOutputVCFStream(out).build();
      variantContextWriter.writeHeader(header);
    }

    @Override
    public void write(Void ignore, VariantContext variantContext) {
      variantContextWriter.add(variantContext);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
      variantContextWriter.close();
    }
  }

  private static VCFHeader header;
  private static String extension;

  public static void setHeader(VCFHeader vcfHeader) {
    VcfOutputFormat.header = vcfHeader;
  }

  public static void setExtension(String extension) {
    VcfOutputFormat.extension = extension;
  }

  @Override
  public RecordWriter<Void, VariantContext> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Path file = getDefaultWorkFile(taskAttemptContext, extension);
    return new VcfRecordWriter(taskAttemptContext.getConfiguration(), file, header, extension);
  }
}
