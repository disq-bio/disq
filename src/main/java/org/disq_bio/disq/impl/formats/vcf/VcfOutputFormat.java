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
package org.disq_bio.disq.impl.formats.vcf;

import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
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
        out = new BlockCompressedOutputStream(out, (File) null);
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
