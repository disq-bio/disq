/*
 * Disq
 *
 * MIT License
 *
 * Copyright (c) 2018 Disq contributors
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

import htsjdk.tribble.index.tabix.TabixFormat;
import htsjdk.tribble.index.tabix.TabixIndexCreator;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VCFWriterHelper;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
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
import org.disq_bio.disq.TabixIndexWriteOption;
import org.disq_bio.disq.impl.formats.bgzf.TerminatorlessBlockCompressedOutputStream;

/**
 * An output format for writing {@link VariantContext} objects to VCF files that don't have a header
 * (or terminator), so they can be merged into a single file easily. Files do not have the usual
 * ".vcf" extension since they are not complete VCF files. This class should not be used directly.
 *
 * @see HtsjdkVariantsRdd
 */
public class HeaderlessVcfOutputFormat extends FileOutputFormat<Void, VariantContext> {

  static class VcfRecordWriter extends RecordWriter<Void, VariantContext> {

    private final VariantContextWriter variantContextWriter;

    VcfRecordWriter(
        Configuration conf, Path file, VCFHeader header, boolean blockCompress, Path tbiFile)
        throws IOException {
      if (!blockCompress && tbiFile != null) {
        throw new IllegalArgumentException(
            "Cannot create tabix index for file that is not block compressed.");
      }
      OutputStream out = file.getFileSystem(conf).create(file);
      TabixIndexCreator tabixIndexCreator;
      if (tbiFile == null) {
        tabixIndexCreator = null;
      } else {
        OutputStream tbiOut = file.getFileSystem(conf).create(tbiFile);
        tabixIndexCreator =
            new StreamBasedTabixIndexCreator(
                header.getSequenceDictionary(), TabixFormat.VCF, tbiOut);
      }
      this.variantContextWriter =
          VCFWriterHelper.buildVCFWriter(
              blockCompress ? new TerminatorlessBlockCompressedOutputStream(out, (File) null) : out,
              header.getSequenceDictionary(),
              tabixIndexCreator,
              tbiFile != null);
      variantContextWriter.setHeader(header);
    }

    @Override
    public void write(Void ignore, VariantContext variantContext) {
      variantContextWriter.add(variantContext);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
      variantContextWriter.close();
    }
  }

  private static VCFHeader header;
  private static boolean blockCompress;
  private static boolean writeTbiFile;

  public static void setHeader(VCFHeader header) {
    HeaderlessVcfOutputFormat.header = header;
  }

  public static void setBlockCompress(boolean blockCompress) {
    HeaderlessVcfOutputFormat.blockCompress = blockCompress;
  }

  public static void setWriteTbiFile(boolean writeTbiFile) {
    HeaderlessVcfOutputFormat.writeTbiFile = writeTbiFile;
  }

  @Override
  public RecordWriter<Void, VariantContext> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Path file = getDefaultWorkFile(taskAttemptContext, "");
    Path tbiFile;
    if (writeTbiFile) {
      // ensure tbi files are hidden so they don't interfere with merging of part files
      tbiFile =
          new Path(
              file.getParent(), "." + file.getName() + TabixIndexWriteOption.getIndexExtension());
    } else {
      tbiFile = null;
    }
    return new VcfRecordWriter(
        taskAttemptContext.getConfiguration(), file, header, blockCompress, tbiFile);
  }
}
