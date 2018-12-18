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

import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.tribble.util.TabixUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.disq_bio.disq.TabixIndexWriteOption;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.HiddenFileFilter;
import org.disq_bio.disq.impl.file.Merger;
import org.disq_bio.disq.impl.file.TbiMerger;
import org.disq_bio.disq.impl.formats.bgzf.BGZFCodec;
import scala.Tuple2;

public class VcfSink extends AbstractVcfSink {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  @Override
  public void save(
      JavaSparkContext jsc,
      VCFHeader vcfHeader,
      JavaRDD<VariantContext> variants,
      String path,
      String tempPartsDirectory,
      List<String> indexesToEnable)
      throws IOException {
    Broadcast<VCFHeader> vcfHeaderBroadcast = jsc.broadcast(vcfHeader);
    boolean compressed = path.endsWith(BGZFCodec.DEFAULT_EXTENSION) || path.endsWith(".gz");
    boolean writeTbiFile =
        compressed && indexesToEnable.contains(TabixIndexWriteOption.getIndexExtension());
    variants
        .mapPartitions(
            readIterator -> {
              HeaderlessVcfOutputFormat.setHeader(vcfHeaderBroadcast.getValue());
              HeaderlessVcfOutputFormat.setBlockCompress(compressed);
              HeaderlessVcfOutputFormat.setWriteTbiFile(writeTbiFile);
              return readIterator;
            })
        .mapToPair(
            (PairFunction<VariantContext, Void, VariantContext>)
                variantContext -> new Tuple2<>(null, variantContext))
        .saveAsNewAPIHadoopFile(
            tempPartsDirectory,
            Void.class,
            VariantContext.class,
            HeaderlessVcfOutputFormat.class,
            jsc.hadoopConfiguration());

    String headerFile =
        tempPartsDirectory + "/header" + (compressed ? BGZFCodec.DEFAULT_EXTENSION : "");
    try (OutputStream headerOut = fileSystemWrapper.create(jsc.hadoopConfiguration(), headerFile)) {
      OutputStream out =
          compressed ? new BlockCompressedOutputStream(headerOut, (File) null) : headerOut;
      VariantContextWriter writer =
          new VariantContextWriterBuilder().clearOptions().setOutputVCFStream(out).build();
      writer.writeHeader(vcfHeader);
      out.flush(); // don't close BlockCompressedOutputStream since we don't want to write the
      // terminator after the header
    }
    if (compressed) {
      String terminatorFile = tempPartsDirectory + "/terminator";
      try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), terminatorFile)) {
        out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
      }
    }

    List<String> vcfParts =
        fileSystemWrapper
            .listDirectory(jsc.hadoopConfiguration(), tempPartsDirectory)
            .stream()
            .filter(new HiddenFileFilter())
            .collect(Collectors.toList());
    List<Long> partLengths = new ArrayList<>();
    for (String part : vcfParts) {
      partLengths.add(fileSystemWrapper.getFileLength(jsc.hadoopConfiguration(), part));
    }

    new Merger().mergeParts(jsc.hadoopConfiguration(), tempPartsDirectory, path);
    if (writeTbiFile) {
      new TbiMerger(fileSystemWrapper)
          .mergeParts(
              jsc.hadoopConfiguration(),
              tempPartsDirectory,
              path + TabixUtils.STANDARD_INDEX_EXTENSION,
              partLengths);
    }

    fileSystemWrapper.delete(jsc.hadoopConfiguration(), tempPartsDirectory);
  }
}
