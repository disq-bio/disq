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
package org.disq_bio.disq.serializer;

import static com.google.common.base.Preconditions.checkNotNull;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.NioFileSystemWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kryo registrator for Disq.
 *
 * <p>To use this class, specify the following Spark configuration parameters
 *
 * <pre>
 * spark.serializer=org.apache.spark.serializer.KryoSerializer
 * spark.kryo.registrator=org.disq_bio.disq.serializer.DisqKryoRegistrator
 * spark.kryo.registrationRequired=true
 * </pre>
 *
 * To include the classes registered here in your own registrator, either extend DisqKryoRegistrator
 *
 * <pre>
 * class MyKryoRegistrator extends DisqKryoRegistrator {
 *   public void registerClasses(final Kryo kryo) {
 *     super.registerClasses(kryo);
 *     kryo.register(MyClass.class);
 *   }
 * }
 * </pre>
 *
 * or extend by delegation
 *
 * <pre>
 * class MyKryoRegistrator implements KryoRegistrator {
 *   public void registerClasses(final Kryo kryo) {
 *     DisqKryoRegistrator.registerDisqClasses(kryo);
 *     kryo.register(MyClass.class);
 *   }
 * }
 * </pre>
 */
public class DisqKryoRegistrator implements KryoRegistrator {
  private final Logger logger = LoggerFactory.getLogger(DisqKryoRegistrator.class);

  @Override
  public void registerClasses(final Kryo kryo) {

    // Register Avro classes using fully qualified class names
    // Sort alphabetically and add blank lines between packages

    // htsjdk.samtools
    kryo.register(htsjdk.samtools.AlignmentBlock.class);
    kryo.register(htsjdk.samtools.BAMRecord.class);
    kryo.register(htsjdk.samtools.Chunk.class);
    kryo.register(htsjdk.samtools.Cigar.class);
    kryo.register(htsjdk.samtools.CigarElement.class);
    kryo.register(htsjdk.samtools.CigarOperator.class);
    kryo.register(htsjdk.samtools.SAMBinaryTagAndValue.class);
    kryo.register(htsjdk.samtools.SAMFileHeader.class);
    kryo.register(htsjdk.samtools.SAMFileHeader.GroupOrder.class);
    kryo.register(htsjdk.samtools.SAMFileHeader.SortOrder.class);
    kryo.register(htsjdk.samtools.SAMProgramRecord.class);
    kryo.register(htsjdk.samtools.SAMReadGroupRecord.class);
    kryo.register(htsjdk.samtools.SAMRecord.class);
    kryo.register(htsjdk.samtools.SAMSequenceDictionary.class);
    kryo.register(htsjdk.samtools.SAMSequenceRecord.class);
    kryo.register(htsjdk.samtools.SBIIndex.class);
    kryo.register(htsjdk.samtools.SBIIndex.Header.class);
    kryo.register(htsjdk.samtools.ValidationStringency.class);

    // htsjdk.samtools.cram.ref
    kryo.register(htsjdk.samtools.cram.ref.ReferenceSource.class);

    // htsjdk.samtools.reference
    kryo.register(htsjdk.samtools.reference.FastaSequenceIndex.class);
    kryo.register(htsjdk.samtools.reference.FastaSequenceIndexEntry.class);
    kryo.register(htsjdk.samtools.reference.IndexedFastaSequenceFile.class);

    // htsjdk.samtools.util
    kryo.register(htsjdk.samtools.util.Interval.class);

    // htsjdk.variant.variantcontext
    kryo.register(htsjdk.variant.variantcontext.Allele.class);
    kryo.register(htsjdk.variant.variantcontext.CommonInfo.class);
    kryo.register(htsjdk.variant.variantcontext.FastGenotype.class);
    kryo.register(htsjdk.variant.variantcontext.GenotypeType.class);
    // Use JavaSerializer for LazyGenotypesContext to handle transient fields correctly
    kryo.register(htsjdk.variant.variantcontext.LazyGenotypesContext.class, new JavaSerializer());
    kryo.register(htsjdk.variant.variantcontext.VariantContext.class);
    kryo.register(htsjdk.variant.variantcontext.VariantContext.Type.class);

    // htsjdk.variant.vcf
    kryo.register(htsjdk.variant.vcf.VCFAltHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFCompoundHeaderLine.SupportedHeaderLineType.class);
    kryo.register(htsjdk.variant.vcf.VCFContigHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFFilterHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFFormatHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFHeader.class);
    kryo.register(htsjdk.variant.vcf.VCFHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFHeaderLineCount.class);
    kryo.register(htsjdk.variant.vcf.VCFHeaderLineType.class);
    kryo.register(htsjdk.variant.vcf.VCFHeaderVersion.class);
    kryo.register(htsjdk.variant.vcf.VCFInfoHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFMetaHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFPedigreeHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFSampleHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFSimpleHeaderLine.class);

    // java.io
    kryo.register(java.io.FileDescriptor.class);

    // java.lang
    kryo.register(java.lang.Object.class);
    kryo.register(java.lang.Object[].class);

    // java.util
    kryo.register(java.util.ArrayList.class);
    kryo.register(
        java.util.Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
    kryo.register(
        java.util.Collections.singletonList("").getClass(),
        new CollectionsSingletonListSerializer());
    kryo.register(java.util.HashMap.class);
    kryo.register(java.util.HashSet.class);
    kryo.register(java.util.LinkedHashMap.class);
    registerByName(kryo, "java.util.LinkedHashMap$Entry");
    registerByName(kryo, "java.util.LinkedHashMap$LinkedValueIterator");
    kryo.register(java.util.LinkedHashSet.class);
    kryo.register(java.util.TreeSet.class);

    UnmodifiableCollectionsSerializer.registerSerializers(kryo);

    // org.apache.spark.internal.io
    kryo.register(org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage.class);

    // org.disq_bio.disq
    kryo.register(org.disq_bio.disq.HtsjdkReadsTraversalParameters.class);

    // org.disq_bio.disq.impl.file
    kryo.register(
        HadoopFileSystemWrapper.class,
        new Serializer<HadoopFileSystemWrapper>() {
          @Override
          public void write(
              final Kryo kryo,
              final Output output,
              final HadoopFileSystemWrapper fileSystemWrapper) {
            // empty
          }

          @Override
          public HadoopFileSystemWrapper read(
              final Kryo kryo, final Input input, final Class<HadoopFileSystemWrapper> type) {
            return new HadoopFileSystemWrapper();
          }
        });

    kryo.register(
        NioFileSystemWrapper.class,
        new Serializer<NioFileSystemWrapper>() {
          @Override
          public void write(
              final Kryo kryo, final Output output, final NioFileSystemWrapper fileSystemWrapper) {
            // empty
          }

          @Override
          public NioFileSystemWrapper read(
              final Kryo kryo, final Input input, final Class<NioFileSystemWrapper> type) {
            return new NioFileSystemWrapper();
          }
        });

    // org.disq_bio.disq.impl.formats.bam
    kryo.register(
        org.disq_bio.disq.impl.formats.bam.BamRecordGuesserChecker.RecordStartResult.class);

    // org.disq_bio.disq.impl.formats.bgzf
    kryo.register(org.disq_bio.disq.impl.formats.bgzf.BgzfBlockGuesser.BgzfBlock.class);

    // scala.collection.immutable
    registerByName(kryo, "scala.collection.immutable.Set$EmptySet$");

    // scala.collection.mutable
    kryo.register(scala.collection.mutable.WrappedArray.ofRef.class);
  }

  void registerByName(final Kryo kryo, final String className) {
    try {
      kryo.register(Class.forName(className));
    } catch (ClassNotFoundException e) {
      logger.debug("Unable to register class {} by name", e, className);
    }
  }

  /**
   * Register all classes serialized in Disq with the specified Kryo instance.
   *
   * @param kryo Kryo instance to register all classes serialized in Disq with, must not be null
   */
  public static final void registerDisqClasses(final Kryo kryo) {
    checkNotNull(kryo);
    new DisqKryoRegistrator().registerClasses(kryo);
  }
}
