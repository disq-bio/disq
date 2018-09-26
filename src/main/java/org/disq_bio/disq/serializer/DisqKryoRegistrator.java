package org.disq_bio.disq.serializer;

import com.esotericsoftware.kryo.Kryo;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Kryo registrator for Disq. */
public final class DisqKryoRegistrator implements KryoRegistrator {
  private final Logger logger = LoggerFactory.getLogger(DisqKryoRegistrator.class);

  @Override
  public void registerClasses(final Kryo kryo) {

    // Register Avro classes using fully qualified class names
    // Sort alphabetically and add blank lines between packages

    // htsjdk.samtools
    kryo.register(htsjdk.samtools.AlignmentBlock.class);
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
    kryo.register(htsjdk.variant.variantcontext.LazyGenotypesContext.class);
    kryo.register(htsjdk.variant.variantcontext.VariantContext.class);
    kryo.register(htsjdk.variant.variantcontext.VariantContext.Type.class);

    // htsjdk.variant.vcf
    kryo.register(htsjdk.variant.vcf.VCFCompoundHeaderLine.SupportedHeaderLineType.class);
    kryo.register(htsjdk.variant.vcf.VCFContigHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFFilterHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFFormatHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFHeader.class);
    kryo.register(htsjdk.variant.vcf.VCFHeaderLine.class);
    kryo.register(htsjdk.variant.vcf.VCFHeaderLineCount.class);
    kryo.register(htsjdk.variant.vcf.VCFHeaderLineType.class);
    kryo.register(htsjdk.variant.vcf.VCFInfoHeaderLine.class);

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

    // org.apache.spark.internal.io
    kryo.register(org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage.class);

    // org.disq_bio.disq
    kryo.register(org.disq_bio.disq.HtsjdkReadsTraversalParameters.class);

    // org.disq_bio.disq.impl.formats.bam
    kryo.register(
        org.disq_bio.disq.impl.formats.bam.BamRecordGuesserChecker.RecordStartResult.class);

    // org.disq_bio.disq.impl.formats.bgzf
    kryo.register(org.disq_bio.disq.impl.formats.bgzf.BgzfBlockGuesser.BgzfBlock.class);

    // scala.collection.mutable
    kryo.register(scala.collection.mutable.WrappedArray.ofRef.class);

    // sun.nio.ch
    registerByName(kryo, "sun.nio.ch.FileChannelImpl");
    registerByName(kryo, "sun.nio.ch.FileDispatcherImpl");
    registerByName(kryo, "sun.nio.ch.NativeThreadSet");

    // sun.nio.fs
    registerByName(kryo, "sun.nio.fs.BsdFileSystem");
    registerByName(kryo, "sun.nio.fs.BsdFileSystemProvider");
    registerByName(kryo, "sun.nio.fs.LinuxFileSystem");
    registerByName(kryo, "sun.nio.fs.LinuxFileSystemProvider");
    registerByName(kryo, "sun.nio.fs.MacOSXFileSystem");
    registerByName(kryo, "sun.nio.fs.MacOSXFileSystemProvider");
    registerByName(kryo, "sun.nio.fs.SolarisFileSystem");
    registerByName(kryo, "sun.nio.fs.SolarisFileSystemProvider");
    registerByName(kryo, "sun.nio.fs.UnixFileSystem");
    registerByName(kryo, "sun.nio.fs.UnixFileSystemProvider");
    registerByName(kryo, "sun.nio.fs.UnixPath");
  }

  void registerByName(final Kryo kryo, final String className) {
    try {
      kryo.register(Class.forName(className));
    } catch (ClassNotFoundException e) {
      logger.debug("Unable to register class {} by name", e, className);
    }
  }
}
