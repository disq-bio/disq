# Disq

A library for manipulating bioinformatics sequencing formats in Apache Spark.

[![Build Status](https://travis-ci.org/disq-bio/disq.svg?branch=master)](https://travis-ci.org/disq-bio/disq)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.disq-bio/disq/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.disq-bio/disq)
[![Javadocs](https://www.javadoc.io/badge/org.disq-bio/disq.svg)](https://www.javadoc.io/doc/org.disq-bio/disq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Motivation

This code grew out of, and was heavily inspired by, [Hadoop-BAM](https://github.com/HadoopGenomics/Hadoop-BAM) and
[Spark-BAM](http://www.hammerlab.org/spark-bam/). Spark-BAM has shown that reading BAMs for Spark can be both more
correct and more performant than the Hadoop-BAM implementation. Furthermore, all known users of Hadoop-BAM are using
Spark, not MapReduce, as their processing engine so it is natural to target the Spark API, which gives us higher-level
primitives than raw MR.

[Benchmarks](https://github.com/tomwhite/disq-benchmarks): Disq is faster and more accurate than Hadoop-BAM, and at least as fast as Spark-BAM.

## Support Matrix

This table summarizes the current level of support for each feature across the different file formats. See discussion
below for details on each feature.

| Feature                         | BAM                           | CRAM                          | SAM                           | VCF                           |
| ------------------------------- | ----------------------------- | ----------------------------- | ----------------------------- | ----------------------------- |
| Filesystems - Hadoop (r/w)      | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            |
| Filesystems - NIO (r)           | :white_check_mark:            | :white_check_mark:            | :x:                           | :x:                           |
| Compression                     | NA                            | NA                            | NA                            | :white_check_mark:            |
| Multiple input files            | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            |
| Sharded output                  | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            |
| Indexes - read heuristic        | :white_check_mark:            | :white_check_mark:            | NA                            | NA                            |
| Indexes - read .bai/.crai       | :x:                           | :white_check_mark:            | NA                            | NA                            |
| Indexes - write .bai/.crai      | :white_check_mark:            | :white_check_mark:            | NA                            | NA                            |
| Indexes - read .sbi             | :white_check_mark:            | NA                            | NA                            | NA                            |
| Indexes - write .sbi            | :white_check_mark:            | NA                            | NA                            | NA                            |
| Indexes - write .tbi            | NA                            | NA                            | NA                            | :white_check_mark:            |
| Intervals                       | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            |
| Ordering guarantees             | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            |
| Queryname sorted guarantees     | :x:                           | NA                            | :x:                           | NA                            |
| Stringency                      | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            | NA                            |
| Testing - large files           | :white_check_mark:            | :white_check_mark:            | :x:                           | :white_check_mark:            |
| Testing - samtools and bcftools | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            | :white_check_mark:            |

## Features

The following discusses the features provided by the library.

### Formats

The library should be able to read and write BAM, CRAM, SAM, and VCF formats, at a minimum. More formats
will be added over time, as needed.

Format records are represented by htsjdk types: `SAMRecord` (for BAM/CRAM/SAM) and `VariantContext` (for VCF).

Spark RDDs are used for the collection type.

Writing files will create new files or overwrite existing files without warning.

### Filesystems

Two filesystem abstractions are supported: the Hadoop filesystem (HDFS, local,
and others such as S3), and Java NIO filesystems (local, S3, GCS, etc).

Only one filesystem abstraction is used for each operation (unlike current Hadoop-BAM, which 
mixes the two, e.g. using Hadoop for bulk loading, and the HDFS NIO plugin for metadata
operations). The choice of which to use (Hadoop vs. NIO) is set by the user. Roughly speaking,
Hadoop is best for HDFS clusters (including those running in the cloud), and NIO is appropriate
for cloud stores.

NIO is only supported for reading, since writing has more complex commit semantics and is out of scope.
For writing to a cloud store use a Hadoop filesystem such as S3a or the GCS connector.

### Compression

For BAM and CRAM, compression is a part of the file format, so it is necessarily supported. Compressed SAM files are not
supported.

For reading VCF, support includes
[BGZF](https://samtools.github.io/hts-specs/SAMv1.pdf)-compressed (`.vcf.bgz` or `.vcf.gz`) and
gzip-compressed files (`.vcf.gz`).

For writing (compressed) VCF, only BGZF-compressed files can be written (gzip
is not splittable so it is a mistake to write this format).

### Multiple input files

For reading BAM/CRAM/SAM and VCF, multiple files may be read in one operation. A path may either be a
an individual file, or a directory. Directories are _not_ processed
recursively, so only the files in the directory are processed, and it is an error for the
directory to contain subdirectories.

Directories must contain files with the same header. If files have different headers then the effect of reading the
files is undefined.

File types may not be mixed: it is an error to process BAM and CRAM files, for example, in one operation.

### Sharded output

For writing BAM/CRAM/SAM and VCF, by default whole single files are written, but the output files may
optionally be sharded, for efficiency. A sharded BAM file has the following directory structure:

```
.
└── output.bam.sharded/
    ├── part-00000.bam
    ├── part-00001.bam
    ├── ...
    └── part-00009.bam

```

Note that `output.bam.sharded` is a directory and contains complete BAM files (with header and terminator), and a
`.bam` extension. A similar structure is used for the other formats.

Sharded files are treated as a single file for the purposes of reading multiple inputs.

### Indexes

For reading BAM, if there is no index, then the file is split using a heuristic algorithm to
find record boundaries. Otherwise, if a `.sbi` index file is found it is used to find
splits. A regular `.bai` index file may optionally be used to find splits, although it does not
protect against regions with very high coverage (oversampling) since it specifies genomic
regions, not file regions.

For writing BAM, `.bai` and`.sbi` indexes may optionally be created at the same time as writing the
BAM file. Indexes are not created by default, and have to be explicitly enabled.

For reading CRAM, if there is a `.crai` index then it is used to find record boundaries. Otherwise, the whole CRAM
file is efficiently scanned to read container headers so that record boundaries can be found.

SAM files and VCF files are split using the usual Hadoop file splitting implementation for finding text records.

For writing BGZF-compressed VCF, a tabix (`.tbi`) index may optionally be created at the same time as writing
the VCF file. Indexes are not written by default, and have to be explicitly enabled.

Writing `.crai` indexes is not possible at present. These can be generated using existing
tools, such as htsjdk/GATK/ADAM.

### Intervals

For reading BAM/CRAM/SAM and VCF, a range of intervals may be specified to restrict the records that are
loaded. Intervals are specified using htsjdk's `Interval` class.

For reading BAM/CRAM/SAM, when intervals are specified it is also possible to load unplaced unmapped reads if desired.

### Ordering Guarantees

This library does not do any sorting, so it is up to the user to understand what is being read or written. Furthermore,
no checks are carried out to ensure that the records being read or written are consistent with the header. E.g. it
is possible to write a BAM file whose header says it is `queryname` sorted, when in fact its records are unsorted. 

For reading a single BAM/SAM file, the records in the RDD are ordered by the BAM sort order header (`unknown`,
`unsorted`, `queryname`, or `coordinate`). For reading multiple BAM/SAM files, the records in the RDD are ordered
within each file, and files are ordered lexicographically. If the BAM/SAM files were written using a single invocation
of the write method in this library for an RDD that was sorted, then the RDD that is read back will retain the sort
order, and the sort order honors the sort order in the header of any of the files (the headers will all be the same).
If the BAM/SAM files are not globally sorted, then they should be treated as `unknown` or `unsorted`.

CRAM and VCF files are always sorted by position. If reading multiple files that are not globally sorted, then they
should be treated as unsorted, and should not be written out unless they are sorted first.

### Queryname Sorted Guarantees

For reading `queryname` sorted BAM or SAM, paired reads must never be split across partitions. This allows
applications to be sure that a single task will always be able to process read pairs together.

CRAM files must be `coordinate` sorted (not `queryname` sorted), so this provision is not applicable. 

### Stringency

For reading BAM/CRAM/SAM, the stringency settings from htsjdk are supported.

### Testing

All read and write paths are tested on real files from the field (multi-GB in size).

[Samtools and Bcftools](http://www.htslib.org/download/) are used to verify that files written with this library can be
read successfully.

## Usage

Add the dependency from [![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.disq-bio/disq/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.disq-bio/disq)


```java
// First get a Spark context
JavaSparkContext jsc = ...

// Read a BAM file into a Spark RDD
JavaRDD<SAMRecord> reads = HtsjdkReadsRddStorage.makeDefault(jsc)
    .read("src/test/resources/1.bam")
    .getReads();
```

## Implementation notes for developers

The library requires Java 8 or later. The surface area of the API has deliberately been kept small by exposing only a
few public classes in the top-level `disq` package, and putting all the private implementation classes in `impl`
packages. Users should not access anything in `impl`. While it is not possible to enforce this, anything in `impl`
is not subject to release versioning rules (i.e. it could be removed in any release).

The naming of classes in the public API reflects the fact that they work with htsjdk and Spark RDDs:
e.g. `HtsjdkReadsRddStorage`. In the future it will be possible to have alternative models that are not htsjdk
(or are a different version), or that use Spark datasets or dataframes.

As a general rule, any code that does not have a Spark or Hadoop dependency, or does not have a "distributed" flavor
belongs in htsjdk. This rule may be broken during a transition period while the code is being moved to htsjdk.

## Interoperability tests

Some tests use Samtools and Bcftools to check that files created with this library are readable with them (and htsjdk).

To run the tests first install [Samtools and Bcftools](http://www.htslib.org/download/).
(Version 1.4 of samtools, and 1.3 of bcftools were used. The latter was needed to avoid
[this bug](https://github.com/samtools/bcftools/issues/420).)

Then, when running tests, specify where the binaries are on your system as follows:

```
mvn test \
    -Ddisq.samtools.bin=/path/to/bin/samtools \
    -Ddisq.bcftools.bin=/path/to/bin/bcftools
```

## Real world file testing

The files used for unit testing are small and used to test particular features of the library.
It is also valuable to run tests against more realistic files, which tend to be a lot larger.

`RealWorldFilesIT` is an integration test that will recursively find files that it can parse in a given
directory, and count the number of records in each of them. The counts are compared against the equivalent
htsjdk code, as well as Samtools and Bcftools commands (if configured as above).

The following tests all the [GATK 'large' files](https://github.com/broadinstitute/gatk/tree/master/src/test/resources/large)
(BAM, CRAM, VCF) that have been copied to the local filesystem beforehand:

```
GATK_HOME=...
SAMTOOLS_HOME=...
BCFTOOLS_HOME=...
mvn verify \
    -Ddisq.test.real.world.files.dir=$GATK_HOME/src/test/resources/large \
    -Ddisq.test.real.world.files.ref=$GATK_HOME/src/test/resources/large/human_g1k_v37.20.21.fasta \
    -Ddisq.samtools.bin=$SAMTOOLS_HOME/bin/samtools \
    -Ddisq.bcftools.bin=$BCFTOOLS_HOME/bin/bcftools
```

## Performing a release

To perform a release you need commit rights to the repository, a key in the
`KEYS` file, and an account on [https://oss.sonatype.org/](https://oss.sonatype.org/)
with permission to write to the `disq-bio` Maven group ID. You also need
update your Maven _~/.m2/settings.xml_ file to include your Sonatype credentials
as described [here](https://central.sonatype.org/pages/apache-maven.html#distribution-management-and-authentication).

To [perform a snapshot deployment](https://central.sonatype.org/pages/apache-maven.html#performing-a-snapshot-deployment)
run

```
mvn clean deploy -Prelease
```

If you get the error `gpg: signing failed: Inappropriate ioctl for device` then
run this first:

```
export GPG_TTY=$(tty)
```

If your GitHub account is using two factor authentication then you may need to use
a token when prompted for a password. See [this article](https://help.github.com/en/articles/creating-a-personal-access-token-for-the-command-line).

To [perform a release deployment](https://central.sonatype.org/pages/apache-maven.html#performing-a-release-deployment-with-the-maven-release-plugin)
run 

```
mvn release:clean release:prepare
mvn release:perform
```

You will be prompted for the version number (note that the default may not be
correct), and the release tag, which should be of the form `disq-x.y.z`.

Inspect and release the deployed artifacts manually by logging into
[https://oss.sonatype.org/](https://oss.sonatype.org/). Find the repository
you just created, under "Staging Repositories", then "Close" and "Release" it.

You should see the artifacts appear in the [central repository](http://central.maven.org/maven2/org/disq-bio/disq/) in around 10 minutes.