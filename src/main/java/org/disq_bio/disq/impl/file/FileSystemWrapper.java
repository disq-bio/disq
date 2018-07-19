package org.disq_bio.disq.impl.file;

import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * A wrapper around Hadoop and NIO filesystems so users can choose a single one to use for all
 * filesystem operations.
 */
public interface FileSystemWrapper extends Serializable {

  String normalize(Configuration conf, String path) throws IOException;

  SeekableStream open(Configuration conf, String path) throws IOException;

  OutputStream create(Configuration conf, String path) throws IOException;

  boolean delete(Configuration conf, String path) throws IOException;

  boolean exists(Configuration conf, String path) throws IOException;

  long getFileLength(Configuration conf, String path) throws IOException;

  boolean isDirectory(Configuration conf, String path) throws IOException;

  List<String> listDirectory(Configuration conf, String path) throws IOException;

  void concat(Configuration conf, List<String> parts, String path) throws IOException;

  default String firstFileInDirectory(Configuration conf, String path) throws IOException {
    Optional<String> firstPath =
        listDirectory(conf, path)
            .stream()
            .filter(
                f ->
                    !(FilenameUtils.getBaseName(f).startsWith(".")
                        || FilenameUtils.getBaseName(f).startsWith("_")))
            .findFirst();
    if (!firstPath.isPresent()) {
      throw new IllegalArgumentException("No files found in " + path);
    }
    return firstPath.get();
  }
}
