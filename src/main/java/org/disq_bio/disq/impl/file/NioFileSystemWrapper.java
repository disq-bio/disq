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
package org.disq_bio.disq.impl.file;

import htsjdk.samtools.seekablestream.SeekablePathStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.RuntimeIOException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;

public class NioFileSystemWrapper implements FileSystemWrapper {

  private static final int PREFETCHER_BUFFER_SIZE_MB = 4;

  @Override
  public boolean usesNio() {
    return true;
  }

  @Override
  public String normalize(Configuration conf, String path) {
    return asPath(path).toAbsolutePath().normalize().toString();
  }

  @Override
  public SeekableStream open(Configuration conf, String path) throws IOException {
    Function<SeekableByteChannel, SeekableByteChannel> prefetcherWrapper =
        channel -> {
          try {
            return SeekableByteChannelPrefetcher.addPrefetcher(PREFETCHER_BUFFER_SIZE_MB, channel);
          } catch (IOException e) {
            throw new RuntimeIOException(e);
          }
        };
    return new SeekablePathStream(asPath(path), prefetcherWrapper);
  }

  @Override
  public OutputStream create(Configuration conf, String path) throws IOException {
    return Files.newOutputStream(asPath(path));
  }

  @Override
  public boolean delete(Configuration conf, String path) throws IOException {
    return Files.deleteIfExists(asPath(path));
  }

  @Override
  public boolean exists(Configuration conf, String path) {
    return Files.isRegularFile(asPath(path));
  }

  @Override
  public long getFileLength(Configuration conf, String path) throws IOException {
    return Files.size(asPath(path));
  }

  @Override
  public boolean isDirectory(Configuration conf, String path) throws IOException {
    return Files.isDirectory(asPath(path));
  }

  @Override
  public List<String> listDirectory(Configuration conf, String path) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FileStatus> listDirectoryStatus(Configuration conf, String path) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void concat(Configuration conf, List<String> parts, String path) throws IOException {
    try (OutputStream out = create(conf, path)) {
      for (final String part : parts) {
        Path src = asPath(part);
        Files.copy(src, out);
        Files.delete(src);
      }
    }
  }

  /**
   * Convert the given path {@link URI} to a {@link Path} object.
   *
   * @param uri the path to convert
   * @return a {@link Path} object
   */
  private static Path asPath(URI uri) {
    try {
      return Paths.get(uri);
    } catch (FileSystemNotFoundException e) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) {
        throw e;
      }
      try {
        return FileSystems.newFileSystem(uri, new HashMap<>(), cl).provider().getPath(uri);
      } catch (IOException ex) {
        throw new RuntimeException("Cannot create filesystem for " + uri, ex);
      }
    }
  }

  /**
   * Convert the given path string to a {@link Path} object.
   *
   * @param path the path to convert
   * @return a {@link Path} object
   */
  public static Path asPath(String path) {
    URI uri = URI.create(path);
    return uri.getScheme() == null ? Paths.get(path) : asPath(uri);
  }
}
