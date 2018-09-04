package org.disq_bio.disq.impl.file;

import htsjdk.samtools.seekablestream.SeekableBufferedStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.*;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopFileSystemWrapper implements FileSystemWrapper {

  private static final Logger logger = LoggerFactory.getLogger(HadoopFileSystemWrapper.class);
  private static final boolean TRACK_UNCLOSED_STREAMS =
      false; // set to true and run tests to see if any streams are not being closed

  @Override
  public boolean usesNio() {
    return false;
  }

  @Override
  public String normalize(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return fileSystem.makeQualified(p).toString();
  }

  @Override
  public SeekableStream open(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    long len = fileSystem.getFileStatus(p).getLen();
    return new SeekableBufferedStream(new SeekableHadoopStream<>(fileSystem.open(p), len, path));
  }

  @Override
  public OutputStream create(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return fileSystem.create(p);
  }

  @Override
  public boolean delete(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return fileSystem.delete(p, true);
  }

  @Override
  public boolean exists(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return fileSystem.exists(p);
  }

  @Override
  public long getFileLength(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return fileSystem.getFileStatus(p).getLen();
  }

  @Override
  public boolean isDirectory(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return fileSystem.isDirectory(p);
  }

  @Override
  public List<String> listDirectory(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return Arrays.stream(fileSystem.listStatus(p))
        .map(fs -> fs.getPath().toUri().toString())
        .sorted()
        .collect(Collectors.toList());
  }

  @Override
  public void concat(Configuration conf, List<String> parts, String path) throws IOException {
    // target must be in same directory as parts being concat'ed
    Path tmp = new Path(new Path(parts.get(0)).getParent(), "output");
    FileSystem fileSystem = tmp.getFileSystem(conf);
    fileSystem.create(tmp).close(); // target must already exist for concat
    try {
      concat(parts, tmp, fileSystem);
      Path target = new Path(path);
      if (fileSystem.exists(target)) { // delete target if it exists
        fileSystem.delete(target, true);
      }
      fileSystem.rename(tmp, target);
    } catch (UnsupportedOperationException e) {
      logger.warn("Concat not supported, merging serially");
      try (OutputStream out = create(conf, path)) {
        for (String part : parts) {
          try (InputStream in = open(conf, part)) {
            IOUtils.copyBytes(in, out, conf, false);
          }
          fileSystem.delete(new Path(part), false);
        }
      }
      fileSystem.delete(tmp, false);
    }
  }

  static void concat(List<String> parts, Path outputPath, FileSystem filesystem)
      throws IOException {
    org.apache.hadoop.fs.Path[] fsParts =
        parts
            .stream()
            .map(Path::new)
            .collect(Collectors.toList())
            .toArray(new org.apache.hadoop.fs.Path[parts.size()]);
    filesystem.concat(new org.apache.hadoop.fs.Path(outputPath.toUri()), fsParts);
  }

  public static class SeekableHadoopStream<S extends InputStream & Seekable>
      extends SeekableStream {

    private static Set<SeekableHadoopStream> unclosedStreams = new LinkedHashSet<>();

    static {
      if (TRACK_UNCLOSED_STREAMS) {
        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      if (unclosedStreams.isEmpty()) {
                        System.out.println("No dangling input streams");
                      } else {
                        System.out.println("Dangling input streams");
                        unclosedStreams.forEach(
                            s -> System.out.println(s.source + "\n" + s.constructionStackTrace));
                      }
                    }));
      }
    }

    private final S in;
    private final long length;
    private final String source;
    private final String constructionStackTrace;

    public SeekableHadoopStream(S seekableIn, long length, String source) {
      this.in = seekableIn;
      this.length = length;
      this.source = source;
      this.constructionStackTrace = getStackTrace();
      if (TRACK_UNCLOSED_STREAMS) {
        unclosedStreams.add(this);
      }
    }

    private String getStackTrace() {
      if (TRACK_UNCLOSED_STREAMS) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (PrintWriter pw = new PrintWriter(bos)) {
          new Throwable().printStackTrace(pw);
        }
        return new String(bos.toByteArray());
      }
      return null;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public long position() throws IOException {
      return in.getPos();
    }

    @Override
    public void seek(long position) throws IOException {
      in.seek(position);
    }

    @Override
    public int read() throws IOException {
      return in.read();
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      return in.read(buffer, offset, length);
    }

    @Override
    public void close() throws IOException {
      if (TRACK_UNCLOSED_STREAMS) {
        unclosedStreams.remove(this);
      }
      in.close();
    }

    @Override
    public boolean eof() throws IOException {
      return in.getPos() == length();
    }

    @Override
    public String getSource() {
      return source.toString();
    }
  }
}
