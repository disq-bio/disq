package org.disq_bio.disq.impl.file;

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * An input format for reading information about file splits so as to preserve Hadoop file locality.
 * Should not be used directly.
 */
public class FileSplitInputFormat extends FileInputFormat<Void, FileSplit> {

  static class FileSplitRecordReader extends RecordReader<Void, FileSplit> {

    private FileSplit split;
    private boolean hasNext = true;
    private FileSplit current = null;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
      this.split = (FileSplit) split;
    }

    @Override
    public boolean nextKeyValue() {
      if (hasNext) {
        current = split;
        hasNext = false;
        return true;
      }
      current = null;
      return false;
    }

    @Override
    public Void getCurrentKey() {
      return null;
    }

    @Override
    public FileSplit getCurrentValue() {
      return current;
    }

    @Override
    public float getProgress() {
      return hasNext ? 0 : 1;
    }

    @Override
    public void close() throws IOException {}
  }

  @Override
  public RecordReader<Void, FileSplit> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new FileSplitRecordReader();
  }
}
