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
