package org.disq_bio.disq.impl.formats;

import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;

/**
 * A wrapper around {@link Configuration} that allows configuration to be passed to a Spark task.
 */
public class SerializableHadoopConfiguration implements Serializable {
  Configuration conf;

  public SerializableHadoopConfiguration(Configuration hadoopConf) {
    this.conf = hadoopConf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    this.conf.write(out);
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException {
    this.conf = new Configuration();
    this.conf.readFields(in);
  }
}
