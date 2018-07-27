package org.disq_bio.disq;

/**
 * An option for controlling which directory to write temporary part files to when writing output as
 * a single file.
 */
public class TempPartsDirectoryWriteOption implements WriteOption {
  private String tempPartsDirectory;

  public TempPartsDirectoryWriteOption(String tempPartsDirectory) {
    this.tempPartsDirectory = tempPartsDirectory;
  }

  String getTempPartsDirectory() {
    return tempPartsDirectory;
  }
}
