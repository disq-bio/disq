package htsjdk.samtools;

/**
 * This class is required in order to access the protected
 * {@link SAMRecord} methods in HTSJDK.
 */
public class SAMRecordHelper {
  public static void setFileSource(SAMRecord record, SAMFileSource fileSource) {
    record.setFileSource(fileSource);
  }
}
