package org.disq_bio.disq.impl.file;

import java.util.function.Predicate;
import org.apache.commons.io.FilenameUtils;

public class HiddenFileFilter implements Predicate<String> {
  @Override
  public boolean test(String path) {
    return !(FilenameUtils.getBaseName(path).startsWith(".")
        || FilenameUtils.getBaseName(path).startsWith("_"));
  }
}
