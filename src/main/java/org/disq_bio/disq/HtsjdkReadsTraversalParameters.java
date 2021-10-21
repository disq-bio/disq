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
package org.disq_bio.disq;

import htsjdk.samtools.util.Locatable;

import java.io.Serializable;
import java.util.List;

/**
 * Holds parameters controlling which reads get returned when reading a {@link HtsjdkReadsRdd}.
 * Parameters include the intervals to include, and a flag controlling whether unplaced, unmapped
 * reads should be returned.
 *
 * @param <T> the type of Locatable
 */
public class HtsjdkReadsTraversalParameters<T extends Locatable> implements Serializable {
  private final List<T> intervalsForTraversal;
  private final boolean traverseUnplacedUnmapped;

  public HtsjdkReadsTraversalParameters(
      List<T> intervalsForTraversal, boolean traverseUnplacedUnmapped) {
    this.intervalsForTraversal = intervalsForTraversal;
    this.traverseUnplacedUnmapped = traverseUnplacedUnmapped;
  }

  public List<T> getIntervalsForTraversal() {
    return intervalsForTraversal;
  }

  public boolean getTraverseUnplacedUnmapped() {
    return traverseUnplacedUnmapped;
  }
}
