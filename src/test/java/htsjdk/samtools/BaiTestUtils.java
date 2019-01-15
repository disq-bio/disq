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
package htsjdk.samtools;

import java.util.List;

public class BaiTestUtils {
  public static void dump(List<LinearIndex> linearIndexes) {
    int max = 0;
    for (LinearIndex li : linearIndexes) {
      max = Math.max(max, li.size());
    }
    for (int i = 0; i < max; i++) {
      System.out.print(i + "\t");
      for (LinearIndex li : linearIndexes) {
        long[] indexEntries = li.getIndexEntries();
        if (i < indexEntries.length) {
          System.out.print(indexEntries[i]);
        } else {
          System.out.print("-");
        }
        System.out.print("\t");
      }
      System.out.println();
    }
  }
}
