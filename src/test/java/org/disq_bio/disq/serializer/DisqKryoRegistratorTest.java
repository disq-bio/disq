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
package org.disq_bio.disq.serializer;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import org.disq_bio.disq.HtsjdkReadsTraversalParameters;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit test for DisqKryoRegistrator. */
public final class DisqKryoRegistratorTest {
  @Mock private Kryo kryo;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testRegisterClasses() {
    new DisqKryoRegistrator().registerClasses(kryo);
    verify(kryo, times(1)).register(HtsjdkReadsTraversalParameters.class);
  }

  @Test
  public void testExtendByExtension() {
    new ByExtensionKryoRegistrator().registerClasses(kryo);
    verify(kryo, times(1)).register(HtsjdkReadsTraversalParameters.class);
    verify(kryo, times(1)).register(ToSerialize.class);
  }

  class ToSerialize {
    // empty
  }

  /** Extend DisqKryoRegistrator by extension. */
  class ByExtensionKryoRegistrator extends DisqKryoRegistrator {
    @Override
    public void registerClasses(final Kryo kryo) {
      super.registerClasses(kryo);
      kryo.register(ToSerialize.class);
    }
  }

  @Test
  public void testExtendByDelegation() {
    new ByDelegationKryoRegistrator().registerClasses(kryo);
    verify(kryo, times(1)).register(HtsjdkReadsTraversalParameters.class);
    verify(kryo, times(1)).register(ToSerialize.class);
  }

  /** Extend DisqKryoRegistrator by delegation. */
  class ByDelegationKryoRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(final Kryo kryo) {
      DisqKryoRegistrator.registerDisqClasses(kryo);
      kryo.register(ToSerialize.class);
    }
  }

  @Test(expected = NullPointerException.class)
  public void testRegisterDisqClassesNullKryo() {
    DisqKryoRegistrator.registerDisqClasses(null);
  }

  @Test
  public void testRegisterDisqClasses() {
    DisqKryoRegistrator.registerDisqClasses(kryo);
    verify(kryo, times(1)).register(HtsjdkReadsTraversalParameters.class);
  }
}
