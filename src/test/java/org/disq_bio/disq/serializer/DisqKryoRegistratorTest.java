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
