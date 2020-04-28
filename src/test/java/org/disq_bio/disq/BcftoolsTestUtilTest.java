package org.disq_bio.disq;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

@RunWith(JUnitParamsRunner.class)
public class BcftoolsTestUtilTest extends BaseTest {

    private Object[] parametersForTestBcfToolsCountVariants(){
        return new Object[][]{
                {"test.vcf", 5},
                {"testEmpty.vcf", 0}
        };
    }

    @Test
    @Parameters
    public void testBcfToolsCountVariants(String vcfPath, int expected) throws IOException, URISyntaxException {
        Assume.assumeTrue(BcftoolsTestUtil.isBcftoolsAvailable());
        Assert.assertEquals(expected, BcftoolsTestUtil.countVariants(getPath(vcfPath)));
    }
}
